# Design Document: NOMT-Style Binary Merkle Tree for Geth

## Context

Geth's Merkle Patricia Trie (MPT) is I/O-bound during block execution and state commitment. NOMT (Nearly-Optimal Merkle Trie) addresses this by using a binary merkle trie with SSD-optimized page-based storage, achieving significantly higher throughput through batched updates, aggressive prefetching, and minimal disk reads per operation.

This document describes how to port NOMT's architecture into geth as a **new, independent binary merkle trie implementation** alongside the existing MPT. The existing MPT code (`trie/`, `triedb/hashdb/`, `triedb/pathdb/`) remains untouched — NOMT is added as a new `triedb` backend option. NOMT has two core components:
- **Beatree** (B-tree for flat key-value storage) — replaced by PebbleDB
- **Bitbox** (on-disk hash table storing merkle tree pages) — the focus of this implementation

---

## 1. NOMT Architecture Overview

### 1.1 Binary Merkle Trie

NOMT uses a sparse binary merkle trie where all lookup paths are 256 bits and all nodes are exactly 32 bytes. Three node types exist:

| Type | Value | MSB | Children |
|------|-------|-----|----------|
| **Internal** | `hash(left \|\| right)` | `0` | Two child nodes |
| **Leaf** | `hash(key_path \|\| value_hash)` | `1` | None |
| **Terminator** | `0x00...00` (all zeros) | N/A | None (empty subtrie) |

The MSB (most significant bit) labeling enables O(1) node type discrimination. All node preimages are 512 bits (64 bytes).

**Key insight**: Because every node is exactly 32 bytes, groups of nodes can be packed into fixed-size pages with predictable layouts — no variable-length encoding needed.

### 1.2 Page Structure

Each page is **4096 bytes** (aligned to SSD page size) and stores a **rootless sub-binary-tree of depth 6**:

```
Page Layout (4096 bytes):
┌─────────────────────────────────────────┐
│ 126 nodes × 32 bytes = 4032 bytes       │  Nodes at depths 1-6
│   Level 1:  2 nodes  (siblings)         │  (rootless — the root lives
│   Level 2:  4 nodes                     │   in the parent page)
│   Level 3:  8 nodes                     │
│   Level 4: 16 nodes                     │
│   Level 5: 32 nodes                     │
│   Level 6: 64 nodes  (leaf layer)       │
├─────────────────────────────────────────┤
│ 24 bytes padding                        │
├─────────────────────────────────────────┤
│ 8 bytes: ElidedChildren bitfield (u64)  │  Which child pages are elided
├─────────────────────────────────────────┤
│ 32 bytes: PageID (encoded)              │  Unique page identifier
└─────────────────────────────────────────┘
```

Each page has up to **64 child pages**, one for each leaf position. The tree of pages has maximum depth 42 (since 6 × 42 = 252 ≈ 256 bits).

### 1.3 Page Identification

A `PageID` is a path through the page tree — a sequence of 0-42 child indices (each 0..63). The encoding uses a base-64-like scheme:

```
Encoding: For path [c₀, c₁, ..., cₙ]:
  result = 0
  for each cᵢ:
    result += (cᵢ + 1)
    result <<= 6
```

This produces a unique, ordered 256-bit representation stored in the last 32 bytes of each page. The ordering property ensures depth-first traversal: parent < children < right siblings.

**Key path mapping**: A 256-bit key maps to a chain of pages. Every 6 bits of the key selects a child index at the corresponding page depth.

### 1.4 Page Elision

If a subtree rooted at a page has fewer than **20 leaves** (the `PAGE_ELISION_THRESHOLD`), that page is not stored on disk. Instead, it is reconstructed on-the-fly from the parent page's nodes. The `ElidedChildren` bitfield (8 bytes, one bit per child slot) tracks which child pages are elided.

This optimization significantly reduces storage and I/O for sparse regions of the trie.

---

## 2. Bitbox: The Page Store

Bitbox is an on-disk **open-addressing hash table** that maps PageIDs to 4096-byte pages.

### 2.1 Hash Table File Layout

```
HT File Layout:
┌──────────────────────────────────────────┐
│ Meta Byte Pages                          │  ceil(num_buckets / 4096) pages
│   One byte per bucket (occupancy + tag)  │
├──────────────────────────────────────────┤
│ Data Pages                               │  num_buckets pages
│   Each bucket = one 4096-byte page       │  (the actual merkle tree pages)
└──────────────────────────────────────────┘

Total file size = (meta_pages + num_buckets) × 4096
```

### 2.2 Meta Map

One byte per bucket encodes state + hash tag for fast probing:

| Byte Value | Meaning |
|------------|---------|
| `0x00` | Empty bucket |
| `0x7F` | Tombstone (deleted) |
| `0x80 \| (hash >> 57)` | Occupied, with 7-bit hash tag |

The 7-bit hash tag enables filtering ~99% of non-matching buckets without reading the full page from disk.

### 2.3 Probing

Bitbox uses **triangular probing** (probe offsets: 0, 1, 3, 6, 10, ...):

```
bucket₀ = hash(pageID) % capacity
bucketᵢ = (bucket₀ + i*(i+1)/2) % capacity
```

The hash function is `xxhash3_64` with a random 16-byte seed (generated at DB creation).

**Page lookup flow**:
1. Compute `hash(pageID)` → initial bucket
2. Check meta byte: empty → miss, tombstone → skip, tag mismatch → skip
3. On tag match: read the 4096-byte data page from disk
4. Verify: check if the PageID in the last 32 bytes matches
5. If mismatch (rare): continue probing

### 2.4 WAL (Write-Ahead Log)

Crash recovery uses a simple binary WAL:

```
WAL Format:
[START tag (1 byte)] [sync_seqn (4 bytes LE)]
repeated:
  [CLEAR tag (1 byte)] [bucket (8 bytes LE)]
  [UPDATE tag (1 byte)] [page_id (32 bytes)] [page_diff (16 bytes)]
     [changed_nodes (N × 32 bytes)] [elided_children (8 bytes)] [bucket (8 bytes LE)]
[END tag (1 byte)]
[zero-padded to 4096-byte boundary]
```

**Recovery protocol**:
1. On open, if WAL is non-empty, read sync sequence number
2. If it matches the expected sequence, replay all entries (apply diffs to HT pages, update meta map)
3. Write changed meta pages to HT file
4. Truncate and fsync WAL

### 2.5 Sync Protocol

Persisting dirty pages follows a strict three-phase protocol:

```
Phase 1: begin_sync()
  ├── Lock meta map (write)
  ├── For each dirty page:
  │   ├── Allocate or reuse bucket via probing
  │   ├── Update meta map (set_full or set_tombstone)
  │   └── Record changes in WAL builder
  ├── Build HT page write list
  └── Update page cache (batch insert + evict)

Phase 2: wait_pre_meta()
  └── Write WAL to disk + fsync (atomic durability point)

Phase 3: [External] Write meta/manifest (atomic sync point)

Phase 4: post_meta()
  ├── Write dirty HT pages + meta pages to HT file + fsync
  └── Truncate WAL (no fsync needed — see rationale below)
```

**Why truncate without fsync**: If we crash before the next commit, the WAL replay is idempotent. If we reach the next commit, the new WAL write will fsync.

---

## 3. Merkle Tree Updates: The Page Walker

The `PageWalker` is the core algorithm for batch-updating the binary merkle trie. It processes sorted key-value updates left-to-right through the page tree.

### 3.1 Sub-Trie Replacement

Updates are grouped by which terminal node their keys map to. Each terminal is replaced with a new sub-trie built from the updates:

- Delete a leaf → replace with terminator
- Insert where terminator was → replace with leaf
- Multiple inserts at same prefix → build an internal sub-trie
- Delete + insert at same key → replace leaf with new leaf

### 3.2 Partial Compaction

After each replacement, the walker hashes upward (computing internal node hashes) and compacts terminators. It stops at the point where the next update would also affect the result, avoiding redundant work. The last update hashes all the way to the root.

### 3.3 Algorithm Sketch

```
PageWalker.advance_and_replace(terminal_position, operations):
  1. Build page stack down to terminal position
     (loading existing pages or creating fresh ones)
  2. Build new sub-trie from operations (build_trie)
  3. Place new nodes into the current page
  4. Hash upward, compacting terminators:
     while sibling(current) is terminator or leaf:
       compact (merge leaf up or create terminator pair)
     stop when next update will affect this path

PageWalker.conclude():
  1. Hash all remaining nodes up to root
  2. Emit root node + list of UpdatedPage entries
```

### 3.4 Parallel Updates (Workers)

For large batches, the page tree is partitioned into regions (by root page children). Each region is processed by a separate worker goroutine:

1. **Warm-up phase**: Prefetch pages that will be needed (walk keys, load pages from cache/disk)
2. **Update phase**: Run PageWalker on the region's subset of updates
3. **Merge phase**: Collect child page root nodes, update the root page

---

## 4. Flat Key-Value Storage (PebbleDB)

PebbleDB replaces NOMT's Beatree for storing raw key-value data. This is the "value store" that sits alongside Bitbox:

### 4.1 Key Schema

```
Account data:  key = 0x01 || keccak256(address)       → RLP(SlimAccount)
Storage data:  key = 0x02 || keccak256(address) || keccak256(slot) → value
Metadata:      key = 0x00 || "root"                    → current root node (32 bytes)
               key = 0x00 || "sync_seqn"               → sync sequence number (4 bytes)
               key = 0x00 || "seed"                     → hash table seed (16 bytes)
```

PebbleDB handles compaction, compression, and point-lookup optimization via bloom filters.

---

## 5. Go Implementation Plan

### 5.1 Package Structure

```
go-ethereum/
  nomt/
    core/
      node.go          # Node type, KeyPath, ValueHash, NodeKind, TERMINATOR
      hasher.go        # NodeHasher interface, keccak256-based binary hasher
      page.go          # Page constants, RawPage read/write helpers
      pageid.go        # PageID encode/decode, child/parent, iterator
      triepos.go       # TriePosition: depth tracking, page navigation
      pagediff.go      # PageDiff: 126-bit change tracking bitfield
      update.go        # build_trie helper, WriteNode, leaf splicing
    bitbox/
      db.go            # DB handle: open, sync entry point, utilization
      htfile.go        # HTOffsets, file creation, layout math
      metamap.go       # MetaMap: per-bucket metadata byte
      probe.go         # ProbeSequence: triangular probing, xxhash
      pagecache.go     # Sharded LRU page cache with fixed-level pinning
      pageloader.go    # PageLoader: probe-based page retrieval
      wal.go           # WAL writer and reader (blob format)
      writeout.go      # write_wal, write_ht, truncate_wal
      sync.go          # SyncController: begin_sync, wait_pre_meta, post_meta
      recover.go       # WAL recovery logic
    merkle/
      pagewalker.go    # Left-to-right batch trie update engine
      pageset.go       # PageSet interface for walker's page access
      elided.go        # ElidedChildren 64-bit bitfield
      worker.go        # Parallel update workers (warm-up + sharded)
    io/
      directio.go      # O_DIRECT helpers (linux/darwin build tags)
      pagepool.go      # sync.Pool-backed 4096-byte aligned page allocator
  triedb/
    nomtdb/
      config.go        # NomtDB configuration
      database.go      # Database implementing backend interface
      reader.go        # NodeReader and StateReader implementations
```

### 5.2 Key Data Structures

```go
// --- core/node.go ---
type Node = [32]byte
type KeyPath = [32]byte
type ValueHash = [32]byte
var TERMINATOR Node // all zeros

type NodeKind int
const (
    NodeKindTerminator NodeKind = iota
    NodeKindLeaf
    NodeKindInternal
)

// --- core/page.go ---
const (
    Depth        = 6
    NodesPerPage = 126   // (2^7) - 2
    PageSize     = 4096
    NumChildren  = 64    // 2^Depth
)

// --- core/pageid.go ---
type PageID struct {
    path []uint8  // each 0..63, len 0..42
}

// --- bitbox/db.go ---
type DB struct {
    pagePool  *PagePool
    store     HTOffsets
    seed      [16]byte
    metaMap   *sync.RWMutex  // guards MetaMap
    walFD     *os.File
    htFD      *os.File
    capacity  int
    occupied  atomic.Int64
}

// --- bitbox/probe.go ---
type ProbeSequence struct {
    hash   uint64
    bucket uint64
    step   uint64
}
```

### 5.3 Hash Function

For Ethereum compatibility, use **keccak256** with MSB labeling:

```go
func HashInternal(left, right Node) Node {
    h := crypto.Keccak256(left[:], right[:])
    var node Node
    copy(node[:], h)
    node[0] &= 0x7F  // clear MSB → internal
    return node
}

func HashLeaf(keyPath KeyPath, valueHash ValueHash) Node {
    h := crypto.Keccak256(keyPath[:], valueHash[:])
    var node Node
    copy(node[:], h)
    node[0] |= 0x80  // set MSB → leaf
    return node
}
```

> **Trade-off note**: NOMT defaults to Blake3 (~3-5x faster). The `NodeHasher` interface makes this swappable. For production, a protocol change could adopt Blake3.

### 5.4 I/O Adaptation

| NOMT (Rust) | Go Port |
|-------------|---------|
| `io_uring` (Linux) | `os.File.ReadAt` / `WriteAt` + goroutine pool |
| `pread`/`pwrite` (fallback) | Same via `os.File` |
| `fcntl(F_NOCACHE)` (macOS) | Same via `syscall.Syscall` |
| `O_DIRECT` (Linux) | `syscall.O_DIRECT` + aligned buffers |
| mmap WAL builder | `[]byte` buffer (WAL is small) |
| `threadpool` + channels | goroutines + `chan` |

### 5.5 Geth Integration

The NOMT backend plugs into geth's `triedb` framework as a new backend option alongside HashDB and PathDB:

```go
// triedb/database.go — add to Config
type Config struct {
    HashDB *hashdb.Config
    PathDB *pathdb.Config
    NomtDB *nomtdb.Config  // NEW
}

// nomtdb/database.go — implements backend interface
type Database struct {
    bitbox  *bitbox.DB
    pebble  *pebble.DB
    cache   *bitbox.PageCache
    root    core.Node
    syncSeq uint32
    lock    sync.RWMutex
}

func (db *Database) NodeReader(root common.Hash) (database.NodeReader, error)
func (db *Database) StateReader(root common.Hash) (database.StateReader, error)
func (db *Database) Commit(root common.Hash, report bool) error
```

**StateReader** serves accounts and storage directly from PebbleDB (flat reads, no trie traversal for data). **NodeReader** resolves pages from Bitbox and returns individual node values.

### 5.6 Update Flow (Per Block)

```
1. StateDB collects changed accounts/storage slots

2. Build update set:
   For each changed key:
     key_path = keccak256(key)
     value_hash = keccak256(value)  // or TERMINATOR if deleted
   Sort by key_path

3. Run PageWalker (parallel workers for large batches):
   Input:  sorted [(key_path, value_hash)] + current page tree
   Output: new root node + list of UpdatedPages

4. Persist:
   a. Write flat KV changes to PebbleDB (batch write)
   b. Sync dirty pages via SyncController:
      begin_sync → WAL write → meta update → HT write

5. Return new root hash (the 32-byte root node)
```

---

## 6. Implementation Phases

### Phase 1: Core Primitives
**Goal**: All trie data structures, no I/O.

Files: `nomt/core/*.go` + tests

- Node types, hashing, MSB labeling
- Page layout (read/write nodes, elided children, page ID from page)
- PageID encode/decode/iterate
- TriePosition traversal
- PageDiff bitfield operations

**Milestone**: All unit tests pass. Hash outputs match NOMT's Rust tests.

### Phase 2: Page Walker
**Goal**: In-memory batch update engine.

Files: `nomt/merkle/*.go` + tests

- ElidedChildren bitfield
- PageSet interface + in-memory implementation
- PageWalker: advance_and_replace, conclude
- build_trie helper

**Milestone**: Walker produces correct root hashes and page diffs for known inputs.

### Phase 3: Bitbox Storage
**Goal**: On-disk hash table.

Files: `nomt/bitbox/*.go` (htfile, metamap, probe, pagecache, pageloader, db) + `nomt/io/*.go`

- Page pool (sync.Pool, aligned allocation)
- Direct I/O helpers (linux/darwin)
- MetaMap operations
- HT file creation and opening
- Probe sequence + page loading
- Page cache (sharded LRU with fixed-level pinning)

**Milestone**: Can create HT file, insert pages, read them back. Cache serves hot pages.

### Phase 4: WAL and Sync
**Goal**: Crash-safe persistence.

Files: `nomt/bitbox/wal.go`, `sync.go`, `writeout.go`, `recover.go` + tests

- WAL blob writer/reader
- SyncController three-phase protocol
- Recovery from WAL
- writeout helpers (write_wal, write_ht, truncate_wal)

**Milestone**: Full sync cycle works. Simulated crash recovery restores correct state.

### Phase 5: Geth Backend
**Goal**: Wire into geth's triedb framework.

Files: `triedb/nomtdb/*.go` + modifications to `triedb/database.go`

- Config, Database, NodeReader, StateReader
- PebbleDB flat KV integration
- Update flow: StateSet → PageWalker → SyncController
- Add NomtDB case to triedb.NewDatabase and triedb.Update

**Milestone**: geth configured with NOMT backend can read/write state via standard interfaces.

### Phase 6: Parallel Workers + Optimization
**Goal**: Throughput optimization.

- Parallel page walkers (sharded by root page children)
- Warm-up phase (prefetch pages before updates)
- Benchmarks vs. existing backends

---

## 7. Key Source Files (Reference)

| Component | NOMT Source | Purpose |
|-----------|-------------|---------|
| Node types | `nomt/core/src/trie.rs` | Node, KeyPath, TERMINATOR, NodeKind |
| Hasher | `nomt/core/src/hasher.rs` | MSB labeling, hash functions |
| Page layout | `nomt/core/src/page.rs` | DEPTH=6, NODES_PER_PAGE=126 |
| Page IDs | `nomt/core/src/page_id.rs` | Encode/decode, child/parent |
| Trie position | `nomt/core/src/trie_pos.rs` | Depth tracking, node indexing |
| Page diff | `nomt/core/src/page_diff.rs` | 126-bit change bitfield |
| Page walker | `nomt/nomt/src/merkle/page_walker.rs` | Batch update algorithm |
| Page set | `nomt/nomt/src/merkle/page_set.rs` | Page access interface |
| Workers | `nomt/nomt/src/merkle/worker.rs` | Parallel update workers |
| Bitbox DB | `nomt/nomt/src/bitbox/mod.rs` | Hash table DB, sync, probing |
| HT file | `nomt/nomt/src/bitbox/ht_file.rs` | File layout, create/open |
| Meta map | `nomt/nomt/src/bitbox/meta_map.rs` | Per-bucket metadata bytes |
| WAL | `nomt/nomt/src/bitbox/wal.rs` | Write-ahead log format |
| Page cache | `nomt/nomt/src/page_cache.rs` | Sharded LRU cache |

| Component | Geth Source | Purpose |
|-----------|-------------|---------|
| Backend interface | `go-ethereum/triedb/database/database.go` | NodeReader, StateReader |
| Backend selector | `go-ethereum/triedb/database.go` | Config, NewDatabase |
| PathDB (reference) | `go-ethereum/triedb/pathdb/database.go` | Similar backend pattern |
| State DB | `go-ethereum/core/state/statedb.go` | State management layer |

---

## 8. Verification Plan

1. **Unit tests**: Port NOMT's Rust test cases for PageID, PageDiff, MetaMap, ProbeSequence
2. **Hash compatibility**: Verify node hashes match expected values for known inputs
3. **PageWalker correctness**: Feed same inputs as Rust tests, compare root hashes
4. **WAL recovery**: Simulate crashes at each sync phase, verify recovery
5. **Integration**: Process historical Ethereum blocks, verify state root matches
6. **Benchmarks**: Compare commit latency and throughput vs. HashDB/PathDB backends

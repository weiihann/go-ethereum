# Page-Packed Storage vs PR #33658 Grouped Serialization

Two approaches to reducing disk I/O for the binary trie (EIP-7864).
Both group InternalNodes to amortize PebbleDB read cost. They differ
in **where** grouping happens and **what** gets stored on disk.

---

## 1. Architecture: Where Grouping Happens

PR #33658 groups at the **trie serialization layer** (`trie/bintrie/`).
The pathdb layer receives pre-grouped blobs and stores them as-is.

Our approach groups at the **storage layer** (`triedb/pathdb/`).
The trie layer is untouched — it still emits individual 65B blobs.

```
PR #33658                              Ours (Page-Packed)
─────────                              ──────────────────

┌──────────────────┐                   ┌──────────────────┐
│    BinaryTrie    │                   │    BinaryTrie    │
│                  │                   │                  │
│  CollectNodes()  │                   │  CollectNodes()  │
│  fires every g   │                   │  fires at EVERY  │
│  levels          │                   │  node (unchanged)│
│                  │                   │                  │
│  SerializeNode() │                   │  SerializeNode() │
│  walks subtree,  │                   │  produces one    │
│  emits one fat   │                   │  65-byte blob:   │
│  blob per group  │                   │  [type|Lh|Rh]    │
└────────┬─────────┘                   └────────┬─────────┘
         │                                      │
    one blob per                          one 65B blob
    group boundary                        per node
         │                                      │
         ▼                                      ▼
┌──────────────────┐                   ┌──────────────────┐
│     pathdb       │                   │     pathdb       │
│                  │                   │                  │
│  writeNodes()    │                   │  writeNodes()    │
│  stores blob     │                   │  groups blobs    │
│  as-is into DB   │                   │  into 63-slot    │
│  (no regrouping) │                   │  pages, then     │
│                  │                   │  writes pages    │
└────────┬─────────┘                   └────────┬─────────┘
         │                                      │
         ▼                                      ▼
┌──────────────────┐                   ┌──────────────────┐
│    PebbleDB      │                   │    PebbleDB      │
└──────────────────┘                   └──────────────────┘
```

---

## 2. What Gets Stored On Disk

Consider this depth-4 subtree as a running example:

```
In-memory trie (identical for both approaches):

              [Root]                  depth 0
             /      \
          [A]        [B]             depth 1
         /   \      /   \
       [C]   [D]  [E]   [F]         depth 2
       / \   / \  / \   / \
      G   H I  J K  L  M   N        depth 3 — group/page boundary
      ▲                              (children: next group or StemNodes)
      │
      Each of Root..F is an InternalNode.
      Each of G..N lives in the next group/page down.
```

### 2a. PR #33658: Bottom-Layer Hashes Only

Intermediate InternalNodes (Root, A–F) are **not stored**.
Only the hashes of the children at the group boundary (G–N) are written.

```
On disk — one PebbleDB entry per group:

key: path to group root

value:
┌──────┬────────────┬──────────┬─────────────────────────────────┐
│ type │ groupDepth │  bitmap  │     bottom-layer hashes         │
│ 0x02 │    0x04    │  1 byte  │ H(G) H(H) H(I) ... H(N)        │
│  1B  │     1B     │  8 bits  │     up to 16 x 32B = 512B       │
└──────┴────────────┴──────────┴─────────────────────────────────┘
                                       Total: ~514 bytes

What IS stored:      H(G), H(H), H(I), H(J), H(K), H(L), H(M), H(N)
What is NOT stored:  Root, A, B, C, D, E, F  (reconstructed on read)
```

On deserialization, `deserializeSubtree` recursively rebuilds the
entire tree of 7 InternalNodes from the bottom-layer hashes:

```
deserializeSubtree(remaining=4, position=0)
│
├─ deserializeSubtree(remaining=3, position=0)       builds [A]
│  ├─ deserializeSubtree(remaining=2, position=0)    builds [C]
│  │  ├─ remaining=0, bitmap bit 0 set → HashedNode(H(G))
│  │  └─ remaining=0, bitmap bit 1 set → HashedNode(H(H))
│  │  return &InternalNode{left: H(G), right: H(H)}       ← new [C]
│  │
│  └─ deserializeSubtree(remaining=2, position=1)    builds [D]
│     ├─ remaining=0, bitmap bit 2 set → HashedNode(H(I))
│     └─ remaining=0, bitmap bit 3 set → HashedNode(H(J))
│     return &InternalNode{left: H(I), right: H(J)}       ← new [D]
│
│  return &InternalNode{left: [C], right: [D]}             ← new [A]
│
├─ (same for right side, builds [B] from [E],[F])
│
└─ return &InternalNode{left: [A], right: [B]}             ← new [Root]

Result: 7 fresh InternalNodes, ALL with hash = uncomputed
```

### 2b. Ours: Full 65-Byte Node Blobs

Every InternalNode in the page is stored as its complete 65-byte
serialization: `[1B type][32B left_hash][32B right_hash]`.

```
On disk — one PebbleDB entry per page:

key: "P" + bitpack(page_prefix)

value:
┌─────────┬──────────┬────────────────────────────────────────┐
│ version │  bitmap  │       node blobs (65B each)            │
│  0x01   │  uint64  │                                        │
│   1B    │   8B     │                                        │
└─────────┴──────────┴────────────────────────────────────────┘

The bitmap has 63 bits. Bit i means slot i is populated.
Slots use binary-heap indexing within the depth-6 subtree:

                   slot 0 ← page root
                  /       \
            slot 1         slot 2
           /     \        /     \
        slot 3  slot 4  slot 5  slot 6
        ...                        ...
     slots 31 ──────────────── slot 62     (depth 5)

Each populated slot holds one blob:

  slot 0 (Root): [0x02 | H(A)                | H(B)               ]
                        ├─ 32 bytes ─────────┤├─ 32 bytes ────────┤
                        left child's hash      right child's hash

  slot 1 (A):    [0x02 | H(C)                | H(D)               ]
  slot 2 (B):    [0x02 | H(E)                | H(F)               ]
  slot 3 (C):    [0x02 | H(G)                | H(H)               ]
  slot 4 (D):    [0x02 | H(I)                | H(J)               ]
  slot 5 (E):    [0x02 | H(K)                | H(L)               ]
  slot 6 (F):    [0x02 | H(M)                | H(N)               ]
                                      Total: 9 + 7 x 65 = 464 bytes
```

On read, the trie layer receives one blob and deserializes normally:

```
ReadNodeFromPage(path=[0,1])
  → look up pageKey, load page from DB
  → populate clean cache with ALL 63 slots     ← sibling warm-up
  → return blob at slot 1

Trie layer deserializes the blob:
  [0x02 | H(C) | H(D)]
  → InternalNode{ left: HashedNode(H(C)), right: HashedNode(H(D)) }

No tree reconstruction. One node. Children hashes already present.
```

---

## 3. Hash Recomputation After Deserialization

This is the fundamental performance difference between the approaches.

### 3a. PR #33658: Cascade Required

After deserializing a group, all intermediate InternalNodes have
**no cached hash**. The first call to `Root.Hash()` must cascade
through every intermediate node, computing bottom-up:

```
Root.Hash() = SHA256( A.Hash() || B.Hash() )
                       │            │
              ┌────────┘            └────────┐
              ▼                              ▼
   A.Hash() = SHA256( C.Hash() || D.Hash() )
                       │            │         B.Hash() = SHA256(...)
              ┌────────┘            │                     │
              ▼                     ▼                     ▼
   C.Hash() = SHA256(H(G)||H(H))  D.Hash() = SHA256(...)  (same pattern)
                  ▲       ▲
                  │       │
              HashedNode HashedNode     ← only THESE have known hashes

SHA256 calls: 7  (one per intermediate InternalNode = 2^g - 1)
```

These hashes are cached in memory after computation. But on the next
serialize → deserialize cycle, the cache is lost because intermediate
hashes are not stored on disk. The cascade repeats every cycle.

### 3b. Ours: No Cascade

Each node's blob contains its children's hashes. Computing any
single node's hash requires exactly one SHA256 call:

```
Root.Hash() = SHA256( H(A) || H(B) )
                       ▲       ▲
                       │       │
              stored in Root's blob (bytes 1-32, 33-64)

SHA256 calls: 1  (children hashes already in the blob)
```

No cascade. No dependency on sibling nodes' hashes. The structural
information survives the serialize/deserialize cycle because each
blob explicitly stores its children's hashes.

---

## 4. Write Path Comparison

Scenario: modify one leaf at depth 248, then commit.

### 4a. PR #33658 Write Path

```
Step 1: Commit → root.Hash()
        Cascades through every deserialized group.
        Each group requires 2^g - 1 SHA256 ops to reconstruct
        intermediate hashes (lost during last serialization).

Step 2: CollectNodes(groupDepth=g)
        Fires at every group boundary (every g levels).
        At each boundary: SerializeNode → serializeSubtree
          walks ALL 2^g - 1 positions to collect bottom-layer hashes.
          (visits unchanged siblings too — no dirty tracking within groups)

Step 3: Write to DB
        batch.Put(groupKey, blob)
        No extra DB read needed — serialized from in-memory tree.
```

### 4b. Our Write Path

```
Step 1: Commit → root.Hash()
        Each dirty node: SHA256(stored_left_hash || stored_right_hash)
        One SHA256 per dirty node. No cascade into siblings.

Step 2: CollectNodes (unchanged trie layer)
        Fires at EVERY node. Produces individual 65B blobs.
        No subtree traversal — each node serializes independently.

Step 3: writeNodes() in pathdb
        Groups dirty blobs by page key.
        For each dirty page:
          READ existing page from DB (or clean cache)    ← extra read
          Patch dirty slots with new blobs
          WRITE full page to batch
          Populate clean cache with all 63 nodes         ← sibling warm-up
```

### 4c. Concrete Cost Comparison (one leaf at depth 248)

```
                          PR #33658    PR #33658     Ours
                           (GD-4)      (GD-8)    (PageDepth=6)
                          ─────────   ─────────   ────────────
Hash computation:
  Dirty path SHA256s          248         248          248
  Cascade from deser       + 930      + 7,905       +   0
  TOTAL SHA256              1,178       8,153          248

Serialization:
  serializeSubtree visits    930       7,905            0
  (walks entire subtree    per group  per group     (slot patch
   including clean nodes)                            only)

DB operations on flush:
  Reads                        0           0           42 *
  Writes                      62          31           42

Bytes written per flush:    ~32KB      ~248KB        ~168KB

InternalNode allocs
  on deserialization:
  Per group                   15         255            0 **
  Total (full path)          930       7,905            0

─────────────────────────────────────────────────────────────
*  Mitigated by fastcache — often served from clean cache,
   not actual disk reads.

** Individual blob deserialization, not tree reconstruction.
   The trie layer creates one InternalNode per resolved node.
```

---

## 5. Read Path Comparison

Scenario: cold read of one key at depth 248 (nothing in cache).

### 5a. PR #33658 Read Path

```
For each group on the path (248/g groups):
  1. DB Get → load group blob
  2. DeserializeNode → deserializeSubtree
     Creates 2^g - 1 InternalNodes (tree reconstruction)
  3. Navigate to correct child (follow key bit g times)
  4. Child at boundary is HashedNode → resolve next group

DB Gets:           248/g  (62 for GD-4, 31 for GD-8)
Nodes allocated:   (2^g - 1) * (248/g)
No sibling caching — only the traversed path is in memory.
```

### 5b. Our Read Path

```
For each page on the path (248/6 = 42 pages):
  1. Check clean cache for the specific node → MISS on first access
  2. DB Get → load page blob
  3. DeserializePage → extract requested slot
  4. populateCleanCacheFromPage → insert ALL 63 nodes into fastcache
  5. Subsequent reads from same page → cache HIT (0 DB Gets)

DB Gets:           42 cold, then 0 for siblings
Cache entries:     42 * 63 = 2,646 nodes warmed
Next read sharing any page → instant (no DB, no deserialization)
```

---

## 6. The Core Trade-off

```
PR #33658 (Grouped Serialization)       Ours (Page-Packed Storage)
─────────────────────────────────       ─────────────────────────────

Compact on disk                         Preserves structural info
  only bottom-layer 32B hashes            full 65B blobs per node
  ~514B per GD-4 group                    ~4KB per depth-6 page

No read-modify-write                    Zero cascade rehashing
  serialize directly from memory          children hashes in every blob
  no extra DB reads on flush              no lost hashes across cycles

Smaller total bytes written (GD-4)      Sibling cache warm-up
  ~32KB per modification                  one DB read → 63 cached nodes

Must rebuild 2^g-1 InternalNodes        Read-modify-write on flush
  fresh allocations every deser           extra DB read per dirty page
                                          (mitigated by clean cache)

Must cascade 2^g-1 SHA256s              Larger blobs on disk
  every serialize/deserialize cycle       65B vs 32B per node position

Must traverse 2^g-1 positions           More total bytes written
  during serializeSubtree                 ~168KB per modification
  even for unchanged siblings
```

**In one sentence:** PR #33658 discards intermediate hashes to save disk
space, then pays to reconstruct them every cycle. Our approach preserves
them in every blob, paying more space but avoiding recomputation.

---

## 7. Potential Hybrid

The ideal approach would combine both strengths:

- **Store children hashes** (like ours) to avoid cascade rehashing
- **Serialize from memory** (like PR #33658) to avoid read-modify-write
- **Sibling cache warm-up** (like ours) to amortize cold reads

This would mean moving the page-packing into the trie layer's
`CollectNodes`/`SerializeNode`, so the trie emits pre-packed page
blobs directly. The pathdb layer would store them as-is (no
read-modify-write) while still populating the clean cache on reads.

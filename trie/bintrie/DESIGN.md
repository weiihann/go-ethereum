# Binary Trie Hashing & Commit Optimization

Design doc for the allocation-elimination and commit-path optimizations
applied to `trie/bintrie/` in the go-ethereum EIP-7864 binary trie.

## Problem

The binary trie's `Hash()` and `Commit()` paths allocated heavily on the
heap, causing GC pressure proportional to trie size. For a trie with 100K
entries, hashing produced over 1 million allocations per call.

Three root causes:

1. `h.Sum(nil)` returns a new `[]byte` on each call (heap-allocated).
2. `common.BytesToHash(slice)` copies into a new `[32]byte` (redundant).
3. `CollectNodes` allocated a `[256]byte` path buffer per recursive call.

## Architecture

```
                        Binary Trie Structure
                        =====================

                         +----------------+
                         | InternalNode   |
                         | depth=0        |
                         | left    right  |
                         +---+--------+---+
                             |        |
                    bit=0 --+          +-- bit=1
                             |        |
                    +--------+--+  +--+--------+
                    | Internal  |  | Internal  |
                    | depth=1   |  | depth=1   |
                    +--+-----+--+  +--+-----+--+
                       |     |        |     |
                      ...   ...      ...   ...
                       |     |        |     |
                  +----+--+--+----+---+     |
                  |StemNode| |StemNode|    (more)
                  |stem=31B| |stem=31B|
                  |vals[256]| |vals[256]|
                  +---------+ +---------+


    Key structure:  [<---- 31-byte stem ---->][1-byte suffix]
                    Navigated bit-by-bit       Indexes into
                    through InternalNodes      StemNode.Values
```

Each `StemNode` groups 256 values under a shared 31-byte stem (248 bits).
Internal nodes form the bit-by-bit path from root to stem. The trie depth
can reach up to 248 levels for divergent stems.

## StemNode Internal Merkle Tree

The 256 values inside a `StemNode` are hashed into a single root via an
8-level binary Merkle tree built bottom-up in a flat `[256]common.Hash`
array:

```
    StemNode.Hash() -- 8-level internal Merkle tree
    =================================================

    Level 0 (leaves):  data[0]  data[1]  data[2]  data[3] ... data[255]
                          |        |        |        |           |
                        SHA256   SHA256   SHA256   SHA256     SHA256
                          |        |        |        |           |
                        val[0]  val[1]  val[2]  val[3]      val[255]

    Level 1 (128 nodes): data[0] = SHA256(data[0] || data[1])
                         data[1] = SHA256(data[2] || data[3])
                         ...
                         data[127] = SHA256(data[254] || data[255])

    Level 2 (64 nodes):  data[0] = SHA256(data[0] || data[1])
                         ...
                         data[63] = SHA256(data[126] || data[127])

    ...

    Level 8 (1 node):    data[0] = root of values tree

    Final:               hash = SHA256(stem || 0x00 || data[0])


    Array reuse pattern (level 1 example):
    +-------+-------+-------+-------+-----+-------+-------+
    |  [0]  |  [1]  |  [2]  |  [3]  | ... | [254] | [255] |
    +-------+-------+-------+-------+-----+-------+-------+
        |                                       |
        v  SHA256(old[0] || old[1])              v  (unused after level 1)
    +-------+-------+-------+-------+-----+
    |  [0]  |  [1]  |  [2]  | ...   |[127]|
    +-------+-------+-------+-------+-----+
        |       SHA256(old[2] || old[3])
        v
    Each level halves the active range, writing results
    into the low half of the same array. No extra memory.
```

## Optimization 1: Eliminate h.Sum(nil) Allocations

### Before

```go
// InternalNode.Hash()
h.Write(bt.left.Hash().Bytes())        // .Bytes() copies to new []byte
bt.hash = common.BytesToHash(h.Sum(nil)) // h.Sum(nil) allocs, BytesToHash copies

// StemNode.Hash() inner loop
data[i] = common.Hash(h.Sum(nil))       // h.Sum(nil) allocs []byte on heap
```

### After

```go
// InternalNode.Hash()
leftHash := bt.left.Hash()
h.Write(leftHash[:])          // slice of stack value, no alloc
h.Sum(bt.hash[:0])            // write directly into bt.hash, no alloc

// StemNode.Hash() inner loop
h.Sum(data[i][:0])            // write directly into array slot, no alloc
```

### How h.Sum(buf[:0]) works

```
    h.Sum(nil)          h.Sum(data[i][:0])
    ==========          ==================

    +--------+          +--------+
    | hasher |          | hasher |
    | state  |          | state  |
    +---+----+          +---+----+
        |                   |
        v                   v
    allocate new        append to data[i]'s
    []byte{32}          backing array
        |                   |
        v                   v
    return new slice    return data[i][:32]
    (heap alloc)        (zero alloc, writes
                         in place)
```

`h.Sum(buf)` appends the hash to `buf`. When `buf` is `data[i][:0]`, it
has length 0 but capacity 32 (backing array is `common.Hash` = `[32]byte`).
The append writes directly into the existing array with no allocation.

### Leaf hashing: sha256.Sum256 vs sync.Pool

```
    sha256.Sum256(v)            sync.Pool hasher
    ================            ================

    Stack frame:                Pool:
    +--------------+            +-----+
    | digest [32]B |            | Get  |---> heap-allocated
    | (stack alloc)|            +-----+     hash.Hash
    +--------------+                |
         |                         v
         v                    interface dispatch
    return [32]byte           h.Reset()
    (value type,              h.Write(v)
     zero GC)                 h.Sum(nil) or h.Sum(buf[:0])
                              Pool.Put(h)
                                   |
                                   v
                              3 interface calls +
                              pool sync overhead

    Winner: sha256.Sum256 -- stack-allocated, no GC, no dispatch
```

`sha256.Sum256(v)` returns a `[32]byte` value type. Go allocates the
internal digest on the stack. A `sync.Pool`-based hasher was benchmarked
and found to be slower due to interface dispatch overhead and pool
synchronization, adding ~40% latency to key encoding operations.

## Optimization 2: Skip h.Reset() for Empty Pairs

### Before

```go
for i := range StemNodeWidth / (1 << level) {
    h.Reset()                    // <-- always reset, even if skipping
    if data[i*2] == zero && data[i*2+1] == zero {
        data[i] = common.Hash{}
        continue
    }
    h.Write(data[i*2][:])
    h.Write(data[i*2+1][:])
    data[i] = common.Hash(h.Sum(nil))
}
```

### After

```go
for i := range StemNodeWidth / (1 << level) {
    if data[i*2] == zero && data[i*2+1] == zero {
        data[i] = common.Hash{}
        continue                 // <-- skip Reset entirely
    }
    h.Reset()                    // <-- only reset when needed
    h.Write(data[i*2][:])
    h.Write(data[i*2+1][:])
    h.Sum(data[i][:0])
}
```

For sparse `StemNode`s (few values set), most pairs are empty. Moving
`h.Reset()` after the zero-check avoids unnecessary work.

## Optimization 3: Shared Path Buffer in CollectNodes

`CollectNodes` walks the entire in-memory trie to serialize each node.
The path grows by one bit per level of recursion.

### Before

```
    Each recursive call allocated its own [256]byte:

    CollectNodes(path=[])
      |
      +-- var p [256]byte          <-- alloc #1
      |   copy(p[:], path)
      |   childpath = append(p[:0], 0)
      |   left.CollectNodes(childpath)
      |     |
      |     +-- var p [256]byte    <-- alloc #2
      |     |   copy(p[:], childpath)
      |     |   ...
      |     |     |
      |     |     +-- var p [256]byte  <-- alloc #3
      |     |         ...
      |
      +-- var p [256]byte          <-- alloc #4 (for right child)
          ...

    Total: one [256]byte stack allocation per node visited
```

### After

```
    Single buffer allocated at the top, shared via append/truncate:

    CollectNodes(path=[])
      |
      +-- var buf [256]byte        <-- ONE alloc at top level
      |   collectNodes(buf[:0])
      |     |
      |     +-- childpath = append(path, 0)   // buf[0] = 0
      |     |   n.collectNodes(childpath)      // pass same buf
      |     |     |
      |     |     +-- childpath = append(path, 0)  // buf[1] = 0
      |     |         n.collectNodes(childpath)     // same buf
      |     |
      |     +-- childpath[len(path)] = 1      // flip bit in place
      |         n.collectNodes(childpath)      // same buf
      |
      +-- (buf is shared through entire recursion)

    Total: one [256]byte allocation for the entire tree walk
```

The type switch on `*InternalNode` calls the unexported `collectNodes`
directly, avoiding the public `CollectNodes` which would re-allocate
the buffer:

```go
switch n := bt.left.(type) {
case *InternalNode:
    n.collectNodes(childpath, flushfn)  // stays in shared-buffer path
default:
    n.CollectNodes(childpath, flushfn)  // leaf types (StemNode, etc.)
}
```

## Optimization 4: Pre-compute Root Hash in Commit

### Before

```
    Commit()
      |
      +-- CollectNodes(root)
      |     |
      |     +-- for each node:
      |           SerializeNode(node)
      |           node.Hash()           <-- triggers lazy hash computation
      |                                     during serialization walk
      +-- t.Hash()                      <-- recomputes root hash AGAIN
      |
      +-- return rootHash, nodeset
```

### After

```
    Commit()
      |
      +-- rootHash = t.root.Hash()      <-- compute ALL hashes upfront
      |     |                               (single recursive pass)
      |     +-- InternalNode.Hash()
      |           +-- left.Hash()  (recursive)
      |           +-- right.Hash() (recursive)
      |           +-- SHA256(left || right)
      |           +-- cache in bt.hash, mustRecompute = false
      |
      +-- CollectNodes(root)
      |     |
      |     +-- for each node:
      |           SerializeNode(node)
      |           node.Hash()           <-- returns cached hash (free)
      |
      +-- return rootHash, nodeset      <-- no redundant recomputation
```

Pre-computing separates the hashing phase from the serialization phase.
All `node.Hash()` calls during `CollectNodes` hit the cache
(`mustRecompute == false`) and return immediately.

## Files Changed

```
trie/bintrie/
  stem_node.go       StemNode.Hash(): h.Sum(data[i][:0]), skip Reset
  internal_node.go   InternalNode.Hash(): h.Sum(bt.hash[:0])
                     CollectNodes: shared [256]byte buffer
  trie.go            Commit: pre-compute root hash
  bench_test.go      NEW: comprehensive benchmark suite (11 benchmarks)
```

## Benchmark Results

All measurements: 10 runs, compared with benchstat (p < 0.05).

```
Benchmark                          Before        After       Delta
==================================================================

Hash/100                           37.4us        32.1us      -14%
Hash/1K                            441us         378us       -14%
Hash/10K                           4.69ms        4.02ms      -14%
Hash/100K                          51.2ms        42.5ms      -17%

Hash allocs/op (100K)              1,050,869     0           -100%
Hash B/op (100K)                   33.6MB        0B          -100%

HashIncremental/1K                 51.2us        43.4us      -15%
HashIncremental/10K                489us         435us       -11%
HashIncremental/100K               5.57ms        4.80ms      -14%

Commit/100                         83.5us        70.8us      -15%
Commit/1K                          1.02ms        0.95ms      -7%
Commit/10K                         10.5ms        10.0ms      -5%

Commit allocs/op (10K)             126,490       19,677      -84%
Commit B/op (10K)                  11.7MB        10.5MB      -10%

GetBinaryTreeKey                   93.3ns        93.9ns       ~0%
==================================================================
```

## Approaches Evaluated and Rejected

### sync.Pool for SHA256 hashers

A `sync.Pool` of `hash.Hash` objects was tested to avoid `sha256.New()`
allocations. Result: 42% regression in `GetBinaryTreeKey` (93ns to 131ns)
due to interface dispatch overhead on `Pool.Get()`, `h.Reset()`, and
`Pool.Put()`. The standard library's `sha256.Sum256()` uses a
stack-allocated digest internally, making it faster than any pooled
approach for single-hash operations.

### Parallel goroutine hashing

At shallow tree depths, left and right subtrees were hashed concurrently
using goroutines (matching the pattern in `trie/hasher.go:141-156`).
Result: 3.7x wall-clock speedup but 25x memory overhead (33MB to 828MB
B/op for 100K entries) from goroutine stack allocations. The memory cost
is unacceptable for a function called on every block commit.

# PBT parallel commit — performance problem

## TL;DR

The shallow-depth goroutine fan-out in `trie/bintrie/store_commit.go` (`hashInternal`) is **net-negative** on this branch. End-to-end benchmark numbers show PBT pays a per-block hash+commit tax of **~15–20 ms** that UBT does not. The hash code path is structurally the same in both branches; the only thing that differs is the trie shape PBT creates (via the zone-prefix key derivation) and the fact that PBT's `BinaryTrie.Hash()` actually walks more *shallow* internal nodes per commit. Those shallow nodes are exactly the depths where this file spawns goroutines, and that overhead appears to dominate the gain.

The problem is to characterise the goroutine fan-out behaviour under PBT's tree shape and produce a faster implementation. This document only states the problem and points at the exact code involved — it does not prescribe a fix.

## Symptom (from the locality-sweep benchmark)

Identical 1-tx blocks of ~6 M gas each, identical state, identical EVM workload across configs. Block-shape gate passes: `gas_used` byte-identical UBT vs PBT per cell across all 480 runs.

| benchmark (per-block median, ms) | UBT `state_hash` | PBT `state_hash` | UBT `commit` | PBT `commit` |
|---|---:|---:|---:|---:|
| pure-read 256-touch block | ~2 | ~15–20 | ~1 | ~5–8 |
| write 256-touch block | ~110 | ~125 | ~50 | ~58 |

So even on a **pure-read block** — where the trie state itself hasn't changed and the only commit work is recomputing root hashes — PBT spends an extra ~15 ms in `state_hash_ms` versus UBT. Multiply by every block, and that tax outweighs the disk-read savings PBT's clustering does provide. Per the locality sweep report: PBT/UBT total throughput ratio is 0.54–0.97 across all 12 cells; the gap correlates with `state_hash_ms + commit_ms`, not `state_read_ms`.

This shouldn't happen on a pure-read block. The smoking gun is that PBT's hash time scales with the **number of internal nodes the root-hash walk visits** rather than with the number of leaves changed, and PBT's tree shape (zone prefix → wider sparse paths near the root) makes that walk visit more shallow internal nodes than UBT does.

## Where the parallel commit lives

Branch: `binary/pbt-flat-state`. File: `trie/bintrie/store_commit.go`.

Call chain:

```
BinaryTrie.Hash()                                       trie/bintrie/trie.go:310
  → store.computeHash(store.root)                       trie/bintrie/trie.go:311
    → nodeStore.computeHash(ref)                        trie/bintrie/store_commit.go:36
      switch ref.Kind() { case kindInternal:
        → nodeStore.hashInternal(idx)                   trie/bintrie/store_commit.go:70
          ↻ recurses via computeHash into left/right children

BinaryTrie.Commit(_ bool) (common.Hash, *trienode.NodeSet)
                                                        trie/bintrie/trie.go:316
  reuses the same hashInternal traversal.
```

The hot function is `hashInternal`. Below is the goroutine-spawning branch as it stands in `store_commit.go:86-109`, verbatim:

```go
// store_commit.go (line numbers as shown)

// 51 // parallelHashDepth is the tree depth below which hashInternal spawns
// 52 // goroutines for shallow-depth parallelism. Computed once at init because
// 53 // NumCPU() never changes after startup.
// 54 var parallelHashDepth = min(bits.Len(uint(runtime.NumCPU())), 8)

// 70 func (s *nodeStore) hashInternal(idx uint32) common.Hash {
// 71     node := s.getInternal(idx)
// 72     if !node.mustRecompute {
// 73         return node.hash
// 74     }
// 75
// 76     if s.groupDepth > 0 && int(node.depth)%s.groupDepth == 0 {
// 77         bitmapSize := bitmapSizeForDepth(s.groupDepth)
// 78         bitmap := make([]byte, bitmapSize)
// 79         var hashes []common.Hash
// 80         s.serializeSubtree(makeRef(kindInternal, idx), s.groupDepth, 0, int(node.depth), bitmap, &hashes)
// 81         node.hash = groupedRecursiveHash(s.groupDepth, bitmap, hashes)
// 82         node.mustRecompute = false
// 83         return node.hash
// 84     }
// 85
// 86     if int(node.depth) < parallelHashDepth {
// 87         var input [64]byte
// 88         var lh common.Hash
// 89         var wg sync.WaitGroup
// 90         if !node.left.IsEmpty() {
// 91             wg.Add(1)
// 92             go func() {
// 93                 // defer wg.Done() so a panic in computeHash still releases
// 94                 // the waiter; without this, a recover() higher in the call
// 95                 // stack would leave the parent stuck in wg.Wait forever.
// 96                 defer wg.Done()
// 97                 lh = s.computeHash(node.left)
// 98             }()
// 99         }
// 100        if !node.right.IsEmpty() {
// 101            rh := s.computeHash(node.right)
// 102            copy(input[32:], rh[:])
// 103        }
// 104        wg.Wait()
// 105        copy(input[:32], lh[:])
// 106        node.hash = sha256.Sum256(input[:])
// 107        node.mustRecompute = false
// 108        return node.hash
// 109    }
// 110
// 111    // Deep sequential branch — mirrors the shallow branch's shape to keep
// 112    // input on the stack. Writing lh/rh through hash.Hash (interface)
// 113    // forces escape; copy into a local [64]byte and hash it in one shot.
// 114    var input [64]byte
// 115    if !node.left.IsEmpty() {
// 116        lh := s.computeHash(node.left)
// 117        copy(input[:HashSize], lh[:])
// 118    }
// 119    if !node.right.IsEmpty() {
// 120        rh := s.computeHash(node.right)
// 121        copy(input[HashSize:], rh[:])
// 122    }
// 123    node.hash = sha256.Sum256(input[:])
// 124    node.mustRecompute = false
// 125    return node.hash
// 126 }
```

### Mechanics of the shallow branch

- `parallelHashDepth = min(bits.Len(NumCPU()), 8)`. On the benchmark machine (Xeon 8358, 8 cores), `bits.Len(8) = 4`, so the parallel branch is taken for nodes at depths `0, 1, 2, 3` — i.e. the top four levels of the trie. That's up to **15 internal nodes** in a fully populated top, and in practice (sparse tree) a handful per commit.
- At each of those depths, if the node has a non-empty `left` child:
  - `wg.Add(1)`
  - `go func() { defer wg.Done(); lh = s.computeHash(node.left) }()`
  - the goroutine recurses through `computeHash → hashInternal` on the left subtree (which may itself spawn more goroutines at the next two depths).
- The current goroutine concurrently runs `rh = s.computeHash(node.right)` inline on the right subtree.
- Then `wg.Wait()`, `sha256.Sum256(input[:])`, cache.
- Below `parallelHashDepth`, the code falls through to the sequential branch (115–125) which does the same work without any goroutine/`WaitGroup` overhead.
- There is also a group-boundary fast path (76–84) that bypasses both branches when `depth % groupDepth == 0` and the subtree fits in one group blob; it uses `groupedRecursiveHash` and never spawns.

### Properties worth noting

- **Goroutines spawn per node, not per commit.** Up to ~15 goroutine launches at the top of the tree per `Hash()` call — every commit, not amortised.
- **No work threshold.** The branch chooses parallel-vs-sequential purely on `node.depth < parallelHashDepth`. It does not look at how much hashing is below the node. A shallow node whose left subtree is a single stem (i.e. one `sha256` to compute) still spawns a goroutine to do that one hash.
- **`defer wg.Done()` and stack-allocated `wg`.** Cheap but non-zero, and adds an indirect call through the runtime.
- **`lh` is captured by reference and written by the goroutine, read after `wg.Wait`.** No data race (the `wg.Wait` synchronises), but the compiler must heap-allocate `lh` because the goroutine's lifetime escapes the enclosing stack frame.
- **The recursion can nest goroutines.** A depth-0 hashInternal spawns a goroutine which recurses into a depth-1 internal node, which spawns another goroutine for *its* left child, and so on down to `parallelHashDepth-1`. At 4 levels with both children non-empty, that's up to 15 goroutines per top-of-tree walk.

## Why PBT pays more than UBT through this code path

Both branches share `store_commit.go` byte-for-byte. The only thing that changed between UBT and PBT is **how trie keys are derived**, which determines the *shape* of the tree the hash walk visits:

- UBT (`feat/binary-trie/flat-state`): `key = sha256(addr ‖ slot)`. Keys are uniformly random across the 256-bit space. The tree is balanced; near the root, internal nodes are dense (both children populated, group-boundary fast-path applies at every group boundary).
- PBT (`binary/pbt-flat-state`): `key = zone_prefix ‖ H(addr) ‖ slot` (see `trie/bintrie/key_encoding.go`). The 3-bit zone prefix lives in the **top of the key**, which means the top of the trie has only 2–3 populated branches out of 8 possible — wide *sparse* fanout near the root. Below the zone bits the keys diverge in a long chain of single-child internals before the next dense region.

Net effect: PBT's `Hash()` walks a noticeably different shape, where the shallow-depth region (where the parallel branch fires) contains more single-child / sparse internal nodes than UBT's tree does. The empirical signature in the benchmark logs:

- pure-read block (no leaves dirty, root recomputed only because cache was dropped): UBT `state_hash_ms ≈ 2`, PBT `state_hash_ms ≈ 15–20`. **Same workload, same `hashInternal` source, ~8× longer on PBT.**
- write block (real hashing both sides): the gap shrinks (UBT 110 vs PBT 125), consistent with the per-spawn overhead being amortised once each spawned goroutine has substantial work.

The pure-read case is the clean one: when the *actual* hash work is small, every spawn cost shows up. PBT's tree shape ensures more spawns at shallow depths over thinner subtrees, and that's the regime where this branch was supposed to help.

## What an agent picking this up should investigate

1. **Measure spawn count and per-spawn work distribution** for one PBT `Hash()` call on a representative pure-read block. The right tool is to instrument `hashInternal` (or use `runtime/trace`) and record `(depth, leaves_below, spawned)` for every call. Hypothesis to confirm: median spawned subtree contains very few leaves (≤ a handful of stem leaves), so the goroutine overhead exceeds the work it parallelises.
2. **Profile** a tight loop of `BinaryTrie.Hash()` calls on a PBT-shaped tree (pprof CPU + block + sched-latency). Watch for runtime scheduling overhead and `runtime.gopark`/`runtime.goready` cost in the `hashInternal` stack.
3. **Characterise the threshold.** `parallelHashDepth = min(bits.Len(NumCPU()), 8)` is a static, depth-only threshold with no notion of subtree size. The actual break-even depends on how many `sha256` operations a subtree contains. Quantify what subtree-size threshold actually pays off on this hardware.
4. **Examine the group-boundary interaction.** Lines 76–84 short-circuit hashInternal into `groupedRecursiveHash` for nodes exactly on group boundaries. Confirm whether PBT's shallow tree puts most of the off-boundary work onto the parallel branch (likely) and whether shifting more work onto the group path is feasible (open question).
5. **Determine the cost contributions.** Decompose the 15–20 ms PBT pure-read tax into: (a) goroutine spawn/teardown, (b) `wg.Add/Done/Wait` synchronisation, (c) heap allocation of captured `lh`, (d) the actual `sha256` hashing. Only (d) is unavoidable; (a–c) are candidates for elimination.

## Reproducing the symptom

Build the PBT geth from this branch, run the `ubt-vs-pbt-benchmarks` locality sweep with the small-K cells (1 or 10), and inspect the per-block slow-block log:

- Repository: `weiihann/bintrie-benchmarks` branch `pbt`.
- Benchmark: `bash ubt-vs-pbt/scripts/run_campaign.sh` with `NUM_RUNS=5 NUM_CONTRACTS=256 K_VALUES_STORAGE="1" BENCHMARKS="storage_sload" GAS_BENCHMARK_VALUE=6 COLD_CACHE=1 GROUP_DEPTH=5`.
- Inspect: `data/pbt/storage_sload_k1_run*_geth.log` — look for `"Slow block"` JSON entries; the `timing.state_hash_ms` and `timing.commit_ms` fields show the per-block PBT-side cost. Compare against the matching `data/ubt/…` log to see the UBT baseline.

The block-shape gate (`gas_used` identical between configs per cell) is what makes this an apples-to-apples comparison: the only thing that can produce a `state_hash_ms` delta at byte-identical EVM workload is the in-process hash code's cost on the differently-shaped tree.

## Source pointers

All paths relative to the worktree of this branch (`binary/pbt-flat-state`).

| What | Where |
|---|---|
| Parallel-spawn site | `trie/bintrie/store_commit.go:70–126` (`hashInternal`) |
| Depth threshold | `trie/bintrie/store_commit.go:51–54` (`parallelHashDepth`) |
| Hash dispatch | `trie/bintrie/store_commit.go:36–48` (`computeHash`) |
| Group-boundary fast path | `trie/bintrie/store_commit.go:76–84` + `groupedRecursiveHash` at `:136` |
| Trie-level entry points | `trie/bintrie/trie.go:310` (`Hash`), `:316` (`Commit`) |
| Key derivation (what shapes the tree) | `trie/bintrie/key_encoding.go` (`buildKey3Zone`, `buildKeyStorageZone`) |
| `nodeStore` definition (internal-node layout, `mustRecompute` flag) | `trie/bintrie/node_store.go` |
| Sequential reference for comparison | `trie/bintrie/store_commit.go:114–125` (deep branch — same logic without the goroutine) |

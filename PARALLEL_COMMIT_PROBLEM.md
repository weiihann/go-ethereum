# PBT parallel commit — performance problem

## TL;DR

PBT's per-block update pipeline partitions account/code/storage updates by zone
and applies them concurrently to the two halves of the trie. The mechanism is
**`BinaryTrie.SplitRoot` → two parallel `errgroup` goroutines (zone 000 left,
zone 1 right) → `BinaryTrie.MergeRoot`** in `core/state/statedb.go:592` and
`trie/bintrie/trie.go:367/398`. The parallel path is currently slower than the
sequential reference on the workloads we've measured: PBT pays ~15–20 ms of
hash+commit tax per pure-read block that UBT does not, and the tax correlates
with the per-block `SplitRoot` + `MergeRoot` work rather than with the parallel
storage-update work that runs between them.

The problem is to characterise the `SplitRoot` / `MergeRoot` cost and produce
a parallel design where the per-zone apply speedup actually nets out positive
end-to-end. This document only states the problem and points at the exact code
involved — it does not prescribe a fix.

## Symptom (from the locality-sweep benchmark)

Identical 1-tx blocks of ~6 M gas each, identical EVM workload across configs.
The block-shape gate passes: `gas_used` is byte-identical UBT vs PBT per cell
across all 480 runs.

| benchmark (per-block median, ms) | UBT `state_hash` | PBT `state_hash` | UBT `commit` | PBT `commit` |
|---|---:|---:|---:|---:|
| pure-read 256-touch block | ~2 | ~15–20 | ~1 | ~5–8 |
| write 256-touch block | ~110 | ~125 | ~50 | ~58 |

PBT's read benchmark fires `applyBinaryTrieUpdates` despite no leaves being
dirty (the only mutation is the coinbase / refund book-keeping at end-of-block).
That zero-mutation block still incurs the `SplitRoot` + zero-work-parallel-apply
+ `MergeRoot` round-trip, and the slow-block log shows the PBT-specific time
landing in `state_hash` and `commit`.

End-to-end, PBT/UBT total-throughput ratio is 0.54–0.97 across all 12 cells of
the locality sweep; the gap correlates with `state_hash_ms + commit_ms`, not
with `state_read_ms` (PBT's flat-state reads are actually faster than UBT's in
11/12 cells, but the parallel-commit tax outweighs the disk savings).

## The parallel commit pipeline

Call chain, top to bottom. All paths relative to the worktree of this branch
(`binary/pbt-flat-state`).

```
StateDB.IntermediateRoot
                                        core/state/statedb.go:1053
  if s.db.Type().Is(TypeUBT) {
    → s.applyBinaryTrieUpdates()         core/state/statedb.go:1060
  }

StateDB.applyBinaryTrieUpdates           core/state/statedb.go:592
  bt, ok := s.trie.(*bintrie.BinaryTrie)
  left, right, err := bt.SplitRoot()     trie/bintrie/trie.go:367
  → partition mutations by zone (left = zone 000, right = zone 1)
  workers := errgroup.Group
  workers.Go(rightApply)                 core/state/statedb.go:652
  workers.Go(leftApply)                  core/state/statedb.go:666
  workers.Wait()                         core/state/statedb.go:688
  bt.MergeRoot(left, right)              trie/bintrie/trie.go:398

BinaryTrie.Hash() / .Commit(_)           trie/bintrie/trie.go:310/316
  walks the (now-merged) trie via store.computeHash → hashInternal
```

The zone partition is determined by `isMainStorage` (statedb.go:606) — slots
≥ `bintrie.HeaderStorageSlots` (64) route to zone 1 (right); accounts, header
storage, and code go to zone 000 (left).

### `SplitRoot` — set up the sub-views

`trie/bintrie/trie.go:367–394`:

```go
func (t *BinaryTrie) SplitRoot() (left, right *BinaryTrie, err error) {
    if t.store.root.Kind() != kindInternal {
        return nil, nil, errors.New("SplitRoot: root is not an InternalNode")
    }
    rootNode := t.store.getInternal(t.store.root.Index())

    subDepth := uint8(t.baseDepth + 1)
    leftStore := newSubStore(subDepth)
    leftStore.root = leftStore.copyFrom(t.store, rootNode.left)   // <-- deep copy
    rightStore := newSubStore(subDepth)
    rightStore.root = rightStore.copyFrom(t.store, rootNode.right) // <-- deep copy

    left  = &BinaryTrie{store: leftStore,  reader: t.reader, /* ... */
                         baseDepth: t.baseDepth + 1}
    right = &BinaryTrie{store: rightStore, reader: t.reader, /* ... */
                         baseDepth: t.baseDepth + 1}
    return left, right, nil
}
```

Each sub-view gets its **own independent `nodeStore`** (via `newSubStore`), and
the root's left/right subtrees are deep-copied into them via `copyFrom`.

### `MergeRoot` — fold back

`trie/bintrie/trie.go:398–404`:

```go
func (t *BinaryTrie) MergeRoot(left, right *BinaryTrie) {
    rootNode := t.store.getInternal(t.store.root.Index())
    rootNode.left  = t.store.copyFrom(left.store,  left.store.root)  // <-- deep copy
    rootNode.right = t.store.copyFrom(right.store, right.store.root) // <-- deep copy
    rootNode.mustRecompute = true
    rootNode.dirty = true
}
```

Both sub-view roots get deep-copied **back** into the parent store. The parent
root is then marked `mustRecompute = true`, which discards its cached hash and
forces a fresh `computeHash` walk on the next `Hash()` call.

### `copyFrom` — the recursive deep-copy primitive

`trie/bintrie/node_store.go:211–248`:

```go
func (dst *nodeStore) copyFrom(src *nodeStore, srcRef nodeRef) nodeRef {
    switch srcRef.Kind() {
    case kindEmpty:
        return emptyRef
    case kindInternal:
        srcNode := src.getInternal(srcRef.Index())
        dstIdx := dst.allocInternal()
        dstNode := dst.getInternal(dstIdx)
        dstNode.depth         = srcNode.depth
        dstNode.mustRecompute = srcNode.mustRecompute
        dstNode.dirty         = srcNode.dirty
        dstNode.hash          = srcNode.hash
        dstNode.left          = dst.copyFrom(src, srcNode.left)     // recurse
        dstNode.right         = dst.copyFrom(src, srcNode.right)    // recurse
        return makeRef(kindInternal, dstIdx)
    case kindStem:
        srcStem := src.getStem(srcRef.Index())
        dstIdx  := dst.allocStem()
        dstStem := dst.getStem(dstIdx)
        dstStem.Stem          = srcStem.Stem
        dstStem.depth         = srcStem.depth
        dstStem.mustRecompute = srcStem.mustRecompute
        dstStem.dirty         = srcStem.dirty
        dstStem.hash          = srcStem.hash
        for i, v := range srcStem.values {
            if v == nil { continue }
            cp := make([]byte, len(v))           // <-- per-slot alloc
            copy(cp, v)
            dstStem.values[i] = cp
        }
        return makeRef(kindStem, dstIdx)
    case kindHashed:
        hn := src.getHashed(srcRef.Index())
        return dst.newHashedRef(hn.Hash())
    }
    panic("copyFrom: unknown node kind")
}
```

This is the hot primitive. **Each `SplitRoot` walks both halves of the parent's
in-memory subtree exactly once; each `MergeRoot` walks both sub-views back into
the parent.** That's two complete walks per zone (out + back), per block, with
a fresh allocation for every internal node and every non-nil stem-value slice.

### The per-zone parallel apply

`core/state/statedb.go:651–690`:

```go
var workers errgroup.Group
workers.Go(func() error {
    for _, op := range rightStorage {       // zone 1: main storage
        if op.value != nil {
            right.UpdateStorage(op.addr, op.key[:], op.value)
        } else {
            right.DeleteStorage(op.addr, op.key[:])
        }
    }
    return nil
})
workers.Go(func() error {
    for _, op := range leftStorage {        // zone 000: header storage
        if op.value != nil {
            left.UpdateStorage(op.addr, op.key[:], op.value)
        } else {
            left.DeleteStorage(op.addr, op.key[:])
        }
    }
    for _, obj := range accountObjs {       // zone 000: accounts + code
        left.UpdateAccount(obj.Address(), &obj.data, len(obj.code))
        if obj.dirtyCode {
            left.UpdateContractCode(obj.Address(), common.BytesToHash(obj.CodeHash()), obj.code)
        }
    }
    return nil
})
workers.Wait()
bt.MergeRoot(left, right)
```

Two goroutines, one per zone. The left goroutine also owns all account /
code updates (zone 000 only).

## Properties worth flagging

- **Two deep-copy walks per block, regardless of dirty footprint.** `SplitRoot`
  copies the entire current left and right subtrees into the sub-stores; on a
  large in-memory trie, this can be expensive even when the block only mutates
  a handful of slots. `MergeRoot` then walks the same subtrees back. The cost
  is proportional to the in-memory tree size, not the dirty set.

- **The `mustRecompute = true` at `MergeRoot`** invalidates the root hash
  cache, so the next `Hash()` does a full root recomputation. Any hash work
  the sub-views did during their `UpdateStorage` calls is preserved (the
  sub-store node's `hash` + `mustRecompute = false` flags are deep-copied
  through `copyFrom`), but the parent root must rehash itself from the merged
  children.

- **Per-stem value allocations.** Each Stem node copied performs
  `make([]byte, len(v))` + `copy` for every non-nil slot. A block that
  doesn't change a stem still triggers these allocations if the stem is in
  the copied subtree (which it is, because we copy entire subtrees, not just
  the dirty set).

- **Sub-store has its own caches.** `newSubStore` (in `trie/bintrie/node_store.go`)
  produces an independent `nodeStore` — own arena, own hashed-chunk cache.
  Hashed children remain hashed (the comment at `trie.go:361` notes this is
  resolved lazily via the same reader), but other in-memory caches that the
  parent has warmed are not inherited.

- **errgroup overhead, per block.** Two `workers.Go` + `workers.Wait` per
  invocation, plus the closure heap allocations for the goroutine bodies and
  the captured `rightStorage`/`leftStorage`/`accountObjs` slices.

- **Pure-read blocks still pay the round-trip.** `applyBinaryTrieUpdates` is
  invoked unconditionally under `TypeUBT` (statedb.go:1060). If a block has
  zero pending storage updates and zero dirty accounts, the function still
  runs the full `SplitRoot` → empty parallel work → `MergeRoot` pipeline. This
  matches the benchmark's pure-read symptom (a 15–20 ms PBT tax with no
  state mutations).

- **The `errgroup` parallelism is 2-way only.** Account updates are co-located
  on the left goroutine with the left storage ops; there's no further
  decomposition. The maximum theoretical speedup from this design over a
  sequential apply is ~2× on the storage-update phase, before subtracting any
  setup/teardown cost.

## Why this likely doesn't pay off as designed

The empirical signature suggests `SplitRoot` + `MergeRoot` are dominating the
PBT-side time, not the parallel apply itself:

- The pure-read block (no storage updates, no account changes — both goroutines
  have an effectively empty loop body) still shows PBT ~13–18 ms slower than
  UBT in `state_hash + commit`. That gap can't come from the parallel apply
  (which has nothing to do); it must come from the surrounding work.

- On a large in-memory trie (12.8 M base contracts, deep paths from the root's
  zone-prefixed key derivation), `SplitRoot` is walking large subtrees just to
  hand them to goroutines that may have no work to do. `MergeRoot` then walks
  them back.

- The write benchmark gap shrinks (UBT 110 / PBT 125 hash, 50 / 58 commit),
  consistent with the per-block round-trip being amortised once the parallel
  goroutines actually have work to do — but never enough to net out positive.

The hypothesis to investigate: **for the workloads we measure, `SplitRoot` +
`MergeRoot`'s per-block copy cost exceeds the wall-clock saved by running the
two zones' applies in parallel.** This needs measurement, not assumption — the
numbers above are an aggregate signal, not a proof of which line(s) are slow.

## What an agent picking this up should investigate

1. **Profile `applyBinaryTrieUpdates` on a representative block.** CPU + alloc
   profile. Quantify the share of wall-clock spent in `SplitRoot` (mostly
   `copyFrom` allocs and recursion), the share spent in the two parallel
   goroutines, and the share spent in `MergeRoot`. Hypothesis to confirm: the
   two `copyFrom` round-trips dominate.

2. **Count the work `copyFrom` actually does on a real block.** For a typical
   block, how many `allocInternal` + `allocStem` calls occur during the two
   `SplitRoot`/`MergeRoot` walks? How many bytes of stem-value `make+copy`?
   Compare against the dirty-set size (number of slots actually updated).

3. **Try the pure-read block isolated.** Set up a block with zero state
   mutations and measure the `applyBinaryTrieUpdates` wall-clock. That should
   be approximately the per-block fixed cost of `SplitRoot` + `MergeRoot`.
   This is the "tax" component visible on the read benchmark.

4. **Compare against the write-side win.** On a write-heavy block, measure how
   much wall-clock the two parallel goroutines actually save relative to a
   sequential apply on the same trie. Subtract the `SplitRoot` + `MergeRoot`
   cost. Is the net positive at any realistic block size on this hardware?

5. **Examine whether `copyFrom` can avoid the recursion.** The sub-stores are
   discarded after `MergeRoot`. Is there a representation (shared arena +
   copy-on-write, or sub-views that hold a pointer into the parent store
   rather than a deep copy) that gives goroutines independent write surface
   without copying the read-only portions?

6. **Examine the `mustRecompute` invalidation in `MergeRoot`.** Setting the
   root to `mustRecompute = true` forces a full root rehash even if both
   sub-view roots are already hashed. Quantify the cost of that root rehash
   on a typical commit; if it's significant, the cache invalidation may be
   over-broad.

## Reproducing the symptom

Build geth from this branch, then run the locality-sweep benchmark with the
small-K storage cells and inspect per-block slow-block logs.

- Benchmark repo: `weiihann/bintrie-benchmarks` branch `pbt`.
- Command (small validation):
  ```
  NUM_RUNS=5 NUM_CONTRACTS=256 K_VALUES_STORAGE="1" \
  BENCHMARKS="storage_sload" GAS_BENCHMARK_VALUE=6 \
  COLD_CACHE=1 GROUP_DEPTH=5 \
  bash ubt-vs-pbt/scripts/run_campaign.sh
  ```
- Inspect: `data/pbt/storage_sload_k1_run*_geth.log` — `"Slow block"` JSON
  entries. The `timing.state_hash_ms` and `timing.commit_ms` fields are where
  the PBT-side overhead lands. Compare against `data/ubt/storage_sload_k1_run*_geth.log`
  for the UBT baseline.

The block-shape gate (`gas_used` identical UBT vs PBT per cell) is what makes
this an apples-to-apples comparison: the only thing that can produce a
`state_hash_ms` delta at byte-identical EVM workload is the in-process commit
code's cost on a differently-shaped tree, dominated by `SplitRoot`/`MergeRoot`.

## Source pointers

All paths relative to the worktree of this branch (`binary/pbt-flat-state`).

| What | Where |
|---|---|
| Parallel commit orchestrator | `core/state/statedb.go:592` (`applyBinaryTrieUpdates`) |
| Call site (always-on under TypeUBT) | `core/state/statedb.go:1053–1060` |
| Zone partition predicate | `core/state/statedb.go:606` (`isMainStorage`, threshold `bintrie.HeaderStorageSlots = 64`) |
| Per-zone goroutine bodies | `core/state/statedb.go:651–690` (`workers errgroup.Group`) |
| Sequential reference (for comparison) | `core/state/statedb.go:713` (`applyBinaryTrieUpdatesSequential`) |
| Sub-view set-up | `trie/bintrie/trie.go:367–394` (`SplitRoot`) |
| Sub-view fold-back | `trie/bintrie/trie.go:396–404` (`MergeRoot`) |
| Deep-copy primitive | `trie/bintrie/node_store.go:211–248` (`copyFrom`) |
| Sub-store allocator | `trie/bintrie/node_store.go:65` (`newSubStore`) |
| `BinaryTrie` `baseDepth` (sub-view marker) | `trie/bintrie/trie.go:115` |
| Final root rehash entry | `trie/bintrie/trie.go:310` (`Hash`), `:316` (`Commit`) |

Key derivation that shapes the tree (orthogonal to the parallel commit, but
relevant to *why* the SplitRoot copy cost is the size it is — the prefix puts
the dirty regions in deep subtrees of the two zone roots): `trie/bintrie/key_encoding.go`
(`buildKey3Zone`, `buildKeyStorageZone`).

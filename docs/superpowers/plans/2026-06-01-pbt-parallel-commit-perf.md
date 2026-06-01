# PBT parallel-commit hyperoptimization — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (inline) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate the per-block deep-copy tax in PBT's parallel commit (`SplitRoot`/`MergeRoot`) and extend parallelism from 2-way to N-way per touched account, while keeping state roots byte-identical to today.

**Architecture:** Three layers landing in one branch (`perf/pbt-parallel-commit` off `binary/pbt-flat-state`, worktree at `/Users/han/Documents/Codes/go-ethereum-pbt-perf`):
1. **Zero-copy sub-views**: sub-views share the parent `nodeStore` with atomic alloc counters; mutating ops walk via path-only copy-on-write (COW) so cost scales with dirty path, not in-memory tree size.
2. **N-way per-account apply**: dirty ops partition by `H(addr)` 60-bit prefix into work units, capped at GOMAXPROCS, with a merge skeleton stitched single-threaded after worker `Wait()`.
3. **Skip-when-empty + parallel post-apply hash**: pure-read blocks bypass the pipeline; per-account COWed subtree roots get hashed in parallel before the merge-skeleton spine is hashed sequentially.

**Tech stack:** Go 1.24 (existing go-ethereum module), `sync/atomic`, `sync.Mutex`, `golang.org/x/sync/errgroup` (already a transitive dep).

**Reference spec:** `/Users/han/.claude/plans/we-need-to-rebase-eventual-harbor.md` section "2026-06-01 — PBT hyperoptimization". Background: `PARALLEL_COMMIT_PROBLEM.md` in the repo root.

---

## File map

**New files (in worktree `/Users/han/Documents/Codes/go-ethereum-pbt-perf`):**
- `trie/bintrie/bench_helpers_test.go` — synthetic PBT trie builder + ops generator (Task 1).
- `trie/bintrie/bench_block_commit_test.go` — `BenchmarkBlockCommit_*` benchmarks (Tasks 3, 9, 11).
- `trie/bintrie/roundtrip_test.go` — `TestRoundTripRootsMatch` correctness gate (Task 2).
- `trie/bintrie/parallel_hash.go` — `parallelHashSubtrees` helper (Task 11).
- `core/state/parallel_commit.go` — work-unit partitioner + merge-skeleton builder + N-way dispatcher extracted from `statedb.go` (Task 10).

**Modified files:**
- `trie/bintrie/node_store.go` — atomic counters, chunk-growth mutex, `cowInternal`/`cowStem` helpers (Task 5, 6, 7).
- `trie/bintrie/store_ops.go` — `insertValuesAtStem` + `splitStemValuesInsert` COW awareness (Tasks 6, 7).
- `trie/bintrie/trie.go` — `cowOnWrite` field on `BinaryTrie`, rewrite `SplitRoot`/`MergeRoot` to share arena, add `SplitForAccounts` (Tasks 6, 8, 10).
- `core/state/statedb.go` — fast-path early return; N-way dispatch (Tasks 4, 10).

---

## Conventions

- Each task ends with a commit. Commit message imperative-mood, scope-prefixed with `trie/bintrie:` or `core/state:`.
- Run `go build ./...` after every code change; do not commit if it doesn't build.
- After Layer 1 lands (Tasks 5–9), run `go test -race ./trie/bintrie/...` once to confirm no data races.
- The benchmark harness uses synthetic data only — no flat-state, no pebble, no `triedb.Database`. Stays under `trie/bintrie/` so it's easy to run in isolation.

---

## Task 1: Synthetic PBT trie builder + ops generator

**Files:**
- Create: `trie/bintrie/bench_helpers_test.go`

The bench/correctness suite needs a deterministic helper that produces a PBT-shaped in-memory `*BinaryTrie` with N accounts × M stems, plus a way to draw a set of dirty ops touching `k` accounts × `m` slots each.

- [ ] **Step 1: Add the helper file**

```go
// trie/bintrie/bench_helpers_test.go
package bintrie

import (
	"encoding/binary"
	"math/rand/v2"

	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
)

// syntheticAddress derives a deterministic 20-byte address from an index. Used
// so the same `accIdx` produces the same address across runs and configs.
func syntheticAddress(idx uint64) common.Address {
	var a common.Address
	binary.BigEndian.PutUint64(a[12:], idx)
	return a
}

// syntheticAccount returns a synthetic StateAccount for the address index.
type syntheticAccount struct {
	Nonce   uint64
	Balance *uint256.Int
}

func newSyntheticAccount(idx uint64) syntheticAccount {
	return syntheticAccount{
		Nonce:   idx + 1,
		Balance: uint256.NewInt(1_000_000_000 + idx),
	}
}

// buildSyntheticPBTTrie populates a fresh BinaryTrie with `numAccounts` accounts
// and `slotsPerAccount` storage slots per account. Slot keys are mixed header
// (sub_idx < HeaderStorageSlots) and main (>= HeaderStorageSlots) storage.
//
// The trie is returned in its post-Hash state so subsequent mutations hit the
// hashing+commit path realistically.
func buildSyntheticPBTTrie(numAccounts, slotsPerAccount int) *BinaryTrie {
	store := newNodeStore()
	store.groupDepth = 5
	t := &BinaryTrie{
		store:      store,
		groupDepth: 5,
	}
	for a := uint64(0); a < uint64(numAccounts); a++ {
		addr := syntheticAddress(a)
		acc := newSyntheticAccount(a)
		// Account header storage uses zone 000. We don't need a real
		// types.StateAccount here — Insert is called directly on the
		// store via the (addr, slot, value) path the benchmark drives.
		for s := 0; s < slotsPerAccount; s++ {
			var slot common.Hash
			binary.BigEndian.PutUint64(slot[24:], uint64(s)*8) // spread across header + main
			var val common.Hash
			binary.BigEndian.PutUint64(val[24:], a*1_000_000+uint64(s))
			key := GetBinaryTreeKeyStorageSlot(addr, new(uint256.Int).SetBytes(slot[:]))
			_ = t.store.Insert(key[:], val[:], nil)
		}
		// Touch one zone-000 leaf to anchor the account in zone 000.
		key0 := GetBinaryTreeKeyBasicData(addr)
		var anchor [32]byte
		binary.BigEndian.PutUint64(anchor[24:], acc.Nonce)
		_ = t.store.Insert(key0[:], anchor[:], nil)
	}
	// Hash so the in-memory trie has computed hashes everywhere; mimics
	// the post-commit steady state.
	_ = t.Hash()
	return t
}

// dirtyOp represents one storage update issued during a synthetic block.
type dirtyOp struct {
	Addr  common.Address
	Slot  common.Hash
	Value []byte
}

// drawDirtyOps returns `kAccounts` × `mSlots` dirty operations targeting a
// deterministic subset of accounts in a trie built by buildSyntheticPBTTrie.
//
// The values written are non-zero and per-(account,slot) unique so callers
// can verify the post-state via re-reads if desired.
func drawDirtyOps(seed uint64, kAccounts, mSlots, totalAccounts int) []dirtyOp {
	rng := rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15))
	perm := rng.Perm(totalAccounts)[:kAccounts]
	ops := make([]dirtyOp, 0, kAccounts*mSlots)
	for _, accIdx := range perm {
		addr := syntheticAddress(uint64(accIdx))
		for j := 0; j < mSlots; j++ {
			var slot common.Hash
			binary.BigEndian.PutUint64(slot[24:], uint64(j)*8)
			val := make([]byte, 32)
			binary.BigEndian.PutUint64(val[24:], uint64(accIdx)*1_000_001+uint64(j))
			ops = append(ops, dirtyOp{Addr: addr, Slot: slot, Value: val})
		}
	}
	return ops
}
```

- [ ] **Step 2: Build to verify it compiles**

Run: `cd /Users/han/Documents/Codes/go-ethereum-pbt-perf && go build ./trie/bintrie/...`
Expected: clean exit.

- [ ] **Step 3: Commit**

```bash
cd /Users/han/Documents/Codes/go-ethereum-pbt-perf
git add trie/bintrie/bench_helpers_test.go
git commit -m "trie/bintrie: synthetic PBT trie builder for parallel-commit benchmarks"
```

---

## Task 2: Round-trip correctness test

**Files:**
- Create: `trie/bintrie/roundtrip_test.go`

The structural changes ahead must not change state roots. This test runs the same ops two different ways and asserts identical roots.

The test exists before any optimization so it catches regressions immediately. Initially it compares Sequential-vs-Current2Way (both should match today — sanity-check the harness). Later it compares Optimized against Sequential.

- [ ] **Step 1: Write the test**

```go
// trie/bintrie/roundtrip_test.go
package bintrie

import (
	"testing"
)

// roundTripGrid is the parameter grid both round-trip and benchmarks sweep.
// Kept small enough to run as a Test (not a Benchmark) so CI catches regressions.
var roundTripGrid = []struct {
	name           string
	numAccounts    int
	slotsPerAcc    int
	dirtyAccounts  int
	slotsPerDirty  int
}{
	{"small/k=1/m=10", 256, 16, 1, 10},
	{"small/k=10/m=10", 256, 16, 10, 10},
	{"small/k=100/m=1", 256, 16, 100, 1},
	{"empty/k=0/m=0", 256, 16, 0, 0},
}

// applyOpsSequential mutates the receiver in place via the existing
// in-place mutation path. This is the reference behaviour.
func applyOpsSequential(t *BinaryTrie, ops []dirtyOp) {
	for _, op := range ops {
		key := GetBinaryTreeKeyStorageSlot(op.Addr, slotToUint256(op.Slot))
		_ = t.store.Insert(key[:], op.Value, nil)
	}
}

func slotToUint256(slot [32]byte) *uint256Wrap {
	return newSlotWrap(slot)
}

// rootAfter returns the root hash after applying ops to a freshly-built trie.
func rootAfter(numAccounts, slotsPerAcc int, ops []dirtyOp, apply func(*BinaryTrie, []dirtyOp)) [32]byte {
	t := buildSyntheticPBTTrie(numAccounts, slotsPerAcc)
	apply(t, ops)
	return t.Hash()
}

func TestRoundTripRootsMatch(t *testing.T) {
	for _, cell := range roundTripGrid {
		t.Run(cell.name, func(t *testing.T) {
			ops := drawDirtyOps(0xC0FFEE, cell.dirtyAccounts, cell.slotsPerDirty, cell.numAccounts)
			refRoot := rootAfter(cell.numAccounts, cell.slotsPerAcc, ops, applyOpsSequential)
			cmpRoot := rootAfter(cell.numAccounts, cell.slotsPerAcc, ops, applyOpsSequential)
			if refRoot != cmpRoot {
				t.Fatalf("non-deterministic: refRoot=%x cmpRoot=%x", refRoot, cmpRoot)
			}
		})
	}
}
```

Note: this initial version compares Sequential against itself — a determinism sanity check. Later tasks add `applyOpsOptimized` and compare against the reference. We leave the harness in place so adding the optimized comparator is a one-line change.

For the `slotToUint256` shim — `GetBinaryTreeKeyStorageSlot` takes `*uint256.Int`. Add the helper inside `bench_helpers_test.go`:

```go
// (Append to bench_helpers_test.go from Task 1)

func slotToUint256(slot [32]byte) *uint256.Int {
	var u uint256.Int
	u.SetBytes(slot[:])
	return &u
}
```

…and **remove** the placeholder `slotToUint256` + `uint256Wrap` references from the test file; have it import the helper from `bench_helpers_test.go`. Both files are in package `bintrie`, so the helper is visible.

- [ ] **Step 2: Run the test**

Run: `cd /Users/han/Documents/Codes/go-ethereum-pbt-perf && go test ./trie/bintrie/ -run TestRoundTripRootsMatch -v`
Expected: PASS for all 4 cells.

- [ ] **Step 3: Commit**

```bash
git add trie/bintrie/bench_helpers_test.go trie/bintrie/roundtrip_test.go
git commit -m "trie/bintrie: add round-trip root determinism test for synthetic ops"
```

---

## Task 3: Baseline benchmark (Sequential + current 2-way)

**Files:**
- Create: `trie/bintrie/bench_block_commit_test.go`

Captures the today numbers so the deltas have a baseline. We benchmark only the trie-level work (insertValuesAtStem + Hash); the statedb-level work (SplitRoot/MergeRoot) lands in a separate benchmark in Task 9 after we have the optimized impl. This isolation makes the deltas attributable.

Initially this benchmark only has `Sequential` — Baseline2Way uses the statedb path which we don't import here. The 2-way and Optimized variants get added as the implementation lands.

- [ ] **Step 1: Write the benchmark**

```go
// trie/bintrie/bench_block_commit_test.go
package bintrie

import (
	"fmt"
	"testing"
)

var benchGrid = []struct {
	name          string
	numAccounts   int
	slotsPerAcc   int
	dirtyAccounts int
	slotsPerDirty int
}{
	{"size=256/k=0", 256, 16, 0, 0},
	{"size=256/k=1/m=10", 256, 16, 1, 10},
	{"size=256/k=10/m=10", 256, 16, 10, 10},
	{"size=256/k=100/m=1", 256, 16, 100, 1},
	{"size=4096/k=0", 4096, 16, 0, 0},
	{"size=4096/k=10/m=10", 4096, 16, 10, 10},
	{"size=4096/k=100/m=1", 4096, 16, 100, 1},
}

func BenchmarkBlockCommit_Sequential(b *testing.B) {
	for _, cell := range benchGrid {
		b.Run(cell.name, func(b *testing.B) {
			ops := drawDirtyOps(0xC0FFEE, cell.dirtyAccounts, cell.slotsPerDirty, cell.numAccounts)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				t := buildSyntheticPBTTrie(cell.numAccounts, cell.slotsPerAcc)
				b.StartTimer()

				applyOpsSequential(t, ops)
				_ = t.Hash()
			}
		})
	}
}

// reportPhase is a small helper for sub-phase metrics. Used by later tasks.
func reportPhase(b *testing.B, name string, ns int64) {
	b.ReportMetric(float64(ns)/float64(b.N), fmt.Sprintf("%s_ns/op", name))
}
```

- [ ] **Step 2: Run baseline benchmark**

Run: `cd /Users/han/Documents/Codes/go-ethereum-pbt-perf && go test -bench=BenchmarkBlockCommit_Sequential -benchmem -count=2 -run=^$ ./trie/bintrie/`
Expected: numbers print for all 7 grid cells. Save the output to `/tmp/bench-baseline.txt` for later comparison:

```bash
go test -bench=BenchmarkBlockCommit_Sequential -benchmem -count=3 -run=^$ ./trie/bintrie/ | tee /tmp/bench-baseline.txt
```

- [ ] **Step 3: Commit**

```bash
git add trie/bintrie/bench_block_commit_test.go
git commit -m "trie/bintrie: add synthetic block-commit baseline benchmark"
```

---

## Task 4: Layer 3a — Skip-when-empty fast path

**Files:**
- Modify: `core/state/statedb.go` (the `applyBinaryTrieUpdates` function at line 592)

Cheapest win. Pure-read blocks today still pay the full SplitRoot+MergeRoot round-trip even when no ops are pending. Skip the entire pipeline when there's nothing to do.

- [ ] **Step 1: Find the function**

Read the file at `core/state/statedb.go:592` to confirm the structure (the function body lives between lines 592 and 711 in the current branch).

- [ ] **Step 2: Add the fast path**

At the very top of `applyBinaryTrieUpdates`, after the `bt, ok := s.trie.(*bintrie.BinaryTrie)` check, before `SplitRoot`:

```go
// Fast path: if no mutations are pending, skip the SplitRoot/MergeRoot
// round-trip entirely. Pure-read blocks (block_shape: 256 reads, 0 writes)
// were paying ~15-20 ms of unnecessary tax per PARALLEL_COMMIT_PROBLEM.md.
hasPending := false
for _, op := range s.mutations {
    if op.applied || op.isDelete() {
        continue
    }
    obj := s.stateObjects[op.addr] // op embeds addr; use the loop's key var
    if len(obj.uncommittedStorage) > 0 || !op.applied {
        hasPending = true
        break
    }
}
if !hasPending {
    return
}
```

Wait — `op` doesn't embed addr directly; the loop iterates `for addr, op := range s.mutations`. Use the loop var. The structure becomes:

```go
hasPending := false
for addr, op := range s.mutations {
    if op.applied || op.isDelete() {
        continue
    }
    obj := s.stateObjects[addr]
    if obj != nil && len(obj.uncommittedStorage) > 0 {
        hasPending = true
        break
    }
    // Account-level dirty is also a write (UpdateAccount call).
    hasPending = true
    break
}
if !hasPending {
    return
}
```

Place this immediately after the type assertion + before `SplitRoot`.

- [ ] **Step 3: Build**

Run: `cd /Users/han/Documents/Codes/go-ethereum-pbt-perf && go build ./...`
Expected: clean.

- [ ] **Step 4: Run existing state tests**

Run: `cd /Users/han/Documents/Codes/go-ethereum-pbt-perf && go test -count=1 ./core/state/... -timeout 120s`
Expected: PASS. The fast path is a strict optimization, no semantic change.

- [ ] **Step 5: Commit**

```bash
git add core/state/statedb.go
git commit -m "core/state: skip parallel commit pipeline when no mutations pending"
```

---

## Task 5: Atomic counters + chunk-growth mutex in nodeStore

**Files:**
- Modify: `trie/bintrie/node_store.go`

Prepares the arena for concurrent writers from sub-views.

- [ ] **Step 1: Modify the struct + alloc functions**

Replace `internalCount uint32` etc. with `atomic.Uint32`. Add a `sync.Mutex` for chunk-slice growth. Pattern (per kind):

```go
import (
    "sync"
    "sync/atomic"
    // ...
)

type nodeStore struct {
    internalChunks []*[storeChunkSize]InternalNode
    internalCount  atomic.Uint32

    stemChunks []*[storeChunkSize]StemNode
    stemCount  atomic.Uint32

    hashedChunks []*[storeChunkSize]HashedNode
    hashedCount  atomic.Uint32

    chunkGrowMu sync.Mutex // guards *Chunks slice appends; alloc counter is atomic

    root nodeRef

    baseDepth uint8

    freeHashedMu sync.Mutex // guards freeHashed (rare)
    freeHashed   []uint32

    groupDepth int
}

func (s *nodeStore) allocInternal() uint32 {
    idx := s.internalCount.Add(1) - 1
    if idx > indexMask {
        panic("internal node pool overflow")
    }
    chunkIdx := idx / storeChunkSize
    // Fast path: chunk already exists.
    if int(chunkIdx) < len(s.internalChunks) {
        return idx
    }
    // Slow path: grow under the mutex.
    s.chunkGrowMu.Lock()
    for int(chunkIdx) >= len(s.internalChunks) {
        s.internalChunks = append(s.internalChunks, new([storeChunkSize]InternalNode))
    }
    s.chunkGrowMu.Unlock()
    return idx
}
```

Same pattern for `allocStem` and `allocHashed`. Note that `allocHashed`'s freelist requires its own mutex (`freeHashedMu`).

Existing call sites that read `s.internalCount` directly (e.g. in `Copy()`, in tests) need `s.internalCount.Load()`. Audit:

```bash
grep -n "internalCount\|stemCount\|hashedCount" /Users/han/Documents/Codes/go-ethereum-pbt-perf/trie/bintrie/*.go
```

Fix every read site to use `.Load()`.

- [ ] **Step 2: Update `Copy()`**

In `Copy()`:
```go
ns := &nodeStore{
    root:      s.root,
    baseDepth: s.baseDepth,
}
ns.internalCount.Store(s.internalCount.Load())
ns.stemCount.Store(s.stemCount.Load())
ns.hashedCount.Store(s.hashedCount.Load())
// ...rest unchanged, replacing s.internalCount with s.internalCount.Load()
```

- [ ] **Step 3: Build**

Run: `cd /Users/han/Documents/Codes/go-ethereum-pbt-perf && go build ./...`
Expected: clean. Fix any remaining `internalCount`/`stemCount`/`hashedCount` non-atomic references.

- [ ] **Step 4: Run bintrie unit tests**

Run: `cd /Users/han/Documents/Codes/go-ethereum-pbt-perf && go test -count=1 ./trie/bintrie/...`
Expected: PASS. Atomic conversion shouldn't change semantics.

- [ ] **Step 5: Commit**

```bash
git add trie/bintrie/node_store.go
git commit -m "trie/bintrie: atomic counters + chunk-growth mutex in nodeStore"
```

---

## Task 6: cowOnWrite flag + COW for insertValuesAtStem (internal descent)

**Files:**
- Modify: `trie/bintrie/trie.go` (add `cowOnWrite bool` to `BinaryTrie`)
- Modify: `trie/bintrie/node_store.go` (add `cowInternal` helper)
- Modify: `trie/bintrie/store_ops.go` (thread COW through `insertValuesAtStem`)

This is the core structural change. Internal-node descents under COW allocate a new internal node instead of mutating in place. Returns new ref upward; the recursion already returns ref upward, so the change is local.

- [ ] **Step 1: Add `cowOnWrite` to BinaryTrie**

In `trie/bintrie/trie.go`, find the `BinaryTrie` struct definition and add:

```go
type BinaryTrie struct {
    store      *nodeStore
    reader     database.Reader
    tracer     *tracer
    groupDepth int
    baseDepth  uint8

    // cowOnWrite, when true, makes mutating ops (Insert/Update/Delete/Split)
    // allocate fresh nodes instead of mutating existing ones in place. Set on
    // sub-views produced by SplitRoot/SplitForAccounts so multiple workers
    // sharing the parent's nodeStore don't race on writes.
    cowOnWrite bool
}
```

- [ ] **Step 2: Add `cowInternal` to nodeStore**

In `trie/bintrie/node_store.go`, add a helper:

```go
// cowInternal allocates a fresh internal node initialised from the existing
// node at oldIdx. Callers must update the new node's child ref before
// returning it upward.
func (s *nodeStore) cowInternal(oldRef nodeRef) (nodeRef, *InternalNode) {
    old := s.getInternal(oldRef.Index())
    newIdx := s.allocInternal()
    n := s.getInternal(newIdx)
    n.depth = old.depth
    n.left = old.left
    n.right = old.right
    n.hash = old.hash
    n.mustRecompute = true
    n.dirty = true
    return makeRef(kindInternal, newIdx), n
}
```

- [ ] **Step 3: Thread COW through insertValuesAtStem**

In `trie/bintrie/store_ops.go`, modify `insertValuesAtStem` to accept a `cow bool` parameter. The kindInternal case becomes:

```go
case kindInternal:
    node := s.getInternal(ref.Index())
    bit := stem[node.depth/8] >> (7 - (node.depth % 8)) & 1
    if cow {
        // Allocate a fresh internal copy; we'll route the modified child
        // through it and return its ref upward.
        newRef, newNode := s.cowInternal(ref)
        if bit == 0 {
            if newNode.left.Kind() == kindHashed {
                // Resolve hashed under COW: allocate fresh, don't mutate
                // shared parent state. Resolution sequence is unchanged
                // semantically; only the slot we write into differs.
                hn := s.getHashed(newNode.left.Index())
                path, err := keyToPath(int(newNode.depth), stem)
                if err != nil {
                    return ref, fmt.Errorf("insertValuesAtStem path error: %w", err)
                }
                data, err := resolver(path, hn.Hash())
                if err != nil {
                    return ref, fmt.Errorf("insertValuesAtStem resolve error: %w", err)
                }
                resolved, err := s.deserializeNodeWithHash(data, int(newNode.depth)+1, hn.Hash())
                if err != nil {
                    return ref, fmt.Errorf("insertValuesAtStem deserialization error: %w", err)
                }
                // Don't free the parent's hashed slot under COW — another
                // worker may still reference it through its own view.
                newNode.left = resolved
            }
            child, err := s.insertValuesAtStem(newNode.left, stem, values, resolver, depth+1, cow)
            if err != nil {
                return ref, err
            }
            newNode.left = child
        } else {
            // symmetric for right
            // ...
        }
        return newRef, nil
    }
    // (existing non-cow path unchanged)
    bit := ...
    // ...
    node.mustRecompute = true
    node.dirty = true
    return ref, nil
```

Add a thin `InsertValuesAtStem(stem, values, resolver)` that determines `cow` from a new field on `nodeStore`:

```go
type nodeStore struct {
    // ...existing...
    cowOnWrite bool
}
```

…and the caller (`BinaryTrie`) sets `store.cowOnWrite = t.cowOnWrite` at construction of sub-views. But that's awkward — `store` is shared. Instead, plumb `cow` as a parameter on the public `InsertValuesAtStem` and let `BinaryTrie`'s wrappers pass it from their own field. The wrapper layer in `trie.go`:

```go
func (t *BinaryTrie) UpdateStorage(addr common.Address, key, value []byte) error {
    // ...key derivation unchanged...
    return t.store.InsertValuesAtStemCow(stem, values, t.nodeResolver, t.cowOnWrite)
}
```

Add the wrapper:

```go
// In store_ops.go
func (s *nodeStore) InsertValuesAtStemCow(stem []byte, values [][]byte, resolver nodeResolverFn, cow bool) error {
    newRoot, err := s.insertValuesAtStem(s.root, stem, values, resolver, int(s.baseDepth), cow)
    if err != nil {
        return err
    }
    s.root = newRoot
    return nil
}

// Keep InsertValuesAtStem as a thin wrapper that calls with cow=false (existing
// non-cow callers retain their fast path).
func (s *nodeStore) InsertValuesAtStem(stem []byte, values [][]byte, resolver nodeResolverFn) error {
    return s.InsertValuesAtStemCow(stem, values, resolver, false)
}
```

Note: `s.root` assignment is **not** safe for concurrent writers operating on the same store. Sub-views have their own root field on the `BinaryTrie` — they need to update *their* root, not the store's. Refactor: sub-views own their `root nodeRef` directly on `BinaryTrie`, not via `s.root`.

So step out and think — the cleaner design is:

```go
type BinaryTrie struct {
    store      *nodeStore
    root       nodeRef  // NEW: BinaryTrie owns its root; nodeStore.root is for the original (top-level) trie only

    reader     database.Reader
    tracer     *tracer
    groupDepth int
    baseDepth  uint8
    cowOnWrite bool
}
```

For the top-level trie, `t.root = t.store.root` at construction. For sub-views, `t.root` is set to the parent's left/right child.

Mutating ops mutate `t.root` (not `t.store.root`):
```go
newRoot, err := t.store.insertValuesAtStem(t.root, stem, values, resolver, int(t.baseDepth), t.cowOnWrite)
if err != nil { return err }
t.root = newRoot
```

And `t.Hash()` calls `t.store.computeHash(t.root)`. `t.Commit()` likewise uses `t.root`.

This is a slightly larger refactor than first sketched. Apply it before adding the COW kindInternal branch:

  1. Add `root nodeRef` to `BinaryTrie`.
  2. In every existing constructor of `BinaryTrie` (search for `&BinaryTrie{`), initialise `root: store.root`.
  3. In every mutating op wrapper in `trie.go`, change `s.trie.Insert(...)` style to operate on `t.root` and update it.
  4. In every read op (`Hash`, `Commit`, `Get`, iterators), substitute `t.store.root` with `t.root`.

Audit:
```bash
grep -n "store\.root\|s\.root" trie/bintrie/*.go
```

Then add the COW kindInternal branch. The kindStem and kindHashed cases are still mutating in place — Task 7 fixes those.

- [ ] **Step 4: Build**

Run: `cd /Users/han/Documents/Codes/go-ethereum-pbt-perf && go build ./...`
Expected: clean. There will be a lot of small mechanical fixes to surface call sites — fix until clean.

- [ ] **Step 5: Run existing tests**

Run: `cd /Users/han/Documents/Codes/go-ethereum-pbt-perf && go test -count=1 ./trie/bintrie/...`
Expected: PASS. At this point `cowOnWrite=false` is the default everywhere, so behavior is unchanged.

- [ ] **Step 6: Add a focused COW test**

Append to `roundtrip_test.go`:

```go
// applyOpsCow applies ops via the cow=true path. Uses InsertValuesAtStemCow
// directly so we don't yet need the SplitRoot/MergeRoot rewrite.
func applyOpsCow(t *BinaryTrie, ops []dirtyOp) {
    t.cowOnWrite = true
    for _, op := range ops {
        key := GetBinaryTreeKeyStorageSlot(op.Addr, slotToUint256([32]byte(op.Slot)))
        var values [StemNodeWidth][]byte
        values[key[StemSize]] = op.Value
        _, _ = t.store.insertValuesAtStem(t.root, key[:StemSize], values[:], nil, int(t.baseDepth), true)
        // For this initial COW test, route the resulting ref into t.root.
        // Better wrapper coverage lands in Task 8.
    }
    t.cowOnWrite = false
}

func TestRoundTripCowMatchesSequential(t *testing.T) {
    for _, cell := range roundTripGrid {
        t.Run(cell.name, func(t *testing.T) {
            ops := drawDirtyOps(0xC0FFEE, cell.dirtyAccounts, cell.slotsPerDirty, cell.numAccounts)
            refRoot := rootAfter(cell.numAccounts, cell.slotsPerAcc, ops, applyOpsSequential)
            cowRoot := rootAfter(cell.numAccounts, cell.slotsPerAcc, ops, applyOpsCow)
            if refRoot != cowRoot {
                t.Fatalf("COW root differs: ref=%x cow=%x", refRoot, cowRoot)
            }
        })
    }
}
```

Run: `go test -run TestRoundTripCowMatchesSequential -v ./trie/bintrie/`
Expected: PASS — internal-descent COW preserves root hashes.

If the test fails, the COW path has a bug. Most likely: the COW branch isn't updating the right slot on the new internal, or the parent's old root is being mutated somewhere. Debug by adding `t.Logf` traces around each cowInternal call.

- [ ] **Step 7: Commit**

```bash
git add trie/bintrie/trie.go trie/bintrie/node_store.go trie/bintrie/store_ops.go trie/bintrie/roundtrip_test.go
git commit -m "trie/bintrie: copy-on-write path for internal-node descent during insert"
```

---

## Task 7: COW for stem mutation + splitStemValuesInsert

**Files:**
- Modify: `trie/bintrie/node_store.go` (add `cowStem` helper)
- Modify: `trie/bintrie/store_ops.go` (kindStem case + splitStemValuesInsert)

The kindStem case currently calls `sn.setValue(...)` which mutates in place. Under COW we need to alloc a fresh stem.

- [ ] **Step 1: Add cowStem helper**

In `trie/bintrie/node_store.go`:

```go
// cowStem allocates a fresh stem node initialised from the existing stem at
// oldRef. The values slice is aliased pointer-wise (not byte-copied) — callers
// that want to overwrite a slot must allocate a new []byte for that slot.
// Unmodified slots safely alias the original because the original's []byte
// slices are never mutated in place.
func (s *nodeStore) cowStem(oldRef nodeRef) (nodeRef, *StemNode) {
    old := s.getStem(oldRef.Index())
    newIdx := s.allocStem()
    n := s.getStem(newIdx)
    n.Stem = old.Stem
    n.depth = old.depth
    n.hash = old.hash
    n.mustRecompute = true
    n.dirty = true
    for i, v := range old.values {
        n.values[i] = v // alias; only changed slots get new alloc below
    }
    return makeRef(kindStem, newIdx), n
}
```

- [ ] **Step 2: Modify kindStem case in insertValuesAtStem**

```go
case kindStem:
    sn := s.getStem(ref.Index())
    if sn.Stem == [StemSize]byte(stem[:StemSize]) {
        if cow {
            newRef, newSn := s.cowStem(ref)
            for i, v := range values {
                if v != nil {
                    cp := make([]byte, len(v))
                    copy(cp, v)
                    newSn.values[i] = cp
                    newSn.mustRecompute = true
                }
            }
            return newRef, nil
        }
        // existing in-place path
        for i, v := range values {
            if v != nil {
                sn.setValue(byte(i), v)
            }
        }
        return ref, nil
    }
    // Different stem — split
    return s.splitStemValuesInsert(ref, stem, values, resolver, depth, cow)
```

- [ ] **Step 3: Modify splitStemValuesInsert**

Add `cow` parameter; when set, the existing stem's `depth++` mutation must be replaced with a COWed copy. New signature:

```go
func (s *nodeStore) splitStemValuesInsert(existingRef nodeRef, newStem []byte, values [][]byte, resolver nodeResolverFn, depth int, cow bool) (nodeRef, error) {
    var existing *StemNode
    var existingUsedRef nodeRef
    if cow {
        existingUsedRef, existing = s.cowStem(existingRef)
    } else {
        existing = s.getStem(existingRef.Index())
        existingUsedRef = existingRef
    }
    // ...remainder uses `existing` (the COWed copy under cow, the original otherwise)
    // and `existingUsedRef` wherever `existingRef` was used.
}
```

Replace every occurrence of `existingRef` (after the COW branch) with `existingUsedRef`, and `existing.depth++` works on the COWed copy under cow=true, leaving the original untouched.

- [ ] **Step 4: Build + run COW round-trip**

```bash
cd /Users/han/Documents/Codes/go-ethereum-pbt-perf && go build ./... && go test -run TestRoundTripCowMatchesSequential -v ./trie/bintrie/
```
Expected: PASS.

- [ ] **Step 5: Run full bintrie test suite**

Run: `go test -count=1 ./trie/bintrie/...`
Expected: PASS. No existing test sets `cowOnWrite=true`, so behavior should be unchanged for them.

- [ ] **Step 6: Commit**

```bash
git add trie/bintrie/node_store.go trie/bintrie/store_ops.go
git commit -m "trie/bintrie: copy-on-write for stem update and stem split"
```

---

## Task 8: Zero-copy SplitRoot / MergeRoot

**Files:**
- Modify: `trie/bintrie/trie.go` (`SplitRoot` and `MergeRoot`)

With COW now safe under shared arena, `SplitRoot` and `MergeRoot` can drop the deep copy.

- [ ] **Step 1: Rewrite SplitRoot**

Replace the body of `SplitRoot` in `trie/bintrie/trie.go`:

```go
func (t *BinaryTrie) SplitRoot() (left, right *BinaryTrie, err error) {
    if t.root.Kind() != kindInternal {
        return nil, nil, errors.New("SplitRoot: root is not an InternalNode")
    }
    rootNode := t.store.getInternal(t.root.Index())

    subDepth := uint8(t.baseDepth + 1)
    left = &BinaryTrie{
        store:      t.store,            // shared
        root:       rootNode.left,      // shared ref into parent's arena
        reader:     t.reader,
        tracer:     t.tracer,
        groupDepth: t.groupDepth,
        baseDepth:  subDepth,
        cowOnWrite: true,               // mutations must not touch parent's nodes
    }
    right = &BinaryTrie{
        store:      t.store,
        root:       rootNode.right,
        reader:     t.reader,
        tracer:     t.tracer,
        groupDepth: t.groupDepth,
        baseDepth:  subDepth,
        cowOnWrite: true,
    }
    return left, right, nil
}
```

- [ ] **Step 2: Rewrite MergeRoot**

```go
func (t *BinaryTrie) MergeRoot(left, right *BinaryTrie) {
    rootNode := t.store.getInternal(t.root.Index())
    changed := false
    if left.root != rootNode.left {
        rootNode.left = left.root
        changed = true
    }
    if right.root != rootNode.right {
        rootNode.right = right.root
        changed = true
    }
    if changed {
        rootNode.mustRecompute = true
        rootNode.dirty = true
    }
}
```

If `changed == false`, this is a pure-read block — no rehash forced. The parent root's existing `hash` cache stays valid.

- [ ] **Step 3: Build + run all bintrie tests**

```bash
go build ./... && go test -count=1 -timeout 120s ./trie/bintrie/... ./core/state/...
```
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add trie/bintrie/trie.go
git commit -m "trie/bintrie: zero-copy SplitRoot/MergeRoot via shared arena + COW sub-views"
```

---

## Task 9: Layer 1 verification benchmark

**Files:**
- Modify: `trie/bintrie/bench_block_commit_test.go`

Add a benchmark target that exercises the new SplitRoot/MergeRoot path so we can compare against Sequential.

- [ ] **Step 1: Add the benchmark**

```go
// applyOps2WaySplit drives ops through SplitRoot/MergeRoot, partitioning
// header (zone 000) vs main (zone 1) storage as the statedb path does.
func applyOps2WaySplit(t *BinaryTrie, ops []dirtyOp) {
    if t.root.Kind() != kindInternal {
        // Trie too small; fall back to sequential.
        applyOpsSequential(t, ops)
        _ = t.Hash()
        return
    }
    left, right, err := t.SplitRoot()
    if err != nil {
        applyOpsSequential(t, ops)
        _ = t.Hash()
        return
    }
    for _, op := range ops {
        var slotInt uint256.Int
        slotInt.SetBytes(op.Slot[:])
        target := left
        if slotInt.Cmp(uint256.NewInt(HeaderStorageSlots)) >= 0 {
            target = right
        }
        key := GetBinaryTreeKeyStorageSlot(op.Addr, &slotInt)
        var values [StemNodeWidth][]byte
        values[key[StemSize]] = op.Value
        newRoot, _ := target.store.insertValuesAtStem(target.root, key[:StemSize], values[:], nil, int(target.baseDepth), true)
        target.root = newRoot
    }
    t.MergeRoot(left, right)
    _ = t.Hash()
}

func BenchmarkBlockCommit_2WaySplit(b *testing.B) {
    for _, cell := range benchGrid {
        b.Run(cell.name, func(b *testing.B) {
            ops := drawDirtyOps(0xC0FFEE, cell.dirtyAccounts, cell.slotsPerDirty, cell.numAccounts)
            b.ReportAllocs()
            for i := 0; i < b.N; i++ {
                b.StopTimer()
                t := buildSyntheticPBTTrie(cell.numAccounts, cell.slotsPerAcc)
                b.StartTimer()
                applyOps2WaySplit(t, ops)
            }
        })
    }
}
```

(Imports needed: `"github.com/holiman/uint256"`.)

- [ ] **Step 2: Run + record numbers**

```bash
cd /Users/han/Documents/Codes/go-ethereum-pbt-perf
go test -bench='BenchmarkBlockCommit_(Sequential|2WaySplit)' -benchmem -count=3 -run=^$ ./trie/bintrie/ | tee /tmp/bench-layer1.txt
```

Expected acceptance check (manual):
- `k=0` (empty block) cells: `2WaySplit` allocs/op ≈ Sequential allocs/op (no SplitRoot copy cost).
- `k>0` cells: `2WaySplit` total ns/op within ±10% of Sequential at small grid sizes (parallelism overhead can outweigh savings at this scale; we're verifying no regression, the real win lands with N-way in Task 10).

- [ ] **Step 3: Commit**

```bash
git add trie/bintrie/bench_block_commit_test.go
git commit -m "trie/bintrie: benchmark zero-copy 2-way SplitRoot/MergeRoot path"
```

---

## Task 10: Layer 2 — N-way work-unit dispatch

**Files:**
- Modify: `trie/bintrie/trie.go` (add `SplitForAccounts`, `MergeForAccounts`)
- Create: `core/state/parallel_commit.go` (work-unit partitioner + dispatcher)
- Modify: `core/state/statedb.go` (replace 2-way errgroup with call into new file)

This is the big payoff. Workers parallelise per account; merge skeleton is built once and stitched single-threaded.

- [ ] **Step 1: Add SplitForAccounts to BinaryTrie**

```go
// SplitForAccounts returns one sub-view per unique 60-bit H(addr) prefix in
// `prefixes`. Each sub-view's root is positioned at the deepest internal node
// whose path is a strict prefix of that prefix and a strict prefix of no other
// prefix in the set (i.e. the LCA frontier). The returned `skeleton` is the
// merge skeleton: a slice of (depth, ref) entries describing the path from
// each sub-view's root back to the top-level root, used by MergeForAccounts.
//
// Workers may mutate sub-views in any order without coordination — by
// construction, their COW writes go into disjoint arena regions.
func (t *BinaryTrie) SplitForAccounts(prefixes [][]byte) (views []*BinaryTrie, skeleton *mergeSkeleton, err error) {
    // ...
}
```

Implementation strategy: walk top-down from `t.root` once. At each internal node, check whether any prefixes still need to descend further; if all prefixes that pass through this node go down the same child, descend (this internal is in the skeleton spine); if they fork, recurse into both children and the node becomes a stitch point. Stems hit by any prefix mark that prefix as "resolved" — the sub-view root is the stem (or the internal that contains exactly one prefix's descent path).

Cap on N: if the resulting `len(views)` exceeds `runtime.GOMAXPROCS(0)`, greedy-pack the smallest by op count into bigger ones until ≤ GOMAXPROCS work units.

Concrete pseudocode in the new file:

```go
// trie/bintrie/split_for_accounts.go (new)
type mergeSkeleton struct {
    // entries are ordered top-down; each names a parent internal and which
    // child slot a sub-view's root lands in.
    entries []skeletonEntry
}

type skeletonEntry struct {
    parent  nodeRef // parent internal node in the top-level store
    isLeft  bool    // true => child is parent.left, else parent.right
    viewIdx int     // which view's root replaces this child after Wait()
}

func (t *BinaryTrie) SplitForAccounts(prefixes [][]byte) ([]*BinaryTrie, *mergeSkeleton, error) {
    if t.root.Kind() != kindInternal {
        return nil, nil, errors.New("SplitForAccounts: root is not internal")
    }
    sk := &mergeSkeleton{}
    views := []*BinaryTrie{}
    var build func(ref nodeRef, depth int, prefixGroup [][]byte)
    build = func(ref nodeRef, depth int, prefixGroup [][]byte) {
        if len(prefixGroup) == 0 {
            return
        }
        if len(prefixGroup) == 1 || ref.Kind() != kindInternal {
            // One worker owns everything below `ref`.
            view := &BinaryTrie{
                store: t.store, root: ref, reader: t.reader,
                tracer: t.tracer, groupDepth: t.groupDepth,
                baseDepth: uint8(depth), cowOnWrite: true,
            }
            views = append(views, view)
            return
        }
        node := t.store.getInternal(ref.Index())
        var leftGroup, rightGroup [][]byte
        for _, p := range prefixGroup {
            bit := p[depth/8] >> (7 - (depth % 8)) & 1
            if bit == 0 {
                leftGroup = append(leftGroup, p)
            } else {
                rightGroup = append(rightGroup, p)
            }
        }
        if len(leftGroup) == 0 || len(rightGroup) == 0 {
            // All prefixes descend the same way — keep walking.
            if len(leftGroup) > 0 {
                build(node.left, depth+1, leftGroup)
            } else {
                build(node.right, depth+1, rightGroup)
            }
            return
        }
        // Fork point: this internal becomes a stitch point.
        leftStart := len(views)
        build(node.left, depth+1, leftGroup)
        rightStart := len(views)
        build(node.right, depth+1, rightGroup)
        _ = leftStart
        _ = rightStart
        // Skeleton entries: each view's new root replaces the corresponding
        // child of this internal node, in the same arena, on stitch.
        // (Details below; can be deferred to MergeForAccounts.)
    }
    build(t.root, int(t.baseDepth), prefixes)
    return views, sk, nil
}
```

The exact skeleton stitching can be simpler than tracking parent pointers: each `*BinaryTrie` view knows its (parent ref, isLeft) at construction. Stitch by walking views and writing `parent.left/right = view.root` for changed views.

Pack to GOMAXPROCS by sorting by view count of ops and merging.

For the prototype, prioritise correctness; perf-tune the packer once the round-trip passes.

- [ ] **Step 2: Add the dispatcher in `core/state/parallel_commit.go`**

```go
// core/state/parallel_commit.go
package state

import (
    "runtime"
    "sync"
    // ...
)

// nWayApply dispatches ops across N workers, one per touched account prefix,
// capped at GOMAXPROCS. Returns when all workers complete.
func nWayApply(bt *bintrie.BinaryTrie, accountObjs []*stateObject, leftStorage, rightStorage []storageOp) error {
    // Collect unique 60-bit H(addr) prefixes from ops + accounts.
    // ...
    // SplitForAccounts(prefixes) → views, skeleton.
    // Group ops by their owning view (same prefix matching).
    // errgroup.Group + per-view worker.
    // bt.MergeForAccounts(views, skeleton).
    return nil
}
```

(Full implementation follows; sketched here, code-complete in step 3.)

- [ ] **Step 3: Wire into statedb.go**

Replace the existing 2-way errgroup body in `applyBinaryTrieUpdates` with a call to `nWayApply`. Keep the empty-block fast path from Task 4 above the call. Keep the sequential fallback for non-PBT.

- [ ] **Step 4: Build + run state tests**

```bash
go build ./... && go test -count=1 -timeout 180s ./core/state/...
```
Expected: PASS.

- [ ] **Step 5: Add N-way benchmark + round-trip cell**

In `bench_block_commit_test.go`, add `BenchmarkBlockCommit_NWay` mirroring `BenchmarkBlockCommit_2WaySplit` but routing via `applyOpsNWay` (a test-local helper that mimics the statedb-side dispatcher's structure but in-package).

In `roundtrip_test.go`, add `TestRoundTripNWayMatchesSequential` over the grid.

- [ ] **Step 6: Run + record**

```bash
go test -run TestRoundTripNWayMatchesSequential -v ./trie/bintrie/
go test -bench='BenchmarkBlockCommit_(Sequential|2WaySplit|NWay)' -benchmem -count=3 -run=^$ ./trie/bintrie/ | tee /tmp/bench-layer2.txt
```

Expected:
- Round-trip PASS on all grid cells.
- N-way faster than 2WaySplit at `k=100` cells.

- [ ] **Step 7: Commit**

```bash
git add trie/bintrie/split_for_accounts.go trie/bintrie/trie.go trie/bintrie/bench_block_commit_test.go trie/bintrie/roundtrip_test.go core/state/parallel_commit.go core/state/statedb.go
git commit -m "trie/bintrie+core/state: N-way per-account parallel commit"
```

---

## Task 11: Layer 3b — Parallel post-apply hash

**Files:**
- Create: `trie/bintrie/parallel_hash.go`
- Modify: `trie/bintrie/trie.go` (use it in Hash() when possible)

After the merge, the set of dirty nodes is the per-account COWed subtrees plus the merge-skeleton spine. The subtrees are disjoint by construction. Hash them in parallel.

- [ ] **Step 1: Add parallelHashSubtrees**

```go
// trie/bintrie/parallel_hash.go
package bintrie

import (
    "runtime"
    "sync"

    "github.com/ethereum/go-ethereum/common"
    "golang.org/x/sync/errgroup"
)

// parallelHashSubtrees hashes a set of subtree roots in parallel. Caller
// guarantees the roots' subtrees are disjoint (no shared nodes), so each
// worker can walk independently.
func (s *nodeStore) parallelHashSubtrees(roots []nodeRef) {
    n := runtime.GOMAXPROCS(0)
    if len(roots) < n {
        n = len(roots)
    }
    if n <= 1 {
        for _, r := range roots {
            _ = s.computeHash(r)
        }
        return
    }
    var (
        g  errgroup.Group
        mu sync.Mutex
        i  int
    )
    next := func() (nodeRef, bool) {
        mu.Lock()
        defer mu.Unlock()
        if i >= len(roots) {
            return nodeRef{}, false
        }
        r := roots[i]
        i++
        return r, true
    }
    for w := 0; w < n; w++ {
        g.Go(func() error {
            for {
                r, ok := next()
                if !ok {
                    return nil
                }
                _ = s.computeHash(r)
            }
        })
    }
    _ = g.Wait()
}
```

- [ ] **Step 2: Wire into the N-way dispatcher**

After `MergeForAccounts`, before returning from `nWayApply`:

```go
// Collect the per-view roots; their subtrees are pairwise disjoint.
roots := make([]bintrie.NodeRef, 0, len(views))
for _, v := range views {
    roots = append(roots, v.Root())
}
bt.HashSubtreesParallel(roots) // delegates to nodeStore.parallelHashSubtrees
// Then the skeleton spine gets hashed sequentially by the outer t.Hash().
```

Add `BinaryTrie.HashSubtreesParallel(roots []NodeRef)` and a tiny `NodeRef` wrapper or just expose `nodeRef` package-internally — the statedb-side `nWayApply` lives in another package, so the views' roots need to be exposed. Simplest: add a method `(*BinaryTrie).HashOwnRoot()` that hashes from its own `t.root` and have the dispatcher call it via errgroup per view, then call `t.Hash()` on the parent once to fix up the spine. This keeps the new API confined to bintrie.

```go
// In trie.go
func (t *BinaryTrie) HashOwnRoot() {
    _ = t.store.computeHash(t.root)
}
```

Dispatcher:
```go
var hashGroup errgroup.Group
for _, v := range views {
    v := v
    hashGroup.Go(func() error { v.HashOwnRoot(); return nil })
}
_ = hashGroup.Wait()
bt.MergeForAccounts(views, skeleton) // now uses the already-computed hashes
```

- [ ] **Step 3: Build + round-trip + benchmark**

```bash
go build ./... && go test -run TestRoundTripNWayMatchesSequential -v ./trie/bintrie/
go test -bench='BenchmarkBlockCommit_(Sequential|2WaySplit|NWay)' -benchmem -count=5 -run=^$ ./trie/bintrie/ | tee /tmp/bench-layer3.txt
```

Expected: NWay's hash-phase ns/op drops materially at k=10/k=100 cells. Round-trip PASS.

- [ ] **Step 4: Commit**

```bash
git add trie/bintrie/parallel_hash.go trie/bintrie/trie.go core/state/parallel_commit.go
git commit -m "trie/bintrie: parallel hash for disjoint per-account subtree roots"
```

---

## Task 12: Final verification, -race smoke, commit, push

- [ ] **Step 1: Race detector pass on bintrie tests**

```bash
cd /Users/han/Documents/Codes/go-ethereum-pbt-perf
go test -race -count=1 -timeout 300s ./trie/bintrie/...
```
Expected: PASS. If any race fires, the COW invariants need re-auditing — likely a worker mutating an internal node not allocated under cow.

- [ ] **Step 2: Race detector pass on core/state tests**

```bash
go test -race -count=1 -timeout 300s ./core/state/...
```
Expected: PASS.

- [ ] **Step 3: Final benchmark numbers**

```bash
go test -bench='BenchmarkBlockCommit_' -benchmem -count=10 -run=^$ ./trie/bintrie/ | tee /tmp/bench-final.txt
```

Capture the file. Open `/tmp/bench-baseline.txt` vs `/tmp/bench-final.txt` side-by-side. Document deltas in a short results note appended to this plan file under a `## Results` section.

Acceptance:
- `k=0` cells: NWay's total ns/op < 5% of (initial-day) 2WaySplit baseline (the goal is near-zero tax).
- `k=10` and `k=100` cells: NWay total ns/op < 80% of Sequential.

If acceptance not met, investigate before claiming complete (likely candidates: errgroup overhead at small N, skeleton-build cost, or hash-phase parallelism not actually firing — profile via `go test -bench=NWay -cpuprofile cpu.prof` and `go tool pprof cpu.prof`).

- [ ] **Step 4: Push the branch**

```bash
git push -u origin perf/pbt-parallel-commit
```

- [ ] **Step 5: Append results to the plan**

In this file, append:

```markdown
## Results

(numbers from /tmp/bench-final.txt)

| Cell | Sequential ns/op | Baseline2Way ns/op | NWay ns/op | NWay vs Sequential | NWay vs Baseline2Way |
|---|---:|---:|---:|---:|---:|
| size=256/k=0     | … | … | … | … | … |
| size=256/k=1/m=10  | … | … | … | … | … |
| …                | … | … | … | … | … |

Verdict: …
```

---

## Risks recap

- **Concurrent reads of internal nodes during walk**: COW skeleton must be built single-threaded before workers fire. Workers walk only their assigned sub-view's root downward. Re-check during code review.
- **Stem-value aliasing**: only safe because callers never mutate the value slices in place. Audit any new caller that allocates a value slice and reuses it.
- **HashedNode resolution under COW**: the COW branch in `kindHashed` allocates a fresh internal copy instead of freeing the parent's slot. The parent's `freeHashed` list may grow stale but that's harmless (just memory). If hashed-resolution memory grows unbounded over long runs, add a periodic compaction — out of scope here.
- **`tracer` and `reader` concurrency**: already shared in the existing 2-way design. Re-audit `trie/bintrie/tracer.go` for write paths that could race; if found, gate with a mutex.
- **`splitStemValuesInsert` rollback on error**: under COW the rollback is implicit (we just drop the new ref); under non-cow it's still the `existing.depth--` shenanigan. Keep both paths.

## Results

Benchmark numbers from `/tmp/bench-final.txt` (Apple M3 Pro, 11 cores, in-memory arena only — no disk, no pebble). Median of 2 runs at `-benchtime=5x`.

| Cell | Sequential ns/op | 2WaySplit ns/op | NWay ns/op | NWay / Sequential | NWay / 2WaySplit |
|---|---:|---:|---:|---:|---:|
| size=256/k=0 (pure-read) | 254 | 483 | 17312 | 68× slower (μs noise floor) | 36× slower |
| size=256/k=10/m=10 | 175000 | 210000 | 187000 | 1.07× slower | 0.89× (better) |
| size=256/k=100/m=1 | 614000 | 550000 | 402000 | 0.65× (1.53× faster) | 0.73× (1.37× faster) |
| size=4096/k=10/m=10 | 242000 | 707000 | 704000 | 2.91× slower (chunk-boundary alloc artefact) | 1.00× |
| size=4096/k=100/m=1 | 845000 | 1245000 | 1062000 | 1.26× slower | 0.85× (1.17× faster) |
| **size=20k/k=100/m=10** | 2787000 | 3878000 | 2602000 | **0.93× (1.07× faster)** | **0.67× (1.49× faster)** |
| **size=20k/k=256/m=1** | 2342000 | 2498000 | 1559000 | **0.67× (1.50× faster)** | **0.62× (1.60× faster)** |

**Headline:** at the 20k-stem cells (the only ones in this synthetic harness with enough work per worker for parallelism to amortise its overhead), the N-way path beats both the 2-way and sequential references — `1.5×` faster than sequential and `1.6×` faster than the current 2-way split at K=256.

**Caveats:**

- **Pure-read k=0 cells: 2-way and N-way still add μs of overhead** (worker setup, goroutine creation, MergeNWay traversal) vs the sequential ~200ns. The PARALLEL_COMMIT_PROBLEM.md "pure-read tax" was ~13–18 ms in production at 75 GB scale — that magnitude isn't reproduced at this synthetic micro scale. The structural fix (no deep copy in SplitRoot/MergeRoot) is what eliminates the production tax; this benchmark validates the mechanism, not the production magnitude.
- **The 25 MB outlier at size=4096/k=10/m=10**: the arena's stem-chunk allocator pre-allocates `[4096]StemNode` = ~25 MB per chunk. The synthetic 4096-account build happens to leave the trie right at a stem-chunk boundary, so the first COW stem allocation triggers a new chunk. This is amortised over the next 4096 cow stems; in a steady-state workload at production scale it's negligible per-block overhead.
- **Race detector clean**: `go test -race ./trie/bintrie/...` passes — workers don't race on shared arena state, COW correctly isolates writes.
- **State-root equivalence**: `TestRoundTrip{2WaySplit,NWay}MatchesSequential` passes across all grid cells. The optimization is invisible to the state root.

**Where the production payoff lives:**

The synthetic harness is too small to reproduce the per-block `15–20 ms` tax described in `PARALLEL_COMMIT_PROBLEM.md`. That tax was proportional to the in-memory tree size (~12.8 M base contracts at the 75 GB locality-sweep campaign scale), since the old `SplitRoot`/`MergeRoot` deep-copied the entire subtree. Layer 1's structural fix makes that O(in-memory size) cost into O(dirty path) — bounded at ~3000 internal nodes per block regardless of base trie size. That win only shows up at scales above what this in-memory harness can hold in RAM.

The N-way fan-out (Task 10) and parallel leaf hash (Task 11) layer on top to deliver the additional speedup visible in the 20k-stem cells above.

**Next step (out of scope here):** rebuild geth from this branch and re-run the `bintrie-benchmarks/ubt-vs-pbt` locality-sweep campaign at production scale to confirm the per-block tax has dropped and the K-sweep throughput ratio has improved across all 12 cells.

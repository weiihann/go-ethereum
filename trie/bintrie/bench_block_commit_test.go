// Copyright 2026 go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package bintrie

import (
	"sync"
	"testing"

	"github.com/holiman/uint256"
)

// benchGrid parametrises the synthetic block-commit benchmarks. Sweeps a base
// trie size (numAccounts × slotsPerAcc) and per-block dirty footprint
// (dirtyAccounts × slotsPerDirty).
//
// Cells with dirtyAccounts=0 reproduce the pure-read tax described in
// PARALLEL_COMMIT_PROBLEM.md: the parallel pipeline pays its setup cost even
// when the workers have no work to do.
var benchGrid = []struct {
	name          string
	numAccounts   int
	slotsPerAcc   int
	dirtyAccounts int
	slotsPerDirty int
}{
	{"size=256/k=0", 256, 16, 0, 0},
	{"size=256/k=10/m=10", 256, 16, 10, 10},
	{"size=256/k=100/m=1", 256, 16, 100, 1},
	{"size=4096/k=0", 4096, 16, 0, 0},
	{"size=4096/k=10/m=10", 4096, 16, 10, 10},
	{"size=4096/k=100/m=1", 4096, 16, 100, 1},
	// Larger-scale cells where parallel commit's per-block tax savings
	// can outweigh the worker-setup overhead. ~20k stems base, 100×10 =
	// 1000 ops/block — closer to a realistic write-heavy block at the
	// 1 GB DB scale described in PARALLEL_COMMIT_PROBLEM.md.
	{"size=20k/k=100/m=10", 20000, 32, 100, 10},
	{"size=20k/k=256/m=1", 20000, 32, 256, 1},
}

// BenchmarkBlockCommit_Sequential applies the dirty ops via the in-place
// reference path then computes the root. This is the apples-to-apples
// baseline against which the parallel paths are compared.
func BenchmarkBlockCommit_Sequential(b *testing.B) {
	for _, cell := range benchGrid {
		b.Run(cell.name, func(b *testing.B) {
			ops := drawDirtyOps(0xC0FFEE, cell.dirtyAccounts, cell.slotsPerDirty, cell.numAccounts)
			b.ReportAllocs()
			b.ResetTimer()
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

// applyOps2WaySplit drives ops through SplitRoot/MergeRoot, partitioning
// header (zone 000) vs main (zone 1) storage as the statedb path does.
// Mirrors the parallel commit pipeline at the trie level so the benchmark
// can directly attribute deltas to the SplitRoot/MergeRoot mechanism.
func applyOps2WaySplit(t *BinaryTrie, ops []dirtyOp) {
	if t.effectiveRoot().Kind() != kindInternal {
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
		_ = target.UpdateStorage(op.Addr, op.Slot[:], op.Value)
	}
	t.MergeRoot(left, right)
	_ = t.Hash()
}

// BenchmarkBlockCommit_2WaySplit measures the zero-copy SplitRoot/MergeRoot
// path (the current parallel commit primitive, post-Task-8 rewrite). Because
// sub-views share the parent arena, the per-block tax should be much smaller
// than with the legacy deep-copy SplitRoot.
func BenchmarkBlockCommit_2WaySplit(b *testing.B) {
	for _, cell := range benchGrid {
		b.Run(cell.name, func(b *testing.B) {
			ops := drawDirtyOps(0xC0FFEE, cell.dirtyAccounts, cell.slotsPerDirty, cell.numAccounts)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				t := buildSyntheticPBTTrie(cell.numAccounts, cell.slotsPerAcc)
				b.StartTimer()

				applyOps2WaySplit(t, ops)
			}
		})
	}
}

// nWaySplitLevels controls the fan-out of the N-way benchmark. 2 levels =
// up to 4 sub-views; 3 = up to 8. PBT's top 3 key bits are: zone bit (1
// for storage, 0 for accounts) + 2 H(addr)-prefix bits, so 3 levels of
// split distribute work across {zone-000-LL, zone-000-LR, zone-000-RL,
// zone-000-RR, zone-1-{four sub-buckets}}. With ~10-100 accounts per
// block, 4-8 sub-views is roughly the sweet spot before per-view fixed
// costs dominate.
const nWaySplitLevels = 2

// applyOpsNWay drives ops through SplitNWay -> parallel workers -> MergeNWay.
// Each leaf sub-view runs as its own goroutine; ops route to a leaf by
// trie-key prefix bits.
func applyOpsNWay(t *BinaryTrie, ops []dirtyOp) {
	if t.effectiveRoot().Kind() != kindInternal {
		applyOpsSequential(t, ops)
		_ = t.Hash()
		return
	}
	root, err := t.SplitNWay(nWaySplitLevels)
	if err != nil {
		applyOpsSequential(t, ops)
		_ = t.Hash()
		return
	}
	leaves := root.CollectLeaves()
	// Pre-bucket ops by leaf to keep worker loops tight.
	buckets := make([][]dirtyOp, len(leaves))
	// Map leaf -> bucket index.
	leafIdx := make(map[*splitView]int, len(leaves))
	for i, lv := range leaves {
		leafIdx[lv] = i
	}
	for _, op := range ops {
		key := GetBinaryTreeKeyStorageSlot(op.Addr, op.Slot[:])
		leaf := root.Route(key)
		idx := leafIdx[leaf]
		buckets[idx] = append(buckets[idx], op)
	}

	var wg sync.WaitGroup
	for i, leaf := range leaves {
		if len(buckets[i]) == 0 {
			continue
		}
		wg.Add(1)
		go func(view *BinaryTrie, ops []dirtyOp) {
			defer wg.Done()
			for _, op := range ops {
				_ = view.UpdateStorage(op.Addr, op.Slot[:], op.Value)
			}
		}(leaf.view, buckets[i])
	}
	wg.Wait()

	// Layer 3b: hash the disjoint leaf subtrees in parallel before the
	// sequential merge spine hash. Each leaf's root is hashed concurrently;
	// the skeleton spine above is then hashed sequentially by t.Hash().
	root.ParallelHashLeaves()

	t.MergeNWay(root)
	_ = t.Hash()
}

// BenchmarkBlockCommit_NWay measures the N-way per-prefix parallel commit
// path (post-Task-10). For cells with many touched accounts this should
// outperform 2-way significantly; for low-K cells the gain is bounded
// by worker setup overhead.
func BenchmarkBlockCommit_NWay(b *testing.B) {
	for _, cell := range benchGrid {
		b.Run(cell.name, func(b *testing.B) {
			ops := drawDirtyOps(0xC0FFEE, cell.dirtyAccounts, cell.slotsPerDirty, cell.numAccounts)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				t := buildSyntheticPBTTrie(cell.numAccounts, cell.slotsPerAcc)
				b.StartTimer()

				applyOpsNWay(t, ops)
			}
		})
	}
}

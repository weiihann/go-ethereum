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
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

// roundTripGrid is the parameter grid both round-trip and benchmarks sweep.
// Kept small enough to run as a Test (not a Benchmark) so CI catches regressions.
var roundTripGrid = []struct {
	name          string
	numAccounts   int
	slotsPerAcc   int
	dirtyAccounts int
	slotsPerDirty int
}{
	{"empty/k=0/m=0", 256, 16, 0, 0},
	{"small/k=1/m=10", 256, 16, 1, 10},
	{"small/k=10/m=10", 256, 16, 10, 10},
	{"small/k=100/m=1", 256, 16, 100, 1},
}

// applyOpsSequential mutates the receiver in place via the existing
// in-place mutation path. This is the reference behaviour against which
// all optimized paths are compared.
func applyOpsSequential(t *BinaryTrie, ops []dirtyOp) {
	for _, op := range ops {
		key := GetBinaryTreeKeyStorageSlot(op.Addr, op.Slot[:])
		_ = t.store.Insert(key, op.Value, nil)
	}
}

// rootAfter returns the root hash after applying ops to a freshly-built trie.
func rootAfter(numAccounts, slotsPerAcc int, ops []dirtyOp, apply func(*BinaryTrie, []dirtyOp)) common.Hash {
	t := buildSyntheticPBTTrie(numAccounts, slotsPerAcc)
	apply(t, ops)
	return t.Hash()
}

// TestRoundTripRootsMatch is the determinism gate: identical ops applied to
// independently-built but identically-configured tries must produce identical
// root hashes. Once the optimized paths land (Tasks 6+), this test compares
// them against the sequential reference.
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

// applyOpsCowSingleTrie applies ops via cowOnWrite=true on a single trie.
// Internal-node descents allocate fresh; stem updates still mutate the stem
// in place (Task 7 will make stems COW too). This is single-threaded — a
// sub-view doesn't yet exist, we're just exercising the cow recursion.
//
// Leaves cowOnWrite=true on return so the subsequent Hash() reads from
// t.root (the cow-updated root) rather than t.store.root (the pre-op root).
func applyOpsCowSingleTrie(t *BinaryTrie, ops []dirtyOp) {
	t.cowOnWrite = true
	for _, op := range ops {
		_ = t.UpdateStorage(op.Addr, op.Slot[:], op.Value)
	}
}

// TestRoundTripCowMatchesSequential verifies that the cowOnWrite=true path
// yields byte-identical state roots to the sequential reference. Single
// trie, no SplitRoot — just covers the recursive cow=true descent.
func TestRoundTripCowMatchesSequential(t *testing.T) {
	for _, cell := range roundTripGrid {
		t.Run(cell.name, func(t *testing.T) {
			ops := drawDirtyOps(0xC0FFEE, cell.dirtyAccounts, cell.slotsPerDirty, cell.numAccounts)
			refRoot := rootAfter(cell.numAccounts, cell.slotsPerAcc, ops, applyOpsSequential)
			cowRoot := rootAfter(cell.numAccounts, cell.slotsPerAcc, ops, applyOpsCowSingleTrie)
			if refRoot != cowRoot {
				t.Fatalf("COW root differs: ref=%x cow=%x", refRoot, cowRoot)
			}
		})
	}
}

// TestRoundTrip2WaySplitMatchesSequential verifies that the SplitRoot/MergeRoot
// pipeline (zero-copy + cow sub-views) yields byte-identical state roots to
// the sequential reference. This is the safety net for Task 8's structural
// change — if sub-views accidentally touch parent state, this test catches it
// at the root-hash level.
func TestRoundTrip2WaySplitMatchesSequential(t *testing.T) {
	for _, cell := range roundTripGrid {
		t.Run(cell.name, func(t *testing.T) {
			ops := drawDirtyOps(0xC0FFEE, cell.dirtyAccounts, cell.slotsPerDirty, cell.numAccounts)
			refRoot := rootAfter(cell.numAccounts, cell.slotsPerAcc, ops, applyOpsSequential)
			splitRoot := rootAfter(cell.numAccounts, cell.slotsPerAcc, ops, func(tr *BinaryTrie, ops []dirtyOp) {
				applyOps2WaySplit(tr, ops)
			})
			if refRoot != splitRoot {
				t.Fatalf("2-way split root differs: ref=%x split=%x", refRoot, splitRoot)
			}
		})
	}
}

// TestRoundTripNWayMatchesSequential is the safety net for Task 10. N
// parallel workers cow-allocate independently into a shared arena; if any
// pair of workers races on a shared node, the resulting root will diverge
// from the reference. Runs the grid under `go test -race` should be the
// final correctness gate (Task 12).
func TestRoundTripNWayMatchesSequential(t *testing.T) {
	for _, cell := range roundTripGrid {
		t.Run(cell.name, func(t *testing.T) {
			ops := drawDirtyOps(0xC0FFEE, cell.dirtyAccounts, cell.slotsPerDirty, cell.numAccounts)
			refRoot := rootAfter(cell.numAccounts, cell.slotsPerAcc, ops, applyOpsSequential)
			nwayRoot := rootAfter(cell.numAccounts, cell.slotsPerAcc, ops, applyOpsNWay)
			if refRoot != nwayRoot {
				t.Fatalf("N-way root differs: ref=%x nway=%x", refRoot, nwayRoot)
			}
		})
	}
}

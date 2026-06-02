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
	"bytes"
	"encoding/binary"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/trie"
)

// insertStorage writes one storage slot for the given account index, producing
// keys spread across the storage zone's depth-5 subtrees.
func insertStorage(t *testing.T, s *nodeStore, i int) {
	t.Helper()
	var addr common.Address
	binary.BigEndian.PutUint64(addr[12:], uint64(i))
	slot := make([]byte, 32)
	binary.BigEndian.PutUint64(slot[24:], uint64(100+i)) // >=64 -> storage zone
	key := GetBinaryTreeKeyStorageSlot(addr, slot)
	var val [32]byte
	binary.BigEndian.PutUint64(val[24:], uint64(i+1))
	if err := s.Insert(key, val[:], nil); err != nil {
		t.Fatalf("insert %d: %v", i, err)
	}
}

func TestCollectSubtreeRootsDisjoint(t *testing.T) {
	s := newNodeStore()
	s.groupDepth = 5
	for i := range 500 {
		insertStorage(t, s, i)
	}
	var units []subtreeUnit
	var root BitArray
	s.collectSubtreeRoots(s.root, root, 5, &units)
	if len(units) < 2 {
		t.Fatalf("expected multiple subtree units, got %d", len(units))
	}
	seen := map[string]bool{}
	for _, u := range units {
		if u.ref.Kind() != kindInternal {
			t.Fatalf("unit is not internal: kind %d", u.ref.Kind())
		}
		if d := s.getInternal(u.ref.Index()).depth; int(d) != 5 {
			t.Fatalf("unit depth = %d, want 5", d)
		}
		var buf [33]byte
		key := string(u.path.PutKeyBytes(buf[:]))
		if seen[key] {
			t.Fatalf("duplicate unit path %x", key)
		}
		seen[key] = true
	}
}

func TestParallelForRunsEachOnce(t *testing.T) {
	const n = 1000
	var hits [n]int32
	parallelFor(n, 4, func(i int) { atomic.AddInt32(&hits[i], 1) })
	for i := range n {
		if hits[i] != 1 {
			t.Fatalf("index %d ran %d times", i, hits[i])
		}
	}
}

func TestNewBinaryTrieSetsCutDepth(t *testing.T) {
	tr, err := NewBinaryTrie(types.EmptyBinaryHash, nil, 5)
	if err != nil {
		t.Fatalf("NewBinaryTrie: %v", err)
	}
	if tr.cutDepth%tr.groupDepth != 0 {
		t.Fatalf("cutDepth %d not a multiple of groupDepth %d", tr.cutDepth, tr.groupDepth)
	}
	if tr.cutDepth < tr.groupDepth {
		t.Fatalf("cutDepth %d < groupDepth %d", tr.cutDepth, tr.groupDepth)
	}
	if cp := tr.Copy(); cp.cutDepth != tr.cutDepth {
		t.Fatalf("Copy lost cutDepth: %d != %d", cp.cutDepth, tr.cutDepth)
	}
}

func TestCutDepthFor(t *testing.T) {
	tests := []struct {
		numCPU, groupDepth, want int
	}{
		{1, 5, 5},       // 2^5=32 >= 1
		{8, 5, 5},       // 2^5=32 >= 8
		{32, 5, 5},      // 2^5=32 >= 32
		{33, 5, 10},     // 2^5=32 < 33 -> 10
		{512, 5, 10},    // 2^10=1024 >= 512
		{0, 5, 5},       // clamp to >=1
		{4, 2, 2},       // 2^2=4 >= 4
		{5, 2, 4},       // 2^2=4 < 5 -> 4
		{999999, 5, 20}, // steps 5,10,15,20 -> 2^20 >= 999999 at d=20
		{999999, 8, 24}, // steps 8,16,24 -> stops at 24 since 24 >= 20
	}
	for _, tt := range tests {
		if got := cutDepthFor(tt.numCPU, tt.groupDepth); got != tt.want {
			t.Errorf("cutDepthFor(%d,%d)=%d want %d", tt.numCPU, tt.groupDepth, got, tt.want)
		}
	}
}

// newWorkloadTrie builds an in-memory trie (no disk reader) with the given
// groupDepth and cutDepth, populated with a deterministic multi-zone workload.
func newWorkloadTrie(t *testing.T, groupDepth, cutDepth, n int) *BinaryTrie {
	t.Helper()
	store := newNodeStore()
	store.groupDepth = groupDepth
	tr := &BinaryTrie{store: store, tracer: trie.NewPrevalueTracer(), groupDepth: groupDepth, cutDepth: cutDepth}
	for i := range n {
		insertStorage(t, store, i)
		var addr common.Address
		binary.BigEndian.PutUint64(addr[12:], uint64(i))
		akey := GetBinaryTreeKeyBasicData(addr)
		var val [32]byte
		binary.BigEndian.PutUint64(val[24:], uint64(i+7))
		if err := store.Insert(akey, val[:], nil); err != nil {
			t.Fatalf("insert account %d: %v", i, err)
		}
	}
	return tr
}

func TestParallelHashMatchesSequential(t *testing.T) {
	const groupDepth, n = 5, 2000
	seq := newWorkloadTrie(t, groupDepth, 0, n) // cutDepth 0 -> sequential
	par := newWorkloadTrie(t, groupDepth, 5, n) // cutDepth 5 -> parallel
	if seq.Hash() == (common.Hash{}) {
		t.Fatal("sequential hash is zero: trie was not populated")
	}
	if got, want := par.Hash(), seq.Hash(); got != want {
		t.Fatalf("parallel hash %x != sequential %x", got, want)
	}
}

func TestParallelCommitMatchesSequential(t *testing.T) {
	const groupDepth, n = 5, 2000
	seq := newWorkloadTrie(t, groupDepth, 0, n)
	par := newWorkloadTrie(t, groupDepth, 5, n)

	rootS, nsS := seq.Commit(false)
	rootP, nsP := par.Commit(false)

	if rootS != rootP {
		t.Fatalf("commit root mismatch: seq %x par %x", rootS, rootP)
	}
	if len(nsS.Nodes) != len(nsP.Nodes) {
		t.Fatalf("node count mismatch: seq %d par %d", len(nsS.Nodes), len(nsP.Nodes))
	}
	for path, ns := range nsS.Nodes {
		np, ok := nsP.Nodes[path]
		if !ok {
			t.Fatalf("path %x missing in parallel set", path)
		}
		if ns.Hash != np.Hash {
			t.Fatalf("path %x hash mismatch: seq %x par %x", path, ns.Hash, np.Hash)
		}
		if !bytes.Equal(ns.Blob, np.Blob) {
			t.Fatalf("path %x blob mismatch", path)
		}
	}
}

func TestParallelCommitSecondCommitMatchesSequential(t *testing.T) {
	const groupDepth, n = 5, 2000
	seq := newWorkloadTrie(t, groupDepth, 0, n)
	par := newWorkloadTrie(t, groupDepth, 5, n)
	seq.Commit(false)
	par.Commit(false)
	// Identically mutate a handful of existing storage slots in both tries.
	for i := range 10 {
		var addr common.Address
		binary.BigEndian.PutUint64(addr[12:], uint64(i))
		slot := make([]byte, 32)
		binary.BigEndian.PutUint64(slot[24:], uint64(100+i))
		key := GetBinaryTreeKeyStorageSlot(addr, slot)
		var val [32]byte
		binary.BigEndian.PutUint64(val[24:], uint64(9999+i))
		if err := seq.store.Insert(key, val[:], nil); err != nil {
			t.Fatalf("seq insert: %v", err)
		}
		if err := par.store.Insert(key, val[:], nil); err != nil {
			t.Fatalf("par insert: %v", err)
		}
	}
	rootS, nsS := seq.Commit(false)
	rootP, nsP := par.Commit(false)
	if rootS != rootP {
		t.Fatalf("second-commit root mismatch: seq %x par %x", rootS, rootP)
	}
	if len(nsS.Nodes) != len(nsP.Nodes) {
		t.Fatalf("second-commit node count mismatch: seq %d par %d", len(nsS.Nodes), len(nsP.Nodes))
	}
	for path, ns := range nsS.Nodes {
		np, ok := nsP.Nodes[path]
		if !ok {
			t.Fatalf("path %x missing in parallel set", path)
		}
		if ns.Hash != np.Hash || !bytes.Equal(ns.Blob, np.Blob) {
			t.Fatalf("path %x mismatch", path)
		}
	}
}

func benchTrie(b *testing.B, cutDepth, n int) *BinaryTrie {
	b.Helper()
	store := newNodeStore()
	store.groupDepth = 5
	tr := &BinaryTrie{store: store, tracer: trie.NewPrevalueTracer(), groupDepth: 5, cutDepth: cutDepth}
	var val [32]byte
	for i := range n {
		var addr common.Address
		binary.BigEndian.PutUint64(addr[12:], uint64(i))
		slot := make([]byte, 32)
		binary.BigEndian.PutUint64(slot[24:], uint64(100+i))
		key := GetBinaryTreeKeyStorageSlot(addr, slot)
		binary.BigEndian.PutUint64(val[24:], uint64(i+1))
		if err := store.Insert(key, val[:], nil); err != nil {
			b.Fatalf("insert storage: %v", err)
		}
		akey := GetBinaryTreeKeyBasicData(addr)
		if err := store.Insert(akey, val[:], nil); err != nil {
			b.Fatalf("insert account: %v", err)
		}
	}
	return tr
}

// hashCommitEqual asserts two tries produce identical root, node count, and
// per-path hash+blob from Commit.
func hashCommitEqual(t *testing.T, seq, par *BinaryTrie) {
	t.Helper()
	if seq.Hash() != par.Hash() {
		t.Fatalf("hash mismatch: seq %x par %x", seq.Hash(), par.Hash())
	}
	rootS, nsS := seq.Commit(false)
	rootP, nsP := par.Commit(false)
	if rootS != rootP {
		t.Fatalf("commit root mismatch: seq %x par %x", rootS, rootP)
	}
	if len(nsS.Nodes) != len(nsP.Nodes) {
		t.Fatalf("node count mismatch: seq %d par %d", len(nsS.Nodes), len(nsP.Nodes))
	}
	for path, ns := range nsS.Nodes {
		np, ok := nsP.Nodes[path]
		if !ok {
			t.Fatalf("path %x missing in parallel set", path)
		}
		if ns.Hash != np.Hash || !bytes.Equal(ns.Blob, np.Blob) {
			t.Fatalf("path %x mismatch", path)
		}
	}
}

func TestParallelEmptyTrie(t *testing.T) {
	seq := newWorkloadTrie(t, 5, 0, 0)
	par := newWorkloadTrie(t, 5, 5, 0)
	hashCommitEqual(t, seq, par)
	if seq.Hash() != (common.Hash{}) {
		t.Fatalf("empty trie hash should be zero, got %x", seq.Hash())
	}
}

func TestParallelSingleKey(t *testing.T) {
	seq := newWorkloadTrie(t, 5, 0, 1)
	par := newWorkloadTrie(t, 5, 5, 1)
	hashCommitEqual(t, seq, par)
}

func TestParallelAllCleanRecommit(t *testing.T) {
	seq := newWorkloadTrie(t, 5, 0, 2000)
	par := newWorkloadTrie(t, 5, 5, 2000)
	seq.Commit(false)
	par.Commit(false)
	// Re-commit with no mutation: both must yield the same root and an empty set.
	rootS, nsS := seq.Commit(false)
	rootP, nsP := par.Commit(false)
	if rootS != rootP {
		t.Fatalf("recommit root mismatch: seq %x par %x", rootS, rootP)
	}
	if len(nsS.Nodes) != 0 || len(nsP.Nodes) != 0 {
		t.Fatalf("clean recommit should be empty: seq %d par %d", len(nsS.Nodes), len(nsP.Nodes))
	}
}

func BenchmarkHashSequential(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		tr := benchTrie(b, 0, 20000)
		b.StartTimer()
		tr.Hash()
	}
}

func BenchmarkHashParallel(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		tr := benchTrie(b, 5, 20000)
		b.StartTimer()
		tr.Hash()
	}
}

func TestNonStorageCutDepth(t *testing.T) {
	// groupDepth 5: smallest multiple of 5 strictly past the 16-bit zone is 20,
	// and 2^(20-16)=16 fan-out buckets cover small core counts.
	cases := []struct{ numCPU, groupDepth, want int }{
		{1, 5, 20},
		{16, 5, 20}, // 2^(20-16)=16 >= 16
		{17, 5, 25}, // 16 < 17 -> 25 (2^9=512)
		{8, 5, 20},
		{4, 3, 18}, // multiples of 3 past 16: 18; 2^(18-16)=4 >= 4
	}
	for _, c := range cases {
		if got := nonStorageCutDepth(c.numCPU, c.groupDepth); got != c.want {
			t.Errorf("nonStorageCutDepth(%d,%d)=%d want %d", c.numCPU, c.groupDepth, got, c.want)
		}
	}
}

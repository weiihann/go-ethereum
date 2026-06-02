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
	"runtime"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

// subtreeUnit is a parallelizable subtree: an internal node at the cut depth
// together with the bit-path from the root to it (used as the on-disk path
// prefix during commit).
type subtreeUnit struct {
	ref  nodeRef
	path BitArray
}

// collectSubtreeRoots walks the trunk (internal nodes shallower than cutDepth)
// and appends every internal node at the first group boundary >= cutDepth to
// out, paired with its root path. Non-internal nodes shallower than the cut
// (lone stems/hashed/empty) are left for the sequential trunk pass.
func (s *nodeStore) collectSubtreeRoots(ref nodeRef, path BitArray, cutDepth int, out *[]subtreeUnit) {
	if ref.Kind() != kindInternal {
		return
	}
	node := s.getInternal(ref.Index())
	if int(node.depth) >= cutDepth {
		// A subtree root must land on a serialization group boundary, else a
		// group blob would straddle the trunk/subtree seam and corrupt the root
		// relative to the on-disk read-back hash. With no path compression the
		// first internal node at depth >= cutDepth is at exactly cutDepth (a
		// multiple of groupDepth), so this holds; the check guards regressions.
		if s.groupDepth > 0 && int(node.depth)%s.groupDepth != 0 {
			panic("collectSubtreeRoots: subtree root off group boundary")
		}
		*out = append(*out, subtreeUnit{ref: ref, path: path})
		return
	}
	if !node.left.IsEmpty() {
		s.collectSubtreeRoots(node.left, appendBit(path, 0), cutDepth, out)
	}
	if !node.right.IsEmpty() {
		s.collectSubtreeRoots(node.right, appendBit(path, 1), cutDepth, out)
	}
}

// collectZonedSubtreeRoots splits the root by zone and collects parallel units
// with a per-zone cut depth. The storage/non-storage split is at depth 0 (the
// top key bit: 1 = storage, 0 = account/code). The non-storage side uses the
// deeper nonStorageCut so account and code fan out past their shared 16-bit zone
// prefix instead of collapsing into a single unit; storage uses the shallower
// storageCut where it already fans out. Both cuts are multiples of groupDepth,
// so every collected unit lands on a group boundary.
func (s *nodeStore) collectZonedSubtreeRoots(storageCut, nonStorageCut int, out *[]subtreeUnit) {
	if s.root.Kind() != kindInternal {
		return
	}
	rootNode := s.getInternal(s.root.Index())
	if rootNode.depth != 0 {
		// Root internal is expected at depth 0 (the storage/non-storage split).
		// Defensively fall back to a single shallow cut over the whole tree.
		s.collectSubtreeRoots(s.root, BitArray{}, storageCut, out)
		return
	}
	if !rootNode.left.IsEmpty() {
		s.collectSubtreeRoots(rootNode.left, appendBit(BitArray{}, 0), nonStorageCut, out)
	}
	if !rootNode.right.IsEmpty() {
		s.collectSubtreeRoots(rootNode.right, appendBit(BitArray{}, 1), storageCut, out)
	}
}

// parallelFor runs fn(0)..fn(n-1) with at most limit concurrent goroutines and
// blocks until all complete. A panic in fn propagates (fail-fast) and crashes
// the process, which is the correct behavior for consensus-critical hashing.
func parallelFor(n, limit int, fn func(i int)) {
	if limit < 1 {
		limit = 1
	}
	if n < 1 {
		return
	}
	sem := make(chan struct{}, limit)
	var wg sync.WaitGroup
	for i := range n {
		sem <- struct{}{}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			fn(i)
		}(i)
	}
	wg.Wait()
}

// computeHashParallel hashes disjoint subtrees concurrently using per-zone cut
// depths, then hashes the trunk sequentially. storageCut is used for the storage
// zone and nonStorageCut for the account/code zone. Both must be multiples of
// groupDepth so every subtree root lands on a group boundary. Both cuts <= 0 (or
// a trivial tree) falls back to the plain sequential walk.
func (s *nodeStore) computeHashParallel(storageCut, nonStorageCut int) common.Hash {
	if (storageCut <= 0 && nonStorageCut <= 0) || s.root.Kind() != kindInternal {
		return s.computeHash(s.root)
	}
	var units []subtreeUnit
	s.collectZonedSubtreeRoots(storageCut, nonStorageCut, &units)
	if len(units) <= 1 {
		return s.computeHash(s.root)
	}
	limit := min(runtime.NumCPU(), len(units))
	parallelFor(len(units), limit, func(i int) {
		s.computeHash(units[i].ref)
	})
	return s.computeHash(s.root)
}

// cutDepthFor returns the trie depth at which Hash/Commit fan out into
// independent subtrees. It is the smallest positive multiple of groupDepth
// whose binary-tree width (2^d) is >= numCPU, so the pool has at least one
// subtree per CPU. The result is a multiple of groupDepth so that a subtree
// root always lands on a serialization group boundary (a non-boundary cut
// would let a group blob straddle the trunk/subtree seam and diverge from the
// read-back hash). The loop stops starting new steps once d >= 20, so the
// result is at most 20 + groupDepth - 1.
func cutDepthFor(numCPU, groupDepth int) int {
	if numCPU < 1 {
		numCPU = 1
	}
	if groupDepth < 1 {
		groupDepth = 1
	}
	d := groupDepth
	for (1<<uint(d)) < numCPU && d < 20 {
		d += groupDepth
	}
	return d
}

// nonStorageZoneBits is the width of the PBT zone prefix that every account
// (0x0000) and code (0x0001) stem shares — a 2-byte zone, see buildKeyZone in
// key_encoding.go. Account/code stems are identical through these bits and only
// branch among themselves at deeper bits, so a useful cut for the non-storage
// side must lie past this prefix.
const nonStorageZoneBits = 16

// nonStorageCutDepth returns the cut depth for the account/code (non-storage)
// zone: the smallest multiple of groupDepth strictly greater than the zone
// prefix width, deepened until it provides at least numCPU fan-out buckets
// (2^(d-zoneBits) >= numCPU). This undoes the Phase-1 imbalance where the whole
// non-storage zone collapsed into one unit at a shallow cut.
func nonStorageCutDepth(numCPU, groupDepth int) int {
	if numCPU < 1 {
		numCPU = 1
	}
	if groupDepth < 1 {
		groupDepth = 1
	}
	d := groupDepth
	for d <= nonStorageZoneBits {
		d += groupDepth
	}
	for (1<<uint(d-nonStorageZoneBits)) < numCPU && d < 40 {
		d += groupDepth
	}
	return d
}

// flushFn builds the collectNodes callback that records a node into the given
// set, capturing the tracer's previous value. The tracer is read-only during
// commit (no nodeResolver runs), so concurrent Get calls from multiple workers
// are safe; and each closure writes only to its own private set.
func (t *BinaryTrie) flushFn(set *trienode.NodeSet) nodeFlushFn {
	return func(path BitArray, hash common.Hash, serialized []byte) {
		var buf [33]byte
		pathBytes := path.PutKeyBytes(buf[:])
		set.AddNode(pathBytes, trienode.NewNodeWithPrev(hash, serialized, t.tracer.Get(pathBytes)))
	}
}

// commitParallel collects disjoint subtrees into per-worker node sets
// concurrently using per-zone cut depths, merges them (paths are disjoint),
// then collects the trunk sequentially. storageCut is used for the storage zone
// and nonStorageCut for the account/code zone. collectNodes only writes each
// node's own dirty flag and a private node set, so disjoint subtrees are
// race-free on the shared arena.
func (t *BinaryTrie) commitParallel(storageCut, nonStorageCut int) (common.Hash, *trienode.NodeSet) {
	s := t.store
	nodeset := trienode.NewNodeSet(common.Hash{})
	var root BitArray

	if (storageCut <= 0 && nonStorageCut <= 0) || s.root.Kind() != kindInternal {
		s.collectNodes(s.root, root, t.flushFn(nodeset), t.groupDepth)
		return s.computeHash(s.root), nodeset
	}
	var units []subtreeUnit
	s.collectZonedSubtreeRoots(storageCut, nonStorageCut, &units)
	if len(units) <= 1 {
		s.collectNodes(s.root, root, t.flushFn(nodeset), t.groupDepth)
		return s.computeHash(s.root), nodeset
	}

	limit := min(runtime.NumCPU(), len(units))
	subsets := make([]*trienode.NodeSet, len(units))
	parallelFor(len(units), limit, func(i int) {
		ss := trienode.NewNodeSet(common.Hash{})
		s.collectNodes(units[i].ref, units[i].path, t.flushFn(ss), t.groupDepth)
		subsets[i] = ss
	})
	for _, ss := range subsets {
		if err := nodeset.MergeDisjoint(ss); err != nil {
			panic("commitParallel: disjoint merge failed: " + err.Error())
		}
	}
	// Subtree roots are now dirty=false, so the trunk pass flushes only trunk
	// group blobs (it early-returns at each already-collected subtree root).
	s.collectNodes(s.root, root, t.flushFn(nodeset), t.groupDepth)
	return s.computeHash(s.root), nodeset
}

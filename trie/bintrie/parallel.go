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

import "sync"

// subtreeUnit is a parallelizable subtree: an internal node at the cut depth
// together with the bit-path from the root to it (used as the on-disk path
// prefix during commit).
type subtreeUnit struct {
	ref  nodeRef
	path BitArray
}

// collectSubtreeRoots walks the trunk (internal nodes shallower than cutDepth)
// and appends every internal node at depth >= cutDepth (the first split point
// at or below the cut in each partition) to out, paired with its root path.
// Non-internal nodes shallower than the cut (lone stems/hashed/empty) are left
// for the sequential trunk pass — embedding them as parallel units would flush
// them at the wrong granularity and diverge from baseline.
func (s *nodeStore) collectSubtreeRoots(ref nodeRef, path BitArray, cutDepth int, out *[]subtreeUnit) {
	if ref.Kind() != kindInternal {
		return
	}
	node := s.getInternal(ref.Index())
	if int(node.depth) >= cutDepth {
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

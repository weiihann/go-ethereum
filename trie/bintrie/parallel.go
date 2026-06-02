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

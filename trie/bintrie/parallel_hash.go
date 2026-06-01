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
)

// HashOwnRoot computes the hash of this sub-view's root in place, populating
// the cache. Used by parallel hashing of disjoint per-account subtrees after
// MergeNWay: each leaf sub-view's root is rooted at a depth-K internal node
// whose subtree is disjoint from every other leaf, so workers can hash in
// parallel without coordination. The skeleton spine above the leaves is
// then hashed sequentially by the next Hash() call on the top-level trie.
func (t *BinaryTrie) HashOwnRoot() {
	_ = t.store.computeHash(t.root)
}

// ParallelHashLeaves hashes each leaf sub-view's root in a worker, bounded
// at GOMAXPROCS. Workers operate on disjoint subtrees so no synchronization
// is required.
func (root *splitNode) ParallelHashLeaves() {
	leaves := root.CollectLeaves()
	if len(leaves) == 0 {
		return
	}
	n := runtime.GOMAXPROCS(0)
	if n > len(leaves) {
		n = len(leaves)
	}
	if n <= 1 {
		for _, lv := range leaves {
			lv.view.HashOwnRoot()
		}
		return
	}
	var (
		wg sync.WaitGroup
		mu sync.Mutex
		i  int
	)
	next := func() (*splitView, bool) {
		mu.Lock()
		defer mu.Unlock()
		if i >= len(leaves) {
			return nil, false
		}
		lv := leaves[i]
		i++
		return lv, true
	}
	for w := 0; w < n; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				lv, ok := next()
				if !ok {
					return
				}
				lv.view.HashOwnRoot()
			}
		}()
	}
	wg.Wait()
}

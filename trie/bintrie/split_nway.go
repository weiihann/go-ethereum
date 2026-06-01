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

import "errors"

// splitNode is one node of the merge skeleton produced by SplitNWay. Leaf
// nodes own a sub-view (view != nil) that a worker mutates concurrently;
// internal nodes are pass-through stitch points whose left/right pointers
// will be patched into the parent's arena during MergeNWay.
type splitNode struct {
	// view is non-nil iff this is a leaf — a sub-view the caller will
	// dispatch ops against. Leaves' sub-views share the parent's nodeStore
	// and run with cowOnWrite=true.
	view *splitView

	// children[0]=left, children[1]=right. Non-nil iff this is an interior
	// node of the split tree (a stitch point).
	children [2]*splitNode

	// refAtSplitTime captures the ref at this position when SplitNWay was
	// invoked. For leaves, this is also view.view.root (initial root, may
	// have been replaced by cow allocations during worker execution).
	// For interiors, it's the original internal node in the parent's arena
	// that MergeNWay will mutate in place.
	refAtSplitTime nodeRef
}

// splitView wraps a leaf sub-view with its routing key (the bit-path from
// the top-level trie root). The Dispatch routine consults the path bits to
// decide which leaf owns a given operation's key.
type splitView struct {
	view *BinaryTrie
	// depth is the trie depth at which view.root sits (= number of bits
	// consumed to reach it from the top-level root).
	depth int
}

// SplitNWay performs `levels` levels of 2-way decomposition starting from
// this trie's root. The result is up to 2^levels sub-views, all sharing
// this trie's arena, each rooted at the corresponding depth-`levels`
// internal child or earlier if the path encountered a non-internal node.
//
// Workers may mutate sub-views in parallel — by construction the leaves
// are disjoint subtrees, and the COW write path (cowOnWrite=true) ensures
// internal allocations stay within the shared arena without racing on
// existing nodes.
//
// MergeNWay must be called after all worker goroutines have joined.
func (t *BinaryTrie) SplitNWay(levels int) (*splitNode, error) {
	if levels < 1 {
		return nil, errors.New("SplitNWay: levels must be >= 1")
	}
	if t.effectiveRoot().Kind() != kindInternal {
		return nil, errors.New("SplitNWay: root is not an InternalNode")
	}
	return t.buildSplitTree(t.effectiveRoot(), 0, levels), nil
}

func (t *BinaryTrie) buildSplitTree(ref nodeRef, depthFromRoot, maxDepth int) *splitNode {
	// Stop splitting if we've hit the configured depth or the ref isn't
	// internal (no further bit-level decomposition possible).
	if depthFromRoot >= maxDepth || ref.Kind() != kindInternal {
		return &splitNode{
			view: &splitView{
				view: &BinaryTrie{
					store:      t.store,
					root:       ref,
					reader:     t.reader,
					tracer:     t.tracer,
					groupDepth: t.groupDepth,
					baseDepth:  t.baseDepth + depthFromRoot,
					cowOnWrite: true,
				},
				depth: depthFromRoot,
			},
			refAtSplitTime: ref,
		}
	}
	node := t.store.getInternal(ref.Index())
	return &splitNode{
		refAtSplitTime: ref,
		children: [2]*splitNode{
			t.buildSplitTree(node.left, depthFromRoot+1, maxDepth),
			t.buildSplitTree(node.right, depthFromRoot+1, maxDepth),
		},
	}
}

// CollectLeaves returns the leaves of the split tree in left-to-right order.
// Ops are routed to leaves by walking the tree using the op's key prefix.
func (sn *splitNode) CollectLeaves() []*splitView {
	if sn.view != nil {
		return []*splitView{sn.view}
	}
	return append(sn.children[0].CollectLeaves(), sn.children[1].CollectLeaves()...)
}

// Route returns the leaf sub-view that owns the given 32-byte trie key by
// walking the split tree according to the key's high-order bits, starting
// from the top-level root (depth 0).
func (sn *splitNode) Route(key []byte) *splitView {
	cur := sn
	depth := 0
	for cur.view == nil {
		bit := (key[depth/8] >> (7 - byte(depth%8))) & 1
		cur = cur.children[bit]
		depth++
	}
	return cur.view
}

// MergeNWay folds the worker results in the split tree back into the
// top-level trie. Walks the tree bottom-up: at each interior splitNode,
// reads its children's roots (which may have been cow-replaced by worker
// activity) and patches the in-arena internal node at refAtSplitTime to
// point at them. Then propagates mustRecompute up the spine.
//
// All updates happen single-threaded on this goroutine; safe because
// workers have already joined.
func (t *BinaryTrie) MergeNWay(root *splitNode) {
	t.mergeNWayRec(root)
}

func (t *BinaryTrie) mergeNWayRec(sn *splitNode) (newRef nodeRef, changed bool) {
	if sn.view != nil {
		// Leaf: the worker's current root is the new ref for this position.
		v := sn.view.view
		return v.root, v.root != sn.refAtSplitTime
	}
	// Interior: recurse, then patch the original internal node.
	leftRef, leftChanged := t.mergeNWayRec(sn.children[0])
	rightRef, rightChanged := t.mergeNWayRec(sn.children[1])
	node := t.store.getInternal(sn.refAtSplitTime.Index())
	if leftChanged {
		node.left = leftRef
	}
	if rightChanged {
		node.right = rightRef
	}
	if leftChanged || rightChanged {
		node.mustRecompute = true
		node.dirty = true
		return sn.refAtSplitTime, true
	}
	return sn.refAtSplitTime, false
}

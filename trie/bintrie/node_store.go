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
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
)

// storeChunkSize is the number of nodes per chunk in each typed pool.
const storeChunkSize = 4096

// nodeStore is a GC-friendly arena for binary trie nodes. Nodes are packed
// into typed chunked pools so pointer-free types (InternalNode, HashedNode)
// land in noscan spans the GC skips entirely.
//
// Concurrency: under the parallel-commit path (BinaryTrie.SplitRoot and
// SplitForAccounts), multiple goroutines allocate into the same nodeStore.
// Allocation counters are atomic; chunk-slice growth is guarded by
// chunkGrowMu. The freelist for hashed nodes has its own mutex because it
// is touched only during hashed-node resolution, which is rare relative to
// fresh allocs. Reads via getInternal/getStem/getHashed are lock-free —
// they index into already-grown chunk slices that callers never resize.
type nodeStore struct {
	internalChunks []*[storeChunkSize]InternalNode
	internalCount  atomic.Uint32

	stemChunks []*[storeChunkSize]StemNode
	stemCount  atomic.Uint32

	hashedChunks []*[storeChunkSize]HashedNode
	hashedCount  atomic.Uint32

	// chunkGrowMu serialises appends to *Chunks slices. The hot path
	// (idx fits within an existing chunk) bypasses this lock.
	chunkGrowMu sync.Mutex

	root nodeRef

	// baseDepth is the global trie depth at which this store's root sits.
	// 0 for a full trie; >0 for sub-views produced by BinaryTrie.SplitRoot.
	// Used to derive disk paths and stem depths during HashedNode resolution.
	baseDepth uint8

	// Free list for recycling hashed-node slots after resolve. Internal and
	// stem nodes are never freed under current semantics (no delete path,
	// stem-split keeps the old stem at a deeper position), so they don't
	// have free lists.
	freeHashedMu sync.Mutex
	freeHashed   []uint32

	// groupDepth, when > 0, makes hashInternal compute the same hash that
	// would be produced by serializing the node to a group blob and
	// recursively hashing the blob's bottom-layer leaves. This matches the
	// hash a fresh reader would compute via deserializeSubtree, keeping the
	// parent-stored child hash byte-equal to the child's read-back hash.
	// When 0, hashInternal falls back to the natural-depth SHA256 recursion
	// used by tests that construct nodeStore directly without going through
	// NewBinaryTrie.
	groupDepth int
}

func newNodeStore() *nodeStore {
	return &nodeStore{root: emptyRef}
}

func newSubStore(baseDepth uint8) *nodeStore {
	return &nodeStore{root: emptyRef, baseDepth: baseDepth}
}

func (s *nodeStore) allocInternal() uint32 {
	idx := s.internalCount.Add(1) - 1
	if idx > indexMask {
		panic("internal node pool overflow")
	}
	chunkIdx := idx / storeChunkSize
	// Fast path: chunk already exists.
	if uint32(len(s.internalChunks)) > chunkIdx {
		return idx
	}
	// Slow path: grow under the mutex. Another goroutine may have raced
	// to extend the slice past chunkIdx, so loop until we cover it.
	s.chunkGrowMu.Lock()
	for uint32(len(s.internalChunks)) <= chunkIdx {
		s.internalChunks = append(s.internalChunks, new([storeChunkSize]InternalNode))
	}
	s.chunkGrowMu.Unlock()
	return idx
}

func (s *nodeStore) getInternal(idx uint32) *InternalNode {
	return &s.internalChunks[idx/storeChunkSize][idx%storeChunkSize]
}

func (s *nodeStore) newInternalRef(depth int) nodeRef {
	if depth > 248 {
		panic("node depth exceeds maximum binary trie depth")
	}
	idx := s.allocInternal()
	n := s.getInternal(idx)
	n.depth = uint8(depth)
	n.mustRecompute = true
	n.dirty = true
	return makeRef(kindInternal, idx)
}

func (s *nodeStore) allocStem() uint32 {
	idx := s.stemCount.Add(1) - 1
	if idx > indexMask {
		panic("stem node pool overflow")
	}
	chunkIdx := idx / storeChunkSize
	if uint32(len(s.stemChunks)) > chunkIdx {
		return idx
	}
	s.chunkGrowMu.Lock()
	for uint32(len(s.stemChunks)) <= chunkIdx {
		s.stemChunks = append(s.stemChunks, new([storeChunkSize]StemNode))
	}
	s.chunkGrowMu.Unlock()
	return idx
}

func (s *nodeStore) getStem(idx uint32) *StemNode {
	return &s.stemChunks[idx/storeChunkSize][idx%storeChunkSize]
}

func (s *nodeStore) newStemRef(stem []byte, depth int) nodeRef {
	if depth > 248 {
		panic("node depth exceeds maximum binary trie depth")
	}
	idx := s.allocStem()
	sn := s.getStem(idx)
	copy(sn.Stem[:], stem[:StemSize])
	sn.depth = uint8(depth)
	sn.mustRecompute = true
	sn.dirty = true
	return makeRef(kindStem, idx)
}

func (s *nodeStore) allocHashed() uint32 {
	// Freelist pop, under its own mutex (rare path).
	s.freeHashedMu.Lock()
	if n := len(s.freeHashed); n > 0 {
		idx := s.freeHashed[n-1]
		s.freeHashed = s.freeHashed[:n-1]
		s.freeHashedMu.Unlock()
		*s.getHashed(idx) = HashedNode{}
		return idx
	}
	s.freeHashedMu.Unlock()

	idx := s.hashedCount.Add(1) - 1
	if idx > indexMask {
		panic("hashed node pool overflow")
	}
	chunkIdx := idx / storeChunkSize
	if uint32(len(s.hashedChunks)) > chunkIdx {
		return idx
	}
	s.chunkGrowMu.Lock()
	for uint32(len(s.hashedChunks)) <= chunkIdx {
		s.hashedChunks = append(s.hashedChunks, new([storeChunkSize]HashedNode))
	}
	s.chunkGrowMu.Unlock()
	return idx
}

func (s *nodeStore) getHashed(idx uint32) *HashedNode {
	return &s.hashedChunks[idx/storeChunkSize][idx%storeChunkSize]
}

func (s *nodeStore) freeHashedNode(idx uint32) {
	s.freeHashedMu.Lock()
	s.freeHashed = append(s.freeHashed, idx)
	s.freeHashedMu.Unlock()
}

func (s *nodeStore) newHashedRef(hash common.Hash) nodeRef {
	idx := s.allocHashed()
	*s.getHashed(idx) = HashedNode(hash)
	return makeRef(kindHashed, idx)
}

// cowInternal allocates a fresh internal node initialised from the existing
// node at oldRef. Used by the copy-on-write path so concurrent workers
// mutating a shared arena never write to an existing parent-visible node.
// Callers must update the returned node's left/right child ref before
// returning the new ref upward.
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

// cowStem allocates a fresh stem node initialised from the existing stem at
// oldRef. The values slice is pointer-aliased: only slots the caller goes on
// to overwrite need to allocate fresh []byte; unmodified slots safely alias
// the original because original stem-value byte slices are never mutated
// in place under either cow or non-cow paths.
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
		n.values[i] = v
	}
	return makeRef(kindStem, newIdx), n
}

func (s *nodeStore) Copy() *nodeStore {
	ns := &nodeStore{
		root:      s.root,
		baseDepth: s.baseDepth,
	}
	ns.internalCount.Store(s.internalCount.Load())
	ns.stemCount.Store(s.stemCount.Load())
	ns.hashedCount.Store(s.hashedCount.Load())
	ns.internalChunks = make([]*[storeChunkSize]InternalNode, len(s.internalChunks))
	for i, chunk := range s.internalChunks {
		cp := *chunk
		ns.internalChunks[i] = &cp
	}
	ns.stemChunks = make([]*[storeChunkSize]StemNode, len(s.stemChunks))
	for i, chunk := range s.stemChunks {
		cp := *chunk
		ns.stemChunks[i] = &cp
	}
	// Deep-copy each stem's value slots — they may alias serialized buffers,
	// so we can't rely on the chunk-wise struct copy above.
	stemCount := s.stemCount.Load()
	for i := uint32(0); i < stemCount; i++ {
		src := s.getStem(i)
		dst := ns.getStem(i)
		for j, v := range src.values {
			if v == nil {
				continue
			}
			cp := make([]byte, len(v))
			copy(cp, v)
			dst.values[j] = cp
		}
	}
	ns.hashedChunks = make([]*[storeChunkSize]HashedNode, len(s.hashedChunks))
	for i, chunk := range s.hashedChunks {
		cp := *chunk
		ns.hashedChunks[i] = &cp
	}
	if len(s.freeHashed) > 0 {
		ns.freeHashed = make([]uint32, len(s.freeHashed))
		copy(ns.freeHashed, s.freeHashed)
	}

	return ns
}

// copyFrom recursively copies the subtree rooted at srcRef from src into the
// receiver and returns the new ref in the receiver. Used by SplitRoot to
// build sub-view stores from a parent store, and by MergeRoot to fold the
// sub-views back into the parent. Stem values are deep-copied because the
// arena's value slices may alias serialized buffers.
func (dst *nodeStore) copyFrom(src *nodeStore, srcRef nodeRef) nodeRef {
	switch srcRef.Kind() {
	case kindEmpty:
		return emptyRef
	case kindInternal:
		srcNode := src.getInternal(srcRef.Index())
		dstIdx := dst.allocInternal()
		dstNode := dst.getInternal(dstIdx)
		dstNode.depth = srcNode.depth
		dstNode.mustRecompute = srcNode.mustRecompute
		dstNode.dirty = srcNode.dirty
		dstNode.hash = srcNode.hash
		dstNode.left = dst.copyFrom(src, srcNode.left)
		dstNode.right = dst.copyFrom(src, srcNode.right)
		return makeRef(kindInternal, dstIdx)
	case kindStem:
		srcStem := src.getStem(srcRef.Index())
		dstIdx := dst.allocStem()
		dstStem := dst.getStem(dstIdx)
		dstStem.Stem = srcStem.Stem
		dstStem.depth = srcStem.depth
		dstStem.mustRecompute = srcStem.mustRecompute
		dstStem.dirty = srcStem.dirty
		dstStem.hash = srcStem.hash
		for i, v := range srcStem.values {
			if v == nil {
				continue
			}
			cp := make([]byte, len(v))
			copy(cp, v)
			dstStem.values[i] = cp
		}
		return makeRef(kindStem, dstIdx)
	case kindHashed:
		hn := src.getHashed(srcRef.Index())
		return dst.newHashedRef(hn.Hash())
	default:
		panic("copyFrom: unknown node kind")
	}
}

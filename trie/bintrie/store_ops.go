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
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
)

// nodeResolverFn resolves a hashed node from the database.
type nodeResolverFn func([]byte, common.Hash) ([]byte, error)

// GetValue returns the value at (stem, suffix) or nil if absent. Thin
// wrapper over GetValuesAtStem — the underlying StemNode returns its
// 256-slot array as a slice header (no allocation), so the per-call cost
// is the tree walk plus one index.
func (s *nodeStore) GetValue(stem []byte, suffix byte, resolver nodeResolverFn) ([]byte, error) {
	values, err := s.GetValuesAtStem(stem, resolver)
	if err != nil || values == nil {
		return nil, err
	}
	return values[suffix], nil
}

// GetValuesAtStem returns the 256 value slots at stem, or nil if the stem
// is not in the trie. The returned slice is a view over the in-place
// StemNode values array (no allocation) and must be treated read-only.
func (s *nodeStore) GetValuesAtStem(stem []byte, resolver nodeResolverFn) ([][]byte, error) {
	cur := s.root
	var parentIdx uint32
	var hasParent bool
	var parentIsLeft bool

	for {
		switch cur.Kind() {
		case kindInternal:
			node := s.getInternal(cur.Index())
			if node.depth >= 31*8 {
				return nil, errors.New("node too deep")
			}
			bit := stem[node.depth/8] >> (7 - (node.depth % 8)) & 1
			parentIdx = cur.Index()
			hasParent = true
			if bit == 0 {
				parentIsLeft = true
				cur = node.left
			} else {
				parentIsLeft = false
				cur = node.right
			}

		case kindStem:
			sn := s.getStem(cur.Index())
			if sn.Stem != [StemSize]byte(stem[:StemSize]) {
				return nil, nil
			}
			return sn.allValues(), nil

		case kindHashed:
			// HashedNode at root is possible in SplitRoot sub-views; otherwise
			// it's a child of a previously-visited internal node.
			if resolver == nil {
				return nil, errors.New("getValuesAtStem: cannot resolve hashed node without resolver")
			}
			hn := s.getHashed(cur.Index())
			var (
				nodeDepth int
				path      []byte
			)
			if hasParent {
				nodeDepth = int(s.getInternal(parentIdx).depth) + 1
				p, err := keyToPath(nodeDepth-1, stem)
				if err != nil {
					return nil, fmt.Errorf("getValuesAtStem path error: %w", err)
				}
				path = p
			} else {
				// Root is hashed (sub-view). Its depth is the store's baseDepth.
				nodeDepth = int(s.baseDepth)
				if nodeDepth > 0 {
					p, err := keyToPath(nodeDepth-1, stem)
					if err != nil {
						return nil, fmt.Errorf("getValuesAtStem path error: %w", err)
					}
					path = p
				}
			}
			data, err := resolver(path, hn.Hash())
			if err != nil {
				return nil, fmt.Errorf("getValuesAtStem resolve error: %w", err)
			}
			resolved, err := s.deserializeNodeWithHash(data, nodeDepth, hn.Hash())
			if err != nil {
				return nil, fmt.Errorf("getValuesAtStem deserialization error: %w", err)
			}
			s.freeHashedNode(cur.Index())
			if hasParent {
				parentNode := s.getInternal(parentIdx)
				if parentIsLeft {
					parentNode.left = resolved
				} else {
					parentNode.right = resolved
				}
			} else {
				s.root = resolved
			}
			cur = resolved

		case kindEmpty:
			var values [StemNodeWidth][]byte
			return values[:], nil

		default:
			return nil, fmt.Errorf("getValuesAtStem: unexpected node kind %d", cur.Kind())
		}
	}
}

// InsertSingle writes a single value slot at (stem, suffix). Thin wrapper
// over InsertValuesAtStem — builds a stack-allocated 256-slot array with
// only the target slot set and delegates. Matches the original design
// gballet referenced (comment 3101751325): one primary insert path; the
// single-slot variant dispatches through it so the split / resolve logic
// lives in one place.
func (s *nodeStore) InsertSingle(stem []byte, suffix byte, value []byte, resolver nodeResolverFn) error {
	if len(value) != HashSize {
		return errors.New("invalid insertion: value length")
	}
	var values [StemNodeWidth][]byte
	values[suffix] = value
	return s.InsertValuesAtStem(stem, values[:], resolver)
}

// InsertValuesAtStem writes the supplied value slots at stem. values may be
// sparse (nil entries are ignored). The recursive implementation dispatches
// through the same body, so a single code path handles internal descent,
// HashedNode resolution, stem merge, and stem split.
//
// Operates against the store's root in non-cow mode. Sub-views that share a
// store with other writers must use InsertValuesAtStemCow.
func (s *nodeStore) InsertValuesAtStem(stem []byte, values [][]byte, resolver nodeResolverFn) error {
	var err error
	s.root, err = s.insertValuesAtStem(s.root, stem, values, resolver, int(s.baseDepth), false)
	return err
}

// InsertValuesAtStemCow performs the same logical operation as InsertValuesAtStem
// but starting from an explicit root ref and respecting the cow flag. When cow
// is true, internal nodes along the descent path are allocated fresh rather
// than mutated in place — required when multiple goroutines share the arena.
// Returns the new root ref.
func (s *nodeStore) InsertValuesAtStemCow(curRoot nodeRef, stem []byte, values [][]byte, resolver nodeResolverFn, cow bool) (nodeRef, error) {
	return s.insertValuesAtStem(curRoot, stem, values, resolver, int(s.baseDepth), cow)
}

func (s *nodeStore) insertValuesAtStem(ref nodeRef, stem []byte, values [][]byte, resolver nodeResolverFn, depth int, cow bool) (nodeRef, error) {
	switch ref.Kind() {
	case kindInternal:
		if cow {
			return s.insertValuesAtStemInternalCow(ref, stem, values, resolver, depth)
		}
		node := s.getInternal(ref.Index())
		bit := stem[node.depth/8] >> (7 - (node.depth % 8)) & 1
		if bit == 0 {
			if node.left.Kind() == kindHashed {
				if resolver == nil {
					return ref, errors.New("insertValuesAtStem: cannot resolve hashed node without resolver")
				}
				hn := s.getHashed(node.left.Index())
				path, err := keyToPath(int(node.depth), stem)
				if err != nil {
					return ref, fmt.Errorf("InsertValuesAtStem path error: %w", err)
				}
				data, err := resolver(path, hn.Hash())
				if err != nil {
					return ref, fmt.Errorf("InsertValuesAtStem resolve error: %w", err)
				}
				resolved, err := s.deserializeNodeWithHash(data, int(node.depth)+1, hn.Hash())
				if err != nil {
					return ref, fmt.Errorf("InsertValuesAtStem deserialization error: %w", err)
				}
				s.freeHashedNode(node.left.Index())
				node.left = resolved
			}
			newChild, err := s.insertValuesAtStem(node.left, stem, values, resolver, depth+1, cow)
			if err != nil {
				return ref, err
			}
			node.left = newChild
		} else {
			if node.right.Kind() == kindHashed {
				if resolver == nil {
					return ref, errors.New("insertValuesAtStem: cannot resolve hashed node without resolver")
				}
				hn := s.getHashed(node.right.Index())
				path, err := keyToPath(int(node.depth), stem)
				if err != nil {
					return ref, fmt.Errorf("InsertValuesAtStem path error: %w", err)
				}
				data, err := resolver(path, hn.Hash())
				if err != nil {
					return ref, fmt.Errorf("InsertValuesAtStem resolve error: %w", err)
				}
				resolved, err := s.deserializeNodeWithHash(data, int(node.depth)+1, hn.Hash())
				if err != nil {
					return ref, fmt.Errorf("InsertValuesAtStem deserialization error: %w", err)
				}
				s.freeHashedNode(node.right.Index())
				node.right = resolved
			}
			newChild, err := s.insertValuesAtStem(node.right, stem, values, resolver, depth+1, cow)
			if err != nil {
				return ref, err
			}
			node.right = newChild
		}
		node.mustRecompute = true
		node.dirty = true
		return ref, nil

	case kindStem:
		sn := s.getStem(ref.Index())
		if sn.Stem == [StemSize]byte(stem[:StemSize]) {
			// Same stem — merge values (setValue marks dirty+mustRecompute).
			// Under cow, allocate a fresh stem so the parent's view of the
			// original stem isn't mutated.
			if cow {
				newRef, newSn := s.cowStem(ref)
				for i, v := range values {
					if v != nil {
						newSn.setValue(byte(i), v)
					}
				}
				return newRef, nil
			}
			for i, v := range values {
				if v != nil {
					sn.setValue(byte(i), v)
				}
			}
			return ref, nil
		}
		// Different stem — split
		return s.splitStemValuesInsert(ref, stem, values, resolver, depth, cow)

	case kindHashed:
		hn := s.getHashed(ref.Index())
		if resolver == nil {
			return ref, errors.New("InsertValuesAtStem: resolver is nil")
		}
		// Path to *this* node has `depth` bits. keyToPath(d) returns d+1 bits,
		// so use depth-1; depth==0 (root) is the empty path.
		var path []byte
		if depth > 0 {
			var err error
			path, err = keyToPath(depth-1, stem)
			if err != nil {
				return ref, fmt.Errorf("InsertValuesAtStem path error: %w", err)
			}
		}
		data, err := resolver(path, hn.Hash())
		if err != nil {
			return ref, fmt.Errorf("InsertValuesAtStem resolve error: %w", err)
		}
		resolved, err := s.deserializeNodeWithHash(data, depth, hn.Hash())
		if err != nil {
			return ref, fmt.Errorf("InsertValuesAtStem deserialization error: %w", err)
		}
		// Under cow, leave the parent's hashed slot in place (it may still
		// be referenced from the parent's view). Otherwise free it for reuse.
		if !cow {
			s.freeHashedNode(ref.Index())
		}
		return s.insertValuesAtStem(resolved, stem, values, resolver, depth, cow)

	case kindEmpty:
		// Create new StemNode. Flag flips before the value loop so an
		// all-nil values input still marks the newly-created stem dirty.
		stemIdx := s.allocStem()
		sn := s.getStem(stemIdx)
		copy(sn.Stem[:], stem[:StemSize])
		sn.depth = uint8(depth)
		sn.mustRecompute = true
		sn.dirty = true
		for i, v := range values {
			if v != nil {
				sn.setValue(byte(i), v)
			}
		}
		return makeRef(kindStem, stemIdx), nil

	default:
		return ref, fmt.Errorf("insertValuesAtStem: unexpected kind %d", ref.Kind())
	}
}

// insertValuesAtStemInternalCow is the cow=true branch of insertValuesAtStem's
// kindInternal case. It allocates a fresh internal node via cowInternal, routes
// the modified child through it, and returns the new ref. The original
// internal node is never mutated, so a concurrent worker walking through it
// from a parent view sees a consistent state.
func (s *nodeStore) insertValuesAtStemInternalCow(ref nodeRef, stem []byte, values [][]byte, resolver nodeResolverFn, depth int) (nodeRef, error) {
	newRef, newNode := s.cowInternal(ref)
	bit := stem[newNode.depth/8] >> (7 - (newNode.depth % 8)) & 1
	if bit == 0 {
		if newNode.left.Kind() == kindHashed {
			if resolver == nil {
				return ref, errors.New("insertValuesAtStem: cannot resolve hashed node without resolver")
			}
			hn := s.getHashed(newNode.left.Index())
			path, err := keyToPath(int(newNode.depth), stem)
			if err != nil {
				return ref, fmt.Errorf("InsertValuesAtStem path error: %w", err)
			}
			data, err := resolver(path, hn.Hash())
			if err != nil {
				return ref, fmt.Errorf("InsertValuesAtStem resolve error: %w", err)
			}
			resolved, err := s.deserializeNodeWithHash(data, int(newNode.depth)+1, hn.Hash())
			if err != nil {
				return ref, fmt.Errorf("InsertValuesAtStem deserialization error: %w", err)
			}
			newNode.left = resolved
		}
		child, err := s.insertValuesAtStem(newNode.left, stem, values, resolver, depth+1, true)
		if err != nil {
			return ref, err
		}
		newNode.left = child
	} else {
		if newNode.right.Kind() == kindHashed {
			if resolver == nil {
				return ref, errors.New("insertValuesAtStem: cannot resolve hashed node without resolver")
			}
			hn := s.getHashed(newNode.right.Index())
			path, err := keyToPath(int(newNode.depth), stem)
			if err != nil {
				return ref, fmt.Errorf("InsertValuesAtStem path error: %w", err)
			}
			data, err := resolver(path, hn.Hash())
			if err != nil {
				return ref, fmt.Errorf("InsertValuesAtStem resolve error: %w", err)
			}
			resolved, err := s.deserializeNodeWithHash(data, int(newNode.depth)+1, hn.Hash())
			if err != nil {
				return ref, fmt.Errorf("InsertValuesAtStem deserialization error: %w", err)
			}
			newNode.right = resolved
		}
		child, err := s.insertValuesAtStem(newNode.right, stem, values, resolver, depth+1, true)
		if err != nil {
			return ref, err
		}
		newNode.right = child
	}
	return newRef, nil
}

// splitStemValuesInsert splits a StemNode when the new stem diverges.
// Under cow, the existing stem is itself copied (cowStem) so the parent's
// view of the original stem isn't mutated — the depth promotion lands on
// the COWed copy. The new internal node we allocate is parent-invisible
// either way (we just allocated it), so we can safely mutate it in place.
func (s *nodeStore) splitStemValuesInsert(existingRef nodeRef, newStem []byte, values [][]byte, resolver nodeResolverFn, depth int, cow bool) (nodeRef, error) {
	var (
		existing        *StemNode
		existingUsedRef = existingRef
	)
	if cow {
		existingUsedRef, existing = s.cowStem(existingRef)
	} else {
		existing = s.getStem(existingRef.Index())
	}

	if int(existing.depth) >= StemSize*8 {
		panic("splitStemValuesInsert: identical stems")
	}

	bitStem := existing.Stem[existing.depth/8] >> (7 - (existing.depth % 8)) & 1
	nRef := s.newInternalRef(int(existing.depth))
	nNode := s.getInternal(nRef.Index())
	existing.depth++
	// The existing stem's on-disk path is derived from its depth via
	// extendPathToGroupLeaf. Promoting its depth changes that path, so the
	// stem must be re-flushed at the new path; otherwise the old blob (at
	// the prior path) gets overwritten by the new ancestor internal blob
	// and the stem's data has no on-disk home.
	existing.dirty = true

	bitKey := newStem[nNode.depth/8] >> (7 - (nNode.depth % 8)) & 1
	if bitKey == bitStem {
		// Same direction — need deeper split
		var child nodeRef
		if bitStem == 0 {
			nNode.left = existingUsedRef
			child = nNode.left
		} else {
			nNode.right = existingUsedRef
			child = nNode.right
		}
		newChild, err := s.insertValuesAtStem(child, newStem, values, resolver, depth+1, cow)
		if err != nil {
			// Roll back the depth increment only in the non-cow path — under
			// cow we mutated the COWed copy, so the original is intact and
			// retry will get a fresh copy anyway.
			if !cow {
				existing.depth--
			}
			return nRef, err
		}
		if bitStem == 0 {
			nNode.left = newChild
			nNode.right = emptyRef
		} else {
			nNode.right = newChild
			nNode.left = emptyRef
		}
	} else {
		// Divergence — create new StemNode for the new values
		newStemIdx := s.allocStem()
		newSn := s.getStem(newStemIdx)
		copy(newSn.Stem[:], newStem[:StemSize])
		newSn.depth = nNode.depth + 1
		newSn.mustRecompute = true
		newSn.dirty = true
		for i, v := range values {
			if v != nil {
				newSn.setValue(byte(i), v)
			}
		}
		newStemRef := makeRef(kindStem, newStemIdx)

		if bitStem == 0 {
			nNode.left = existingUsedRef
			nNode.right = newStemRef
		} else {
			nNode.left = newStemRef
			nNode.right = existingUsedRef
		}
	}
	return nRef, nil
}

func (s *nodeStore) Insert(key []byte, value []byte, resolver nodeResolverFn) error {
	return s.InsertSingle(key[:StemSize], key[StemSize], value, resolver)
}

func (s *nodeStore) Get(key []byte, resolver nodeResolverFn) ([]byte, error) {
	return s.GetValue(key[:StemSize], key[StemSize], resolver)
}

func (s *nodeStore) getHeight(ref nodeRef) int {
	switch ref.Kind() {
	case kindInternal:
		node := s.getInternal(ref.Index())
		lh := s.getHeight(node.left)
		rh := s.getHeight(node.right)
		if lh > rh {
			return 1 + lh
		}
		return 1 + rh
	case kindStem:
		return 1
	case kindEmpty:
		return 0
	default:
		return 0
	}
}

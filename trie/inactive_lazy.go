// Copyright 2026 The go-ethereum Authors
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

package trie

// EIP-8188 lazy materialisation. When a write hits an *expiredNode, we walk
// only the path being modified rather than expanding the whole subtree.
// Off-path siblings remain as *expiredNode references (sub-stubs); off-path
// embedded-ref siblings must be fully materialised because their RLP form
// is inlined into the parent's hash and cannot be substituted.

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/triedb/inactive"
)

// materialiseLazyPath builds a partial subtree along `hexKey`, leaving
// off-path siblings as *expiredNode references. The returned subtree is
// suitable for the trie's insert/delete to descend into; at commit time,
// any *expiredNode children are folded into a hybrid chaindb entry by
// assembleHybridBytes.
func (t *Trie) materialiseLazyPath(n *expiredNode, hexKey []byte) (node, error) {
	if t.archiveResolver == nil {
		return nil, fmt.Errorf("trie: hit *expiredNode at file_offset=%d but no archive resolver attached", n.nodeFileOffset)
	}
	log.Debug("eip8188 lazy mat: descending into stub",
		"hash", n.hash,
		"blob-offset", n.blobOffset,
		"node-offset", n.nodeFileOffset,
		"size", n.size,
		"hex-key-suffix", common.Bytes2Hex(hexKey))
	reader := readerFromResolver(t.archiveResolver)
	return materialiseLazyByReader(reader, n.blobOffset, n.nodeFileOffset, n.size, hexKey, t.newFlag())
}

// materialiseLazyByReader walks one node entry along the on-path child;
// off-path siblings become either *expiredNode (hashed-ref) or fully-
// materialised live nodes (embedded-ref).
//
// The returned node has all its `dirty` flags set (newFlag) so the trie's
// commit pipeline emits fresh chaindb entries for it.
func materialiseLazyByReader(reader blobReader, blobOffset, nodeFileOffset uint64, size uint32, hexKey []byte, flag nodeFlag) (node, error) {
	nodeBytes, err := reader(nodeFileOffset, uint64(size))
	if err != nil {
		return nil, fmt.Errorf("trie: lazy materialise read: %w", err)
	}
	parsed, err := inactive.ParseNodeBytes(nodeBytes)
	if err != nil {
		return nil, fmt.Errorf("trie: lazy materialise parse: %w", err)
	}

	switch parsed.Tag {
	case inactive.TagFullNode:
		return lazyFullNode(reader, blobOffset, parsed, hexKey, flag)
	case inactive.TagShortNode:
		return lazyShortNode(reader, blobOffset, parsed, hexKey, flag)
	}
	return nil, fmt.Errorf("trie: lazy materialise: unknown tag %d", parsed.Tag)
}

// lazyFullNode produces a *fullNode whose on-path child is recursively
// lazy-materialised, and whose off-path siblings follow the substitution
// rules described above.
func lazyFullNode(reader blobReader, blobOffset uint64, parsed *inactive.ParsedNode, hexKey []byte, flag nodeFlag) (node, error) {
	if len(hexKey) == 0 {
		return nil, fmt.Errorf("trie: lazy materialise: empty hex suffix at fullNode")
	}
	target := int(hexKey[0]) // 0..15 or HexTerminator (16)
	if hexKey[0] == inactive.HexTerminator {
		target = 16
	} else if target > 15 {
		return nil, fmt.Errorf("trie: lazy materialise: invalid hex nibble %d", target)
	}

	fn := &fullNode{flags: flag}
	for i := 0; i < 17; i++ {
		slot := parsed.FullNodeChildren[i]
		child, err := materialiseSlot(reader, blobOffset, slot, i == target, advanceHexKeyForFullNode(hexKey, i), flag)
		if err != nil {
			return nil, fmt.Errorf("fullNode child %d: %w", i, err)
		}
		fn.Children[i] = child
	}
	return fn, nil
}

// lazyShortNode produces a *shortNode where the val is recursively
// lazy-materialised if the hexKey matches the shortNode's key prefix.
//
// If the hexKey diverges from the shortNode's key (write inserting a NEW
// branch under this node), we fall back to fully materialising the
// shortNode so the caller's insert/delete logic can split it.
func lazyShortNode(reader blobReader, blobOffset uint64, parsed *inactive.ParsedNode, hexKey []byte, flag nodeFlag) (node, error) {
	if !hasHexPrefix(hexKey, parsed.ShortKey) {
		// Path mismatch — write is creating a new branch at the divergence
		// point. Fall back to full materialisation so insert/delete sees
		// a regular live node.
		return fullyMaterialiseFromParsed(reader, blobOffset, parsed, flag)
	}
	rest := hexKey[len(parsed.ShortKey):]
	keyCopy := make([]byte, len(parsed.ShortKey))
	copy(keyCopy, parsed.ShortKey)

	slot := parsed.ShortChild
	switch {
	case slot.Kind == inactive.SlotKindInlineValue:
		// Leaf — the val is the leaf value. The trie's insert logic will
		// overwrite it; delete will simply remove this shortNode.
		valCopy := make([]byte, len(slot.Value))
		copy(valCopy, slot.Value)
		return &shortNode{Key: keyCopy, Val: valueNode(valCopy), flags: flag}, nil

	case slot.IsRef():
		// Extension — recurse into the val with the remaining suffix.
		// On-path: lazy. The val is by definition on the path because the
		// shortNode's key is a strict prefix of hexKey.
		childFileOffset := blobOffset + uint64(slot.Offset)
		child, err := materialiseLazyByReader(reader, blobOffset, childFileOffset, slot.Size, rest, flag)
		if err != nil {
			return nil, fmt.Errorf("shortNode val: %w", err)
		}
		return &shortNode{Key: keyCopy, Val: child, flags: flag}, nil
	}
	return nil, fmt.Errorf("trie: lazy materialise: shortNode child kind %d", slot.Kind)
}

// materialiseSlot turns one fullNode child slot into a node, choosing
// between on-path lazy descent, off-path *expiredNode substitution, and
// off-path full materialisation depending on the slot kind and whether the
// slot is on the modification path.
func materialiseSlot(reader blobReader, blobOffset uint64, slot inactive.ChildSlot, onPath bool, restKey []byte, flag nodeFlag) (node, error) {
	switch {
	case slot.Kind == inactive.SlotKindEmpty:
		return nil, nil

	case slot.Kind == inactive.SlotKindInlineValue:
		// Always inlined as valueNode regardless of on/off path. The trie's
		// insert/delete handles updating it.
		out := make([]byte, len(slot.Value))
		copy(out, slot.Value)
		return valueNode(out), nil

	case slot.Kind == inactive.SlotKindHashedRef:
		childFileOffset := blobOffset + uint64(slot.Offset)
		if onPath {
			return materialiseLazyByReader(reader, blobOffset, childFileOffset, slot.Size, restKey, flag)
		}
		// Off-path hashed sibling — substitute *expiredNode. The hash from
		// the slot drives expiredNode.encode() so the parent's RLP is
		// byte-identical to its pre-conversion form.
		return &expiredNode{
			blobOffset:     blobOffset,
			nodeFileOffset: childFileOffset,
			size:           slot.Size,
			hash:           common.BytesToHash(slot.Hash),
		}, nil

	case slot.Kind == inactive.SlotKindEmbeddedRef:
		childFileOffset := blobOffset + uint64(slot.Offset)
		if onPath {
			return materialiseLazyByReader(reader, blobOffset, childFileOffset, slot.Size, restKey, flag)
		}
		// Off-path embedded sibling — must fully materialise (the parent's
		// RLP inlines this child; substituting with a hash reference would
		// change the parent's hash).
		return fullyMaterialiseByReader(reader, blobOffset, childFileOffset, slot.Size, flag)
	}
	return nil, fmt.Errorf("trie: materialiseSlot: unknown slot kind %d", slot.Kind)
}

// advanceHexKeyForFullNode returns the hexKey to descend with when the
// caller is recursing into fullNode child index i. Only meaningful when
// i is on-path; for off-path indexes the returned slice is unused.
func advanceHexKeyForFullNode(hexKey []byte, i int) []byte {
	if len(hexKey) == 0 {
		return nil
	}
	if hexKey[0] == inactive.HexTerminator || i == 16 {
		// Terminator slot — the hexKey is fully consumed at this level.
		return nil
	}
	return hexKey[1:]
}

// fullyMaterialiseByReader expands a node entry and all its descendants into
// live `node`s. Used for off-path embedded-ref children (whose RLP is
// inlined into the parent's hash) and for the shortNode-mismatch fallback
// in lazy materialisation.
func fullyMaterialiseByReader(reader blobReader, blobOffset, nodeFileOffset uint64, size uint32, flag nodeFlag) (node, error) {
	nodeBytes, err := reader(nodeFileOffset, uint64(size))
	if err != nil {
		return nil, fmt.Errorf("trie: full materialise read: %w", err)
	}
	parsed, err := inactive.ParseNodeBytes(nodeBytes)
	if err != nil {
		return nil, fmt.Errorf("trie: full materialise parse: %w", err)
	}
	return fullyMaterialiseFromParsed(reader, blobOffset, parsed, flag)
}

func fullyMaterialiseFromParsed(reader blobReader, blobOffset uint64, parsed *inactive.ParsedNode, flag nodeFlag) (node, error) {
	switch parsed.Tag {
	case inactive.TagFullNode:
		fn := &fullNode{flags: flag}
		for i := 0; i < 17; i++ {
			slot := parsed.FullNodeChildren[i]
			child, err := fullyMaterialiseSlot(reader, blobOffset, slot, flag)
			if err != nil {
				return nil, fmt.Errorf("fullNode child %d: %w", i, err)
			}
			fn.Children[i] = child
		}
		return fn, nil

	case inactive.TagShortNode:
		child, err := fullyMaterialiseSlot(reader, blobOffset, parsed.ShortChild, flag)
		if err != nil {
			return nil, fmt.Errorf("shortNode child: %w", err)
		}
		key := make([]byte, len(parsed.ShortKey))
		copy(key, parsed.ShortKey)
		return &shortNode{Key: key, Val: child, flags: flag}, nil
	}
	return nil, fmt.Errorf("trie: full materialise: unknown tag %d", parsed.Tag)
}

func fullyMaterialiseSlot(reader blobReader, blobOffset uint64, slot inactive.ChildSlot, flag nodeFlag) (node, error) {
	switch {
	case slot.Kind == inactive.SlotKindEmpty:
		return nil, nil
	case slot.Kind == inactive.SlotKindInlineValue:
		out := make([]byte, len(slot.Value))
		copy(out, slot.Value)
		return valueNode(out), nil
	case slot.IsRef():
		return fullyMaterialiseByReader(reader, blobOffset, blobOffset+uint64(slot.Offset), slot.Size, flag)
	}
	return nil, fmt.Errorf("trie: full materialise slot: unknown kind %d", slot.Kind)
}

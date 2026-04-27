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

package inactive

// On-disk frozen-trie blob format (v2).
//
// v2 changes vs v1: each non-empty, non-value child slot now carries enough
// metadata for the lazy materialiser to re-emit the original parent's RLP
// (and therefore its hash) without descending into the child. Specifically,
// hashed-ref child slots carry the original 32-byte keccak hash of the child;
// embedded-ref slots carry only (offset, size) since they cannot be lazy-
// materialised (the parent's RLP inlines them, and the inlining cannot be
// substituted with a hash reference without changing the parent's hash).
//
//   header  (16 bytes)
//     bytes  0..1   uint16 BE   version (currently 2)
//     bytes  2..7   reserved (zero)
//     bytes  8..11  uint32 BE   absolute offset of the root node in the blob
//     bytes 12..15  uint32 BE   size of the root node in bytes
//
//   body
//     concatenated nodes in post-order DFS. Children are written before parents,
//     so the root is the LAST node. Header records the root's location.
//
//   node
//     byte 0   tag    1 = fullNode, 2 = shortNode
//     body    (variable)
//
//   fullNode body
//     17 child slots in order children[0..15] then value-slot at index 16.
//
//   shortNode body
//     bytes 0..1  uint16 BE   key length (hex nibbles, including terminator if leaf)
//     bytes 2..   key bytes (hex-encoded — one nibble per byte)
//     followed by 1 child slot
//
//   child slot
//     byte 0   kind:
//       0 = empty                 (no further bytes)
//       1 = hashed-ref            (32B hash + 4B rel_offset + 2B size = 38 trailing bytes)
//       2 = embedded-ref          (4B rel_offset + 2B size = 6 trailing bytes)
//       3 = inline-value          (2B value_len + value_bytes)
//
// Slot kind constraints by position:
//   - fullNode.children[0..15]: kind 0, 1, or 2 (never inline-value).
//   - fullNode.children[16] (value slot): kind 0 or 3.
//   - shortNode.child: kind 1, 2, or 3 (never empty).
//
// All multi-byte integers are big-endian. Offsets in child refs are absolute
// within the blob (i.e., from the start of the 16-byte header).
//
// Invariants:
//   - rel_offset never points before the header (offset >= 16).
//   - rel_offset + size <= len(blob).
//   - For shortNodes: a leaf has an inline-value child (kind=3) and the key ends in 0x10.
//                     an extension has a ref child (kind=1 or 2) and no terminator.

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	// StubMarker is the first byte of every primary stub written into chaindb
	// (full subtree replacement, 17 bytes). RLP-encoded MPT nodes always start
	// with byte >= 0xc0 (list prefix), so 0x00 unambiguously signals an
	// EIP-8188 stub. StubLen is the total stub length on disk.
	StubMarker byte = 0x00
	StubLen         = 1 + 8 + 8 // marker + offset (uint64 BE) + size (uint64 BE)

	// HybridMarker is the first byte of every hybrid node written to chaindb
	// (partially-materialised parent — see EIP-8188 v2). Distinguishes from
	// 0x00 (primary stub) and from 0xc0+ (standard RLP).
	HybridMarker byte = 0x01

	// HeaderSize is the size in bytes of the blob header.
	HeaderSize = 16

	// CurrentVersion is the format version produced by the encoder.
	CurrentVersion uint16 = 2

	// TagFullNode and TagShortNode are the per-node type tags.
	TagFullNode  byte = 0x01
	TagShortNode byte = 0x02

	// SlotKind* are the per-slot kind tags. See the format documentation above.
	SlotKindEmpty       byte = 0x00
	SlotKindHashedRef   byte = 0x01
	SlotKindEmbeddedRef byte = 0x02
	SlotKindInlineValue byte = 0x03

	// HashLen is the length of a child hash in a hashed-ref slot (keccak-256).
	HashLen = 32

	// HexTerminator is the trie's hex-encoding terminator byte indicating a
	// leaf-bearing path. Duplicated here (the trie package's constant is private)
	// so the navigator can recognise leaves without importing trie.
	HexTerminator byte = 0x10
)

// IsStub reports whether the supplied chaindb value is an EIP-8188 primary stub
// (full subtree replacement, marker 0x00). Pathdb's reader uses this to skip
// its hash check on stub blobs.
func IsStub(blob []byte) bool {
	return len(blob) > 0 && blob[0] == StubMarker
}

// IsHybrid reports whether the supplied chaindb value is an EIP-8188 hybrid
// node (partially-materialised parent, marker 0x01). Pathdb's reader uses this
// to skip its hash check on hybrid blobs (their keccak does not match the
// parent's claimed hash, since the metadata bytes are appended after the
// standard RLP).
func IsHybrid(blob []byte) bool {
	return len(blob) > 0 && blob[0] == HybridMarker
}

// IsStubOrHybrid reports whether the blob is either a primary stub or a hybrid
// node. Both categories require pathdb to skip its keccak verification.
func IsStubOrHybrid(blob []byte) bool {
	return IsStub(blob) || IsHybrid(blob)
}

// ErrNotFound is returned by NavigateBlob when the target key is not present
// in the subtree.
var ErrNotFound = errors.New("inactive blob: key not found")

// Header holds the parsed blob header.
type Header struct {
	Version    uint16
	RootOffset uint32
	RootSize   uint32
}

// ParseHeader reads the 16-byte header.
func ParseHeader(blob []byte) (Header, error) {
	if len(blob) < HeaderSize {
		return Header{}, fmt.Errorf("inactive blob: too short for header (%d < %d)", len(blob), HeaderSize)
	}
	h := Header{
		Version:    binary.BigEndian.Uint16(blob[0:2]),
		RootOffset: binary.BigEndian.Uint32(blob[8:12]),
		RootSize:   binary.BigEndian.Uint32(blob[12:16]),
	}
	if h.Version != CurrentVersion {
		return Header{}, fmt.Errorf("inactive blob: unsupported version %d (want %d)", h.Version, CurrentVersion)
	}
	if uint64(h.RootOffset)+uint64(h.RootSize) > uint64(len(blob)) {
		return Header{}, fmt.Errorf("inactive blob: header root range %d+%d exceeds blob size %d",
			h.RootOffset, h.RootSize, len(blob))
	}
	return h, nil
}

// EncodeHeader writes a 16-byte header into dst, which must have len >= HeaderSize.
func EncodeHeader(dst []byte, rootOffset, rootSize uint32) {
	binary.BigEndian.PutUint16(dst[0:2], CurrentVersion)
	// bytes 2..7 reserved (zeroed by caller)
	binary.BigEndian.PutUint32(dst[8:12], rootOffset)
	binary.BigEndian.PutUint32(dst[12:16], rootSize)
}

// ChildSlot represents a parsed child slot from a node body.
type ChildSlot struct {
	Kind   byte
	Hash   []byte // valid when Kind == SlotKindHashedRef (32 bytes, slice into the blob)
	Offset uint32 // valid when Kind in {SlotKindHashedRef, SlotKindEmbeddedRef}
	Size   uint32 // valid when Kind in {SlotKindHashedRef, SlotKindEmbeddedRef}
	Value  []byte // valid when Kind == SlotKindInlineValue (slice into the blob)
}

// IsRef reports whether the slot points to another node in the blob (either
// hashed-ref or embedded-ref). Empty and inline-value slots are not refs.
func (s ChildSlot) IsRef() bool {
	return s.Kind == SlotKindHashedRef || s.Kind == SlotKindEmbeddedRef
}

// readChildSlot parses one child slot starting at body[pos], returning the
// parsed slot and the number of bytes consumed.
func readChildSlot(body []byte, pos int) (ChildSlot, int, error) {
	if pos >= len(body) {
		return ChildSlot{}, 0, fmt.Errorf("inactive blob: truncated child slot at pos %d", pos)
	}
	kind := body[pos]
	switch kind {
	case SlotKindEmpty:
		return ChildSlot{Kind: SlotKindEmpty}, 1, nil
	case SlotKindHashedRef:
		// 1 (kind) + 32 (hash) + 4 (offset) + 2 (size) = 39 bytes total
		if pos+1+HashLen+6 > len(body) {
			return ChildSlot{}, 0, fmt.Errorf("inactive blob: truncated hashed-ref slot at pos %d", pos)
		}
		hash := body[pos+1 : pos+1+HashLen]
		off := binary.BigEndian.Uint32(body[pos+1+HashLen : pos+5+HashLen])
		sz := uint32(binary.BigEndian.Uint16(body[pos+5+HashLen : pos+7+HashLen]))
		return ChildSlot{Kind: SlotKindHashedRef, Hash: hash, Offset: off, Size: sz}, 1 + HashLen + 6, nil
	case SlotKindEmbeddedRef:
		// 1 (kind) + 4 (offset) + 2 (size) = 7 bytes total
		if pos+1+6 > len(body) {
			return ChildSlot{}, 0, fmt.Errorf("inactive blob: truncated embedded-ref slot at pos %d", pos)
		}
		off := binary.BigEndian.Uint32(body[pos+1 : pos+5])
		sz := uint32(binary.BigEndian.Uint16(body[pos+5 : pos+7]))
		return ChildSlot{Kind: SlotKindEmbeddedRef, Offset: off, Size: sz}, 7, nil
	case SlotKindInlineValue:
		if pos+1+2 > len(body) {
			return ChildSlot{}, 0, fmt.Errorf("inactive blob: truncated value slot at pos %d", pos)
		}
		vlen := int(binary.BigEndian.Uint16(body[pos+1 : pos+3]))
		if pos+3+vlen > len(body) {
			return ChildSlot{}, 0, fmt.Errorf("inactive blob: value slot extends past body (need %d, have %d)",
				pos+3+vlen, len(body))
		}
		return ChildSlot{Kind: SlotKindInlineValue, Value: body[pos+3 : pos+3+vlen]}, 3 + vlen, nil
	default:
		return ChildSlot{}, 0, fmt.Errorf("inactive blob: unknown slot kind 0x%02x at pos %d", kind, pos)
	}
}

// ParsedNode is a non-trie-aware view of a single node entry in the blob.
// Either FullNodeChildren is set (for tag=0x01) or ShortNode is set (for 0x02).
type ParsedNode struct {
	Tag              byte
	FullNodeChildren [17]ChildSlot // valid when Tag == TagFullNode
	ShortKey         []byte        // valid when Tag == TagShortNode (hex form, may include terminator)
	ShortChild       ChildSlot     // valid when Tag == TagShortNode
}

// ParseNode parses a node entry of the given size at the given offset. The
// caller is responsible for verifying offset+size is within blob bounds.
func ParseNode(blob []byte, offset, size uint32) (*ParsedNode, error) {
	if uint64(offset)+uint64(size) > uint64(len(blob)) {
		return nil, fmt.Errorf("inactive blob: node range %d+%d exceeds blob size %d",
			offset, size, len(blob))
	}
	if size == 0 {
		return nil, errors.New("inactive blob: zero-size node")
	}
	node := blob[offset : offset+size]
	return ParseNodeBytes(node)
}

// ParseNodeBytes parses a single node entry from the supplied bytes. Used by
// the per-step reader-based navigator, which loads exactly one node's bytes
// at a time rather than referring back into a full blob.
func ParseNodeBytes(node []byte) (*ParsedNode, error) {
	if len(node) == 0 {
		return nil, errors.New("inactive blob: zero-size node")
	}
	tag := node[0]
	body := node[1:]

	out := &ParsedNode{Tag: tag}
	switch tag {
	case TagFullNode:
		pos := 0
		for i := range 17 {
			slot, n, err := readChildSlot(body, pos)
			if err != nil {
				return nil, fmt.Errorf("fullNode child %d: %w", i, err)
			}
			out.FullNodeChildren[i] = slot
			pos += n
		}
		if pos != len(body) {
			return nil, fmt.Errorf("fullNode body has %d trailing bytes", len(body)-pos)
		}
	case TagShortNode:
		if len(body) < 2 {
			return nil, errors.New("shortNode body too short for keyLen")
		}
		keyLen := int(binary.BigEndian.Uint16(body[0:2]))
		if 2+keyLen > len(body) {
			return nil, fmt.Errorf("shortNode key extends past body (need %d, have %d)",
				2+keyLen, len(body))
		}
		out.ShortKey = body[2 : 2+keyLen]
		slot, n, err := readChildSlot(body, 2+keyLen)
		if err != nil {
			return nil, fmt.Errorf("shortNode child: %w", err)
		}
		out.ShortChild = slot
		if 2+keyLen+n != len(body) {
			return nil, fmt.Errorf("shortNode body has %d trailing bytes", len(body)-(2+keyLen+n))
		}
	default:
		return nil, fmt.Errorf("inactive blob: unknown node tag 0x%02x", tag)
	}
	return out, nil
}

// keyBytesToHex converts a raw key (e.g., keccak256 hash bytes) to the trie's
// hex-with-terminator form. Replicated from `trie/encoding.go:keybytesToHex`,
// which is unexported.
func keyBytesToHex(b []byte) []byte {
	out := make([]byte, len(b)*2+1)
	for i, v := range b {
		out[i*2] = v >> 4
		out[i*2+1] = v & 0x0f
	}
	out[len(out)-1] = HexTerminator
	return out
}

// hasHexPrefix reports whether `key` starts with `prefix`.
func hasHexPrefix(key, prefix []byte) bool {
	if len(prefix) > len(key) {
		return false
	}
	for i := range prefix {
		if key[i] != prefix[i] {
			return false
		}
	}
	return true
}

// NavigateBlob returns the leaf value stored at `key` within the blob, or
// (nil, ErrNotFound) if the key isn't present in this subtree. The returned
// slice points into the blob — callers must copy if they need to retain it
// across blob lifetime.
//
// `key` is the raw key bytes (e.g., keccak256(addr) for the account trie);
// the function converts to hex internally.
func NavigateBlob(blob []byte, key []byte) ([]byte, error) {
	hdr, err := ParseHeader(blob)
	if err != nil {
		return nil, err
	}
	hexKey := keyBytesToHex(key)
	return navigate(blob, hdr.RootOffset, hdr.RootSize, hexKey)
}

// navigate descends from the node at (offset, size) to find the leaf matching
// hexKey. hexKey is the remaining suffix to match (it shrinks as we descend).
// hexKey ends in HexTerminator.
func navigate(blob []byte, offset, size uint32, hexKey []byte) ([]byte, error) {
	n, err := ParseNode(blob, offset, size)
	if err != nil {
		return nil, err
	}
	switch n.Tag {
	case TagFullNode:
		// At a fullNode, look at hexKey[0]:
		// - if it's the terminator: return the value-slot at index 16.
		// - otherwise: follow Children[hexKey[0]].
		if len(hexKey) == 0 {
			return nil, fmt.Errorf("inactive blob: empty hexKey at fullNode")
		}
		if hexKey[0] == HexTerminator {
			slot := n.FullNodeChildren[16]
			switch slot.Kind {
			case SlotKindEmpty:
				return nil, ErrNotFound
			case SlotKindInlineValue:
				return slot.Value, nil
			default:
				return nil, fmt.Errorf("fullNode value slot has unexpected kind %d", slot.Kind)
			}
		}
		idx := int(hexKey[0])
		if idx > 15 {
			return nil, fmt.Errorf("invalid hex nibble %d in key", idx)
		}
		slot := n.FullNodeChildren[idx]
		switch {
		case slot.Kind == SlotKindEmpty:
			return nil, ErrNotFound
		case slot.IsRef():
			return navigate(blob, slot.Offset, slot.Size, hexKey[1:])
		case slot.Kind == SlotKindInlineValue:
			// Inline value at a non-terminator slot is unusual but can happen
			// if a leaf value was encoded inline at a fullNode child slot.
			// Treat as terminal.
			if len(hexKey) != 2 || hexKey[1] != HexTerminator {
				return nil, ErrNotFound
			}
			return slot.Value, nil
		}

	case TagShortNode:
		// shortNode has a key segment that hexKey must start with.
		if !hasHexPrefix(hexKey, n.ShortKey) {
			return nil, ErrNotFound
		}
		rest := hexKey[len(n.ShortKey):]
		switch {
		case n.ShortChild.IsRef():
			return navigate(blob, n.ShortChild.Offset, n.ShortChild.Size, rest)
		case n.ShortChild.Kind == SlotKindInlineValue:
			// Leaf node — rest must be empty (the leaf key already includes
			// the terminator).
			if len(rest) != 0 {
				return nil, ErrNotFound
			}
			return n.ShortChild.Value, nil
		default:
			return nil, fmt.Errorf("shortNode child has empty kind")
		}
	}
	return nil, fmt.Errorf("unreachable: node tag %d", n.Tag)
}

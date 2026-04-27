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

// EIP-8188 inactive-subtree integration. This file glues the trie's
// Get/Insert/Delete code paths to the frozen-trie blob format defined in
// triedb/inactive: navigation for reads, materialisation for writes, and
// encoding for the converter (run offline by `geth db convert-inactive`).

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/triedb/database"
	"github.com/ethereum/go-ethereum/triedb/inactive"
)

// blobReader fetches `size` bytes starting at `offset` within an inactive
// file. Used by the v2 navigator and lazy materialiser to read individual
// node entries on demand without preloading an entire subtree blob.
type blobReader func(offset, size uint64) ([]byte, error)

// readerFromResolver adapts an ArchiveResolverFn into a blobReader. v2 reads
// only the bytes of the next node at each step, so the underlying inactive
// file's pread interface is well suited.
func readerFromResolver(fn ArchiveResolverFn) blobReader {
	return func(offset, size uint64) ([]byte, error) {
		return fn(offset, size)
	}
}

// navigateInactive resolves the value at `key` (with `pos` nibbles already
// consumed by the path leading to this expiredNode) by walking the v2 blob
// one node at a time. Returns (nil, nil) when the key is not present in the
// subtree, matching the rest of the Get path.
//
// `n` is fully normalised by decodeStub (or by the lazy materialiser when
// constructing sub-stubs): nodeFileOffset addresses the actual node entry
// in the file, blobOffset is the containing blob's header position. Child
// slot offsets within the v2 format are blob-relative; the navigator turns
// each one into a file offset by adding blobOffset.
func (t *Trie) navigateInactive(n *expiredNode, key []byte, pos int) ([]byte, error) {
	if t.archiveResolver == nil {
		return nil, fmt.Errorf("trie: hit *expiredNode at file_offset=%d but no archive resolver attached", n.nodeFileOffset)
	}
	reader := readerFromResolver(t.archiveResolver)

	val, err := navigateNodeByReader(reader, n.blobOffset, n.nodeFileOffset, n.size, key[pos:])
	if errors.Is(err, inactive.ErrNotFound) {
		return nil, nil
	}
	return val, err
}

// navigateNodeByReader reads and parses one node entry at the given file
// offset, then follows children via blobOffset+slot.Offset. Pulls each
// step's bytes from `reader` rather than preloading the full blob.
func navigateNodeByReader(reader blobReader, blobOffset, nodeFileOffset uint64, size uint32, hexKey []byte) ([]byte, error) {
	nodeBytes, err := reader(nodeFileOffset, uint64(size))
	if err != nil {
		return nil, fmt.Errorf("trie: archive resolver: %w", err)
	}
	parsed, err := inactive.ParseNodeBytes(nodeBytes)
	if err != nil {
		return nil, err
	}
	switch parsed.Tag {
	case inactive.TagFullNode:
		if len(hexKey) == 0 {
			return nil, fmt.Errorf("inactive: empty hex suffix at fullNode")
		}
		if hexKey[0] == inactive.HexTerminator {
			slot := parsed.FullNodeChildren[16]
			switch slot.Kind {
			case inactive.SlotKindEmpty:
				return nil, inactive.ErrNotFound
			case inactive.SlotKindInlineValue:
				return common.CopyBytes(slot.Value), nil
			default:
				return nil, fmt.Errorf("inactive: fullNode value slot has kind %d", slot.Kind)
			}
		}
		idx := int(hexKey[0])
		if idx > 15 {
			return nil, fmt.Errorf("inactive: invalid hex nibble %d", idx)
		}
		slot := parsed.FullNodeChildren[idx]
		switch {
		case slot.Kind == inactive.SlotKindEmpty:
			return nil, inactive.ErrNotFound
		case slot.IsRef():
			return navigateNodeByReader(reader, blobOffset, blobOffset+uint64(slot.Offset), slot.Size, hexKey[1:])
		case slot.Kind == inactive.SlotKindInlineValue:
			if len(hexKey) != 2 || hexKey[1] != inactive.HexTerminator {
				return nil, inactive.ErrNotFound
			}
			return common.CopyBytes(slot.Value), nil
		}
	case inactive.TagShortNode:
		if !hasHexPrefix(hexKey, parsed.ShortKey) {
			return nil, inactive.ErrNotFound
		}
		rest := hexKey[len(parsed.ShortKey):]
		switch {
		case parsed.ShortChild.IsRef():
			return navigateNodeByReader(reader, blobOffset, blobOffset+uint64(parsed.ShortChild.Offset), parsed.ShortChild.Size, rest)
		case parsed.ShortChild.Kind == inactive.SlotKindInlineValue:
			if len(rest) != 0 {
				return nil, inactive.ErrNotFound
			}
			return common.CopyBytes(parsed.ShortChild.Value), nil
		default:
			return nil, fmt.Errorf("inactive: shortNode child empty kind")
		}
	}
	return nil, fmt.Errorf("inactive: unreachable, tag %d", parsed.Tag)
}

// hasHexPrefix reports whether hexKey begins with prefix.
func hasHexPrefix(hexKey, prefix []byte) bool {
	if len(prefix) > len(hexKey) {
		return false
	}
	return bytes.Equal(prefix, hexKey[:len(prefix)])
}

// ============================================================================
// Encoding (v2): live `node` tree → blob bytes.
//
// v2 stores per-child kind+hash so the lazy materialiser can substitute
// off-path siblings with *expiredNode references whose RLP serialisation
// matches the original child's hash byte-for-byte (preserving the parent's
// MPT hash).
// ============================================================================

// EncodeInactiveBlob serialises a live subtree (rooted at `root`) into a
// frozen-trie blob suitable for appending to inactive.bin. The encoder
// rejects unresolved hashNode children — the caller must materialise the
// entire subtree before passing it in.
//
// Used by the offline converter (`geth db convert-inactive`).
func EncodeInactiveBlob(root node) ([]byte, error) {
	if root == nil {
		return nil, errors.New("trie: cannot encode nil root for inactive blob")
	}
	var body bytes.Buffer
	rootOff, rootSize, _, err := encodeInactiveNode(&body, root)
	if err != nil {
		return nil, err
	}
	out := make([]byte, inactive.HeaderSize+body.Len())
	inactive.EncodeHeader(out, rootOff, rootSize)
	copy(out[inactive.HeaderSize:], body.Bytes())
	return out, nil
}

// encodeInactiveNode writes node n to body in post-order DFS. Returns the
// blob (offset, size) of n's entry plus the standard MPT RLP of n. Standard
// RLP is required at every level because the parent encoder needs to know
// each child's collapsed-form size: < 32 bytes ⇒ embed inline (slot kind 2);
// ≥ 32 bytes ⇒ reference by hash (slot kind 1).
func encodeInactiveNode(body *bytes.Buffer, n node) (offset, size uint32, stdRLP []byte, err error) {
	switch n := n.(type) {
	case *fullNode:
		var slots [17]encSlot
		var rlpElems [17][]byte // standard RLP elements for the parent's RLP computation
		for i := 0; i < 17; i++ {
			s, elem, err := encodeChild(body, n.Children[i], i == 16)
			if err != nil {
				return 0, 0, nil, fmt.Errorf("fullNode child %d: %w", i, err)
			}
			slots[i] = s
			rlpElems[i] = elem
		}
		stdRLP = encodeFullNodeStdRLP(slots, rlpElems)
		offset = uint32(inactive.HeaderSize + body.Len())
		body.WriteByte(inactive.TagFullNode)
		for _, s := range slots {
			writeSlot(body, s)
		}
		size = uint32(inactive.HeaderSize+body.Len()) - offset
		return offset, size, stdRLP, nil

	case *shortNode:
		s, elem, err := encodeChild(body, n.Val, false)
		if err != nil {
			return 0, 0, nil, fmt.Errorf("shortNode val: %w", err)
		}
		stdRLP = encodeShortNodeStdRLP(n.Key, s, elem)
		offset = uint32(inactive.HeaderSize + body.Len())
		body.WriteByte(inactive.TagShortNode)
		var keyLen [2]byte
		binary.BigEndian.PutUint16(keyLen[:], uint16(len(n.Key)))
		body.Write(keyLen[:])
		body.Write(n.Key)
		writeSlot(body, s)
		size = uint32(inactive.HeaderSize+body.Len()) - offset
		return offset, size, stdRLP, nil

	case hashNode:
		return 0, 0, nil, errors.New("trie: cannot encode unresolved hashNode in inactive blob (subtree must be fully materialised)")
	case valueNode:
		return 0, 0, nil, errors.New("trie: encodeInactiveNode called on valueNode (must be inlined via parent)")
	case *expiredNode:
		return 0, 0, nil, errors.New("trie: cannot encode *expiredNode (already in another inactive blob)")
	case nil:
		return 0, 0, nil, errors.New("trie: cannot encode nil node")
	}
	return 0, 0, nil, fmt.Errorf("trie: unknown node type %T", n)
}

// encSlot is an in-flight description of a child slot before serialisation
// into the body. Kind dictates which fields are valid:
//   - SlotKindEmpty: no payload
//   - SlotKindHashedRef: hash (32B), offset, size
//   - SlotKindEmbeddedRef: offset, size
//   - SlotKindInlineValue: value
type encSlot struct {
	kind   byte
	hash   []byte
	offset uint32
	size   uint32
	value  []byte
}

// encodeChild dispatches a child node to its appropriate slot kind and
// returns BOTH the slot description AND the child's "RLP element"
// representation (used by the parent's standard-RLP computation).
//
// `isFullNodeValueSlot` is true when this child is the value slot at index
// 16 of a fullNode. Such slots can only be empty or inline-value (never refs).
func encodeChild(body *bytes.Buffer, child node, isFullNodeValueSlot bool) (encSlot, []byte, error) {
	switch c := child.(type) {
	case nil:
		return encSlot{kind: inactive.SlotKindEmpty}, nil, nil
	case valueNode:
		// For fullNode value slot AND shortNode leaf val, the RLP element form
		// is the raw value bytes (the parent's encoder string-encodes them).
		val := []byte(c)
		return encSlot{kind: inactive.SlotKindInlineValue, value: val}, val, nil
	case *shortNode, *fullNode:
		if isFullNodeValueSlot {
			return encSlot{}, nil, errors.New("trie: fullNode value slot must not contain an internal node")
		}
		off, sz, childRLP, err := encodeInactiveNode(body, c)
		if err != nil {
			return encSlot{}, nil, err
		}
		if len(childRLP) >= 32 {
			// Hashed-ref: the child would have been stored separately in chaindb
			// under its own keccak. Capture the hash so the lazy materialiser
			// can re-emit it as part of the parent's RLP without descending.
			h := crypto.Keccak256(childRLP)
			return encSlot{kind: inactive.SlotKindHashedRef, hash: h, offset: off, size: sz}, h, nil
		}
		// Embedded-ref: the child was inlined into the parent's RLP; the parent
		// encoder writes the child's RLP raw (no string wrapping).
		return encSlot{kind: inactive.SlotKindEmbeddedRef, offset: off, size: sz}, childRLP, nil
	case hashNode:
		return encSlot{}, nil, errors.New("trie: unresolved hashNode encountered during encode (must materialise first)")
	case *expiredNode:
		return encSlot{}, nil, errors.New("trie: nested *expiredNode encountered during encode")
	}
	return encSlot{}, nil, fmt.Errorf("trie: unknown node type %T", child)
}

// writeSlot writes the on-disk representation of a child slot.
func writeSlot(body *bytes.Buffer, s encSlot) {
	body.WriteByte(s.kind)
	switch s.kind {
	case inactive.SlotKindEmpty:
		// no further bytes
	case inactive.SlotKindHashedRef:
		body.Write(s.hash) // 32 bytes
		var buf [6]byte
		binary.BigEndian.PutUint32(buf[0:4], s.offset)
		binary.BigEndian.PutUint16(buf[4:6], uint16(s.size))
		body.Write(buf[:])
	case inactive.SlotKindEmbeddedRef:
		var buf [6]byte
		binary.BigEndian.PutUint32(buf[0:4], s.offset)
		binary.BigEndian.PutUint16(buf[4:6], uint16(s.size))
		body.Write(buf[:])
	case inactive.SlotKindInlineValue:
		var lenBuf [2]byte
		binary.BigEndian.PutUint16(lenBuf[:], uint16(len(s.value)))
		body.Write(lenBuf[:])
		body.Write(s.value)
	}
}

// encodeFullNodeStdRLP produces the standard MPT RLP of a fullNode given the
// per-slot kind and "RLP element" bytes (as produced by encodeChild).
//
// Per-slot encoding rules:
//   - empty: rlp.EmptyString
//   - hashed-ref: WriteBytes(32-byte hash) — string-encoded with 0xa0 prefix
//   - embedded-ref: Write(raw RLP) — the child's RLP list is included verbatim
//   - inline-value (slot 16 only): WriteBytes(value) — string-encoded
func encodeFullNodeStdRLP(slots [17]encSlot, elems [17][]byte) []byte {
	w := rlp.NewEncoderBuffer(nil)
	listOff := w.List()
	for i, s := range slots {
		switch s.kind {
		case inactive.SlotKindEmpty:
			w.Write(rlp.EmptyString)
		case inactive.SlotKindHashedRef:
			w.WriteBytes(elems[i])
		case inactive.SlotKindEmbeddedRef:
			w.Write(elems[i])
		case inactive.SlotKindInlineValue:
			w.WriteBytes(elems[i])
		}
	}
	w.ListEnd(listOff)
	out := w.ToBytes()
	w.Flush()
	return out
}

// encodeShortNodeStdRLP produces the standard MPT RLP of a shortNode. A
// shortNode is either a leaf (val is inline-value) or an extension (val is a
// ref). The child's RLP element form has been computed by the caller.
func encodeShortNodeStdRLP(hexKey []byte, child encSlot, childElem []byte) []byte {
	keyCompact := hexToCompact(hexKey)
	switch child.kind {
	case inactive.SlotKindInlineValue:
		// Leaf: RLP list of two byte-strings.
		enc := &leafNodeEncoder{Key: keyCompact, Val: childElem}
		w := rlp.NewEncoderBuffer(nil)
		enc.encode(w)
		out := w.ToBytes()
		w.Flush()
		return out
	case inactive.SlotKindHashedRef, inactive.SlotKindEmbeddedRef:
		// Extension: extNodeEncoder dispatches on len(Val) — < 32 raw, ≥ 32 byte-string.
		// For embedded refs childElem is the child's full RLP (< 32 bytes); for
		// hashed refs childElem is the 32-byte hash.
		enc := &extNodeEncoder{Key: keyCompact, Val: childElem}
		w := rlp.NewEncoderBuffer(nil)
		enc.encode(w)
		out := w.ToBytes()
		w.Flush()
		return out
	}
	// Defensive: unreachable given current call sites.
	return nil
}

// ============================================================================
// Live-subtree materialisation: chaindb → live `node` tree.
//
// Used by the converter (`geth db convert-inactive`) to build the in-memory
// representation of a subtree before encoding it as a frozen-trie blob.
// Walks the database via the supplied NodeReader, resolving every hashNode
// reference recursively until the entire subtree is in memory.
// ============================================================================

// LoadedNodePath is the absolute hex-nibble path of a node fetched from the
// chaindb during materialisation. Embedded nodes (size < 32 bytes inlined in
// their parent's RLP) do NOT appear here — they have no separate DB key.
type LoadedNodePath []byte

// MaterialiseLiveSubtree resolves the subtree rooted at (owner, rootPath, rootHash)
// from the live chaindb into a fully-resolved `node` tree. The returned
// node has no remaining hashNode references — every internal node and leaf
// is materialised. Suitable input for EncodeInactiveBlob.
//
// The second return value is the list of every chaindb-resident node path
// visited during materialisation, in DFS order (root first). The converter
// uses this list to delete the original trie nodes after writing the stub.
func MaterialiseLiveSubtree(reader database.NodeReader, owner common.Hash, rootPath []byte, rootHash common.Hash) (node, []LoadedNodePath, error) {
	if reader == nil {
		return nil, nil, errors.New("trie: nil NodeReader")
	}
	var loaded []LoadedNodePath
	root, err := resolveLiveTracking(reader, owner, rootPath, rootHash, &loaded)
	if err != nil {
		return nil, nil, err
	}
	return root, loaded, nil
}

// resolveLiveTracking is resolveLive with side-effect tracking of visited paths.
func resolveLiveTracking(reader database.NodeReader, owner common.Hash, path []byte, hash common.Hash, loaded *[]LoadedNodePath) (node, error) {
	*loaded = append(*loaded, append(LoadedNodePath{}, path...))
	blob, err := reader.Node(owner, path, hash)
	if err != nil {
		return nil, fmt.Errorf("read node at owner=%x path=%x hash=%x: %w", owner, path, hash, err)
	}
	if len(blob) == 0 {
		return nil, fmt.Errorf("missing node at owner=%x path=%x hash=%x", owner, path, hash)
	}
	n, err := decodeNodeUnsafe(hash[:], blob)
	if err != nil {
		return nil, fmt.Errorf("decode node at path=%x: %w", path, err)
	}
	return resolveLiveChildrenTracking(reader, owner, path, n, loaded)
}

func resolveLiveChildrenTracking(reader database.NodeReader, owner common.Hash, currentPath []byte, n node, loaded *[]LoadedNodePath) (node, error) {
	switch n := n.(type) {
	case *fullNode:
		for i, c := range &n.Children {
			switch c := c.(type) {
			case nil, valueNode:
			case hashNode:
				childPath := append(append([]byte{}, currentPath...), byte(i))
				resolved, err := resolveLiveTracking(reader, owner, childPath, common.BytesToHash(c), loaded)
				if err != nil {
					return nil, err
				}
				n.Children[i] = resolved
			case *fullNode, *shortNode:
				resolved, err := resolveLiveChildrenTracking(reader, owner, currentPath, c, loaded)
				if err != nil {
					return nil, err
				}
				n.Children[i] = resolved
			case *expiredNode:
				return nil, fmt.Errorf("trie: nested *expiredNode at owner=%x path=%x", owner, currentPath)
			default:
				return nil, fmt.Errorf("trie: unknown child type %T at owner=%x path=%x[%d]", c, owner, currentPath, i)
			}
		}
		return n, nil
	case *shortNode:
		switch c := n.Val.(type) {
		case nil, valueNode:
		case hashNode:
			childPath := append(append([]byte{}, currentPath...), n.Key...)
			resolved, err := resolveLiveTracking(reader, owner, childPath, common.BytesToHash(c), loaded)
			if err != nil {
				return nil, err
			}
			n.Val = resolved
		case *fullNode, *shortNode:
			extPath := append(append([]byte{}, currentPath...), n.Key...)
			resolved, err := resolveLiveChildrenTracking(reader, owner, extPath, c, loaded)
			if err != nil {
				return nil, err
			}
			n.Val = resolved
		case *expiredNode:
			return nil, fmt.Errorf("trie: nested *expiredNode at owner=%x path=%x", owner, currentPath)
		default:
			return nil, fmt.Errorf("trie: unknown shortNode child type %T at owner=%x path=%x", c, owner, currentPath)
		}
		return n, nil
	case valueNode, nil:
		return n, nil
	case hashNode:
		return nil, fmt.Errorf("trie: unresolved hashNode at owner=%x path=%x", owner, currentPath)
	case *expiredNode:
		return nil, fmt.Errorf("trie: *expiredNode at owner=%x path=%x", owner, currentPath)
	}
	return nil, fmt.Errorf("trie: unknown node type %T at owner=%x path=%x", n, owner, currentPath)
}

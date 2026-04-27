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

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

// blobBuilder is a small helper that constructs frozen-trie blobs by hand so
// the navigator can be exercised without dragging in the trie package's
// private node types.
//
// The builder collects nodes in post-order: callers first emit child nodes
// (capturing the returned offset/size), then build the parent referencing
// those positions.
type blobBuilder struct {
	buf bytes.Buffer
}

// putValueSlot encodes a (kind=inline-value) child slot containing `v`.
func putValueSlot(out *bytes.Buffer, v []byte) {
	out.WriteByte(SlotKindInlineValue)
	var lenBuf [2]byte
	binary.BigEndian.PutUint16(lenBuf[:], uint16(len(v)))
	out.Write(lenBuf[:])
	out.Write(v)
}

// putHashedRefSlot encodes a (kind=hashed-ref) child slot pointing at
// (offset, size) and carrying the supplied 32-byte hash.
func putHashedRefSlot(out *bytes.Buffer, hash []byte, offset, size uint32) {
	if len(hash) != HashLen {
		panic("hash must be 32 bytes")
	}
	out.WriteByte(SlotKindHashedRef)
	out.Write(hash)
	var buf [6]byte
	binary.BigEndian.PutUint32(buf[0:4], offset)
	binary.BigEndian.PutUint16(buf[4:6], uint16(size))
	out.Write(buf[:])
}

// putEmbeddedRefSlot encodes a (kind=embedded-ref) child slot pointing at
// (offset, size). Embedded-refs carry no hash.
func putEmbeddedRefSlot(out *bytes.Buffer, offset, size uint32) {
	out.WriteByte(SlotKindEmbeddedRef)
	var buf [6]byte
	binary.BigEndian.PutUint32(buf[0:4], offset)
	binary.BigEndian.PutUint16(buf[4:6], uint16(size))
	out.Write(buf[:])
}

// putEmptySlot encodes an empty slot.
func putEmptySlot(out *bytes.Buffer) {
	out.WriteByte(SlotKindEmpty)
}

// emit writes a pre-built node body (with its tag prefix) to the builder and
// returns its (offset, size) within the body.
func (b *blobBuilder) emit(node []byte) (offset, size uint32) {
	offset = HeaderSize + uint32(b.buf.Len())
	b.buf.Write(node)
	return offset, uint32(len(node))
}

// finalize prepends the header and returns the complete blob.
func (b *blobBuilder) finalize(rootOffset, rootSize uint32) []byte {
	out := make([]byte, HeaderSize+b.buf.Len())
	EncodeHeader(out, rootOffset, rootSize)
	copy(out[HeaderSize:], b.buf.Bytes())
	return out
}

// buildLeafShortNode constructs a shortNode with a leaf-terminator key that
// stores `value` inline. Returns its body bytes (tag + body). Caller adds it
// to a builder.
func buildLeafShortNode(hexKeyWithTerm []byte, value []byte) []byte {
	var b bytes.Buffer
	b.WriteByte(TagShortNode)
	var keyLen [2]byte
	binary.BigEndian.PutUint16(keyLen[:], uint16(len(hexKeyWithTerm)))
	b.Write(keyLen[:])
	b.Write(hexKeyWithTerm)
	putValueSlot(&b, value)
	return b.Bytes()
}

// buildExtensionShortNode constructs a shortNode with a non-terminator key
// that points at a child node at (childOff, childSize) via a hashed-ref slot.
// `childHash` may be any 32 bytes; the navigator does not consult it.
func buildExtensionShortNode(hexKey, childHash []byte, childOff, childSize uint32) []byte {
	var b bytes.Buffer
	b.WriteByte(TagShortNode)
	var keyLen [2]byte
	binary.BigEndian.PutUint16(keyLen[:], uint16(len(hexKey)))
	b.Write(keyLen[:])
	b.Write(hexKey)
	putHashedRefSlot(&b, childHash, childOff, childSize)
	return b.Bytes()
}

// slotSpec is a test-only description of a single child slot in a fullNode.
type slotSpec struct {
	kind   byte
	hash   []byte // valid for SlotKindHashedRef
	offset uint32
	size   uint32
	value  []byte
}

func buildFullNode(slots [17]slotSpec) []byte {
	var b bytes.Buffer
	b.WriteByte(TagFullNode)
	for _, s := range slots {
		switch s.kind {
		case SlotKindEmpty:
			putEmptySlot(&b)
		case SlotKindHashedRef:
			putHashedRefSlot(&b, s.hash, s.offset, s.size)
		case SlotKindEmbeddedRef:
			putEmbeddedRefSlot(&b, s.offset, s.size)
		case SlotKindInlineValue:
			putValueSlot(&b, s.value)
		}
	}
	return b.Bytes()
}

// dummyHash returns a deterministic 32-byte filler whose contents are simply
// the supplied byte repeated. Useful for building hashed-ref slots in tests
// that don't actually verify hash contents.
func dummyHash(b byte) []byte {
	out := make([]byte, HashLen)
	for i := range out {
		out[i] = b
	}
	return out
}

// keyToHex is a test alias for keyBytesToHex (the unexported converter).
func keyToHex(b []byte) []byte { return keyBytesToHex(b) }

// TestNavigateSingleLeafShortNode: a blob containing only a leaf shortNode.
// The whole tree is one node — the root.
func TestNavigateSingleLeafShortNode(t *testing.T) {
	// Key = 0xab → hex nibbles [10, 11] + terminator.
	rawKey := []byte{0xab}
	hexKey := keyToHex(rawKey)
	value := []byte("hello")

	b := &blobBuilder{}
	leafBytes := buildLeafShortNode(hexKey, value)
	rootOff, rootSize := b.emit(leafBytes)
	blob := b.finalize(rootOff, rootSize)

	got, err := NavigateBlob(blob, rawKey)
	if err != nil {
		t.Fatalf("NavigateBlob: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Errorf("got %x, want %x", got, value)
	}
}

// TestNavigateMissingKey: a key that doesn't match the leaf's key prefix
// returns ErrNotFound.
func TestNavigateMissingKey(t *testing.T) {
	b := &blobBuilder{}
	leaf := buildLeafShortNode(keyToHex([]byte{0xab}), []byte("present"))
	rootOff, rootSize := b.emit(leaf)
	blob := b.finalize(rootOff, rootSize)

	_, err := NavigateBlob(blob, []byte{0xcd})
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("missing key: got %v, want ErrNotFound", err)
	}
}

// TestNavigateFullNodeWithLeafChildren: a fullNode where two children point
// at leaf shortNodes. Verifies fullNode-with-ref descent and leaf reads.
func TestNavigateFullNodeWithLeafChildren(t *testing.T) {
	// Two keys differ in the FIRST nibble:
	//   key1 = 0x12 → hex [1, 2, term]      → branch[1] → short(key=[2,term], val="A")
	//   key2 = 0x34 → hex [3, 4, term]      → branch[3] → short(key=[4,term], val="B")
	b := &blobBuilder{}
	leaf1 := buildLeafShortNode([]byte{2, HexTerminator}, []byte("A"))
	off1, sz1 := b.emit(leaf1)
	leaf2 := buildLeafShortNode([]byte{4, HexTerminator}, []byte("B"))
	off2, sz2 := b.emit(leaf2)

	var slots [17]slotSpec
	slots[1] = slotSpec{kind: SlotKindHashedRef, hash: dummyHash(0x11), offset: off1, size: sz1}
	slots[3] = slotSpec{kind: SlotKindHashedRef, hash: dummyHash(0x33), offset: off2, size: sz2}
	rootBytes := buildFullNode(slots)
	rootOff, rootSize := b.emit(rootBytes)
	blob := b.finalize(rootOff, rootSize)

	got, err := NavigateBlob(blob, []byte{0x12})
	if err != nil {
		t.Fatalf("Navigate(0x12): %v", err)
	}
	if !bytes.Equal(got, []byte("A")) {
		t.Errorf("Navigate(0x12) = %x, want %x", got, []byte("A"))
	}

	got, err = NavigateBlob(blob, []byte{0x34})
	if err != nil {
		t.Fatalf("Navigate(0x34): %v", err)
	}
	if !bytes.Equal(got, []byte("B")) {
		t.Errorf("Navigate(0x34) = %x, want %x", got, []byte("B"))
	}
}

// TestNavigateFullNodeMissingChild: requesting a key whose first nibble has
// an empty slot in the fullNode returns ErrNotFound.
func TestNavigateFullNodeMissingChild(t *testing.T) {
	b := &blobBuilder{}
	leaf := buildLeafShortNode([]byte{2, HexTerminator}, []byte("A"))
	off, sz := b.emit(leaf)

	var slots [17]slotSpec
	slots[1] = slotSpec{kind: SlotKindHashedRef, hash: dummyHash(0x11), offset: off, size: sz}
	rootBytes := buildFullNode(slots)
	rootOff, rootSize := b.emit(rootBytes)
	blob := b.finalize(rootOff, rootSize)

	if _, err := NavigateBlob(blob, []byte{0x52}); !errors.Is(err, ErrNotFound) {
		t.Errorf("got %v, want ErrNotFound", err)
	}
}

// TestNavigateExtensionThenFullNode: an extension shortNode under which a
// fullNode lives. Tests path compression decoding.
func TestNavigateExtensionThenFullNode(t *testing.T) {
	b := &blobBuilder{}

	leafX := buildLeafShortNode([]byte{HexTerminator}, []byte("X"))
	offX, szX := b.emit(leafX)
	leafY := buildLeafShortNode([]byte{HexTerminator}, []byte("Y"))
	offY, szY := b.emit(leafY)

	var slots [17]slotSpec
	slots[3] = slotSpec{kind: SlotKindHashedRef, hash: dummyHash(0xa1), offset: offX, size: szX}
	slots[5] = slotSpec{kind: SlotKindHashedRef, hash: dummyHash(0xa2), offset: offY, size: szY}
	branch := buildFullNode(slots)
	offBranch, szBranch := b.emit(branch)

	ext := buildExtensionShortNode([]byte{1, 2}, dummyHash(0xc0), offBranch, szBranch)
	rootOff, rootSize := b.emit(ext)
	blob := b.finalize(rootOff, rootSize)

	// Confirm the blob parses; missing key returns ErrNotFound.
	_, err := NavigateBlob(blob, []byte{0x99})
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("0x99: got %v, want ErrNotFound", err)
	}
}

// TestNavigateFullNodeValueSlot: fullNode with a value at index 16, accessed
// via a key whose hex form ends exactly at the fullNode's depth.
func TestNavigateFullNodeValueSlot(t *testing.T) {
	// rawKey = empty bytes → hex = [term]. Its only nibble IS the terminator,
	// so it hits index 16 of the root fullNode.
	b := &blobBuilder{}

	var slots [17]slotSpec
	slots[16] = slotSpec{kind: SlotKindInlineValue, value: []byte("VALUE")}
	rootBytes := buildFullNode(slots)
	rootOff, rootSize := b.emit(rootBytes)
	blob := b.finalize(rootOff, rootSize)

	got, err := NavigateBlob(blob, []byte{}) // empty raw key → hex [term]
	if err != nil {
		t.Fatalf("Navigate(empty): %v", err)
	}
	if !bytes.Equal(got, []byte("VALUE")) {
		t.Errorf("got %x, want %x", got, []byte("VALUE"))
	}
}

// TestNavigateNestedFullNodes: two-level fullNode hierarchy.
func TestNavigateNestedFullNodes(t *testing.T) {
	// rawKey 0x12 → hex [1,2,term].
	// rootFull → child[1] = innerFull → child[2] = leafShortNode([term])
	b := &blobBuilder{}

	leaf := buildLeafShortNode([]byte{HexTerminator}, []byte("VALUE12"))
	offL, szL := b.emit(leaf)

	var inner [17]slotSpec
	inner[2] = slotSpec{kind: SlotKindHashedRef, hash: dummyHash(0x12), offset: offL, size: szL}
	innerBytes := buildFullNode(inner)
	offInner, szInner := b.emit(innerBytes)

	var root [17]slotSpec
	root[1] = slotSpec{kind: SlotKindHashedRef, hash: dummyHash(0x01), offset: offInner, size: szInner}
	rootBytes := buildFullNode(root)
	rootOff, rootSize := b.emit(rootBytes)
	blob := b.finalize(rootOff, rootSize)

	got, err := NavigateBlob(blob, []byte{0x12})
	if err != nil {
		t.Fatalf("Navigate(0x12): %v", err)
	}
	if !bytes.Equal(got, []byte("VALUE12")) {
		t.Errorf("got %x, want %x", got, []byte("VALUE12"))
	}
}

// TestNavigateEmbeddedRef: navigator follows embedded-ref slots identically
// to hashed-ref slots (the original-vs-embedded distinction matters only for
// the lazy materialiser, not for reads).
func TestNavigateEmbeddedRef(t *testing.T) {
	b := &blobBuilder{}

	leaf := buildLeafShortNode([]byte{2, HexTerminator}, []byte("EMBED"))
	offL, szL := b.emit(leaf)

	var slots [17]slotSpec
	slots[1] = slotSpec{kind: SlotKindEmbeddedRef, offset: offL, size: szL}
	rootBytes := buildFullNode(slots)
	rootOff, rootSize := b.emit(rootBytes)
	blob := b.finalize(rootOff, rootSize)

	got, err := NavigateBlob(blob, []byte{0x12})
	if err != nil {
		t.Fatalf("Navigate(0x12): %v", err)
	}
	if !bytes.Equal(got, []byte("EMBED")) {
		t.Errorf("got %x, want %x", got, []byte("EMBED"))
	}
}

// TestParseHeaderRejectsBadVersion: header with wrong version returns error.
func TestParseHeaderRejectsBadVersion(t *testing.T) {
	blob := make([]byte, HeaderSize+10)
	binary.BigEndian.PutUint16(blob[0:2], 99) // bad version
	binary.BigEndian.PutUint32(blob[8:12], HeaderSize)
	binary.BigEndian.PutUint32(blob[12:16], 1)
	if _, err := ParseHeader(blob); err == nil {
		t.Errorf("ParseHeader: want error for bad version, got nil")
	}
}

// TestParseHeaderRejectsRangeOverflow: header with root range past blob end.
func TestParseHeaderRejectsRangeOverflow(t *testing.T) {
	blob := make([]byte, HeaderSize+5)
	binary.BigEndian.PutUint16(blob[0:2], CurrentVersion)
	binary.BigEndian.PutUint32(blob[8:12], HeaderSize)
	binary.BigEndian.PutUint32(blob[12:16], 1000) // way too large
	if _, err := ParseHeader(blob); err == nil {
		t.Errorf("ParseHeader: want error for overflow, got nil")
	}
}

// TestParseNodeUnknownTag: parsing a node with an unknown tag fails cleanly.
func TestParseNodeUnknownTag(t *testing.T) {
	blob := make([]byte, HeaderSize+1)
	binary.BigEndian.PutUint16(blob[0:2], CurrentVersion)
	binary.BigEndian.PutUint32(blob[8:12], HeaderSize)
	binary.BigEndian.PutUint32(blob[12:16], 1)
	blob[HeaderSize] = 0xFE // unknown tag

	if _, err := ParseNode(blob, HeaderSize, 1); err == nil {
		t.Errorf("ParseNode: want error for unknown tag, got nil")
	}
}

// TestSlotKindEmptyRoundTrip exercises the empty-child slot path.
func TestSlotKindEmptyRoundTrip(t *testing.T) {
	var b bytes.Buffer
	putEmptySlot(&b)
	slot, n, err := readChildSlot(b.Bytes(), 0)
	if err != nil {
		t.Fatalf("readChildSlot: %v", err)
	}
	if n != 1 {
		t.Errorf("consumed %d bytes, want 1", n)
	}
	if slot.Kind != SlotKindEmpty {
		t.Errorf("kind = %d, want empty", slot.Kind)
	}
}

// TestSlotKindHashedRefRoundTrip exercises the hashed-ref slot path: encode →
// parse → check that hash, offset, and size are preserved.
func TestSlotKindHashedRefRoundTrip(t *testing.T) {
	var b bytes.Buffer
	hash := dummyHash(0xab)
	putHashedRefSlot(&b, hash, 0xdeadbeef, 0xcafe)
	slot, n, err := readChildSlot(b.Bytes(), 0)
	if err != nil {
		t.Fatalf("readChildSlot: %v", err)
	}
	if n != 1+HashLen+6 {
		t.Errorf("consumed %d bytes, want %d", n, 1+HashLen+6)
	}
	if slot.Kind != SlotKindHashedRef {
		t.Errorf("kind = %d, want hashed-ref", slot.Kind)
	}
	if !bytes.Equal(slot.Hash, hash) {
		t.Errorf("hash = %x, want %x", slot.Hash, hash)
	}
	if slot.Offset != 0xdeadbeef {
		t.Errorf("offset = %x, want 0xdeadbeef", slot.Offset)
	}
	if slot.Size != 0xcafe {
		t.Errorf("size = %x, want 0xcafe", slot.Size)
	}
}

// TestSlotKindEmbeddedRefRoundTrip exercises the embedded-ref slot path.
func TestSlotKindEmbeddedRefRoundTrip(t *testing.T) {
	var b bytes.Buffer
	putEmbeddedRefSlot(&b, 0x11223344, 0x5566)
	slot, n, err := readChildSlot(b.Bytes(), 0)
	if err != nil {
		t.Fatalf("readChildSlot: %v", err)
	}
	if n != 7 {
		t.Errorf("consumed %d bytes, want 7", n)
	}
	if slot.Kind != SlotKindEmbeddedRef {
		t.Errorf("kind = %d, want embedded-ref", slot.Kind)
	}
	if slot.Offset != 0x11223344 {
		t.Errorf("offset = %x, want 0x11223344", slot.Offset)
	}
	if slot.Size != 0x5566 {
		t.Errorf("size = %x, want 0x5566", slot.Size)
	}
}

// TestIsStubAndIsHybrid checks the three-way classification of a chaindb
// blob into primary stub / hybrid node / standard RLP.
func TestIsStubAndIsHybrid(t *testing.T) {
	cases := []struct {
		name         string
		blob         []byte
		stub, hybrid bool
		stubOrHybrid bool
	}{
		{"empty", []byte{}, false, false, false},
		{"primary stub", []byte{0x00, 1, 2, 3}, true, false, true},
		{"hybrid", []byte{0x01, 1, 2, 3}, false, true, true},
		{"standard RLP", []byte{0xc0, 1, 2}, false, false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := IsStub(c.blob); got != c.stub {
				t.Errorf("IsStub = %v, want %v", got, c.stub)
			}
			if got := IsHybrid(c.blob); got != c.hybrid {
				t.Errorf("IsHybrid = %v, want %v", got, c.hybrid)
			}
			if got := IsStubOrHybrid(c.blob); got != c.stubOrHybrid {
				t.Errorf("IsStubOrHybrid = %v, want %v", got, c.stubOrHybrid)
			}
		})
	}
}

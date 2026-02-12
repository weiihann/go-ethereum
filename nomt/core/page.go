package core

import "encoding/binary"

// Page layout constants.
const (
	// PageDepth is the depth of the rootless sub-binary tree stored in a page.
	PageDepth = 6

	// NodesPerPage is the total number of nodes in one page: (2^(depth+1)) - 2 = 126.
	NodesPerPage = (1 << (PageDepth + 1)) - 2

	// NumChildren is the number of child pages each page can have: 2^depth = 64.
	NumChildren = 1 << PageDepth

	// PageSize is the size of a raw page in bytes, aligned to SSD page size.
	PageSize = 4096

	// elidedChildrenOffset stores the 8-byte elided children bitfield.
	// Layout: [nodes 4032] [padding 24] [elided 8] [pageID 32] = 4096
	elidedChildrenOffset = PageSize - 32 - 8 // 4056

	// pageIDOffset stores the 32-byte encoded PageID.
	pageIDOffset = PageSize - 32 // 4064
)

// RawPage is a 4096-byte page storing a rootless sub-tree of depth 6.
//
// Layout:
//
//	[0..4032)     126 nodes × 32 bytes each, in level-order
//	[4032..4056)  24 bytes padding
//	[4056..4064)  ElidedChildren bitfield (8 bytes, little-endian uint64)
//	[4064..4096)  PageID encoded (32 bytes)
type RawPage [PageSize]byte

// NodeAt reads the 32-byte node at the given index (0-based level-order).
func (p *RawPage) NodeAt(index int) Node {
	var n Node
	off := index * 32
	copy(n[:], p[off:off+32])
	return n
}

// SetNodeAt writes a 32-byte node at the given index.
func (p *RawPage) SetNodeAt(index int, n Node) {
	off := index * 32
	copy(p[off:off+32], n[:])
}

// ElidedChildren reads the 8-byte elided children bitfield.
func (p *RawPage) ElidedChildren() uint64 {
	return binary.LittleEndian.Uint64(p[elidedChildrenOffset:])
}

// SetElidedChildren writes the 8-byte elided children bitfield.
func (p *RawPage) SetElidedChildren(ec uint64) {
	binary.LittleEndian.PutUint64(p[elidedChildrenOffset:], ec)
}

// PageIDBytes reads the 32-byte encoded PageID from the page.
func (p *RawPage) PageIDBytes() [32]byte {
	var id [32]byte
	copy(id[:], p[pageIDOffset:pageIDOffset+32])
	return id
}

// SetPageIDBytes writes the 32-byte encoded PageID into the page.
func (p *RawPage) SetPageIDBytes(id [32]byte) {
	copy(p[pageIDOffset:pageIDOffset+32], id[:])
}

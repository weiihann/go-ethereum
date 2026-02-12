package core

import (
	"encoding/binary"
	"math/bits"
)

// PageDiff tracks which nodes in a page have changed, using a 128-bit
// bitfield stored as two uint64 words.
//
// Bit 63 of the second word is the "cleared" flag, indicating the page
// was cleared entirely. Bit 62 of the second word is reserved.
type PageDiff struct {
	words [2]uint64
}

const clearBit = uint64(1) << 63

// SetChanged marks the node at the given index (0-125) as changed.
// Also clears the "cleared" flag if set.
func (d *PageDiff) SetChanged(index int) {
	// Always clear the "cleared" flag when setting a changed node.
	d.words[1] &= ^clearBit
	if index < 64 {
		d.words[0] |= 1 << index
	} else {
		d.words[1] |= 1 << (index - 64)
	}
}

// IsChanged reports whether the node at the given index is marked changed.
func (d *PageDiff) IsChanged(index int) bool {
	if index < 64 {
		return d.words[0]&(1<<index) != 0
	}
	return d.words[1]&(1<<(index-64)) != 0
}

// SetCleared marks the page as cleared (deleted).
func (d *PageDiff) SetCleared() {
	d.words[1] |= clearBit
}

// IsCleared reports whether the page is marked as cleared.
func (d *PageDiff) IsCleared() bool {
	return d.words[1]&clearBit != 0
}

// Join combines two PageDiffs by ORing their bitfields.
func (d PageDiff) Join(other PageDiff) PageDiff {
	return PageDiff{
		words: [2]uint64{
			d.words[0] | other.words[0],
			d.words[1] | other.words[1],
		},
	}
}

// Count returns the number of changed nodes (popcount).
func (d *PageDiff) Count() int {
	return bits.OnesCount64(d.words[0]) + bits.OnesCount64(d.words[1])
}

// ChangedIndices returns the indices of all set (changed) bits.
func (d *PageDiff) ChangedIndices() []int {
	d.assertNotCleared()
	indices := make([]int, 0, d.Count())
	w0 := d.words[0]
	for w0 != 0 {
		i := bits.TrailingZeros64(w0)
		indices = append(indices, i)
		w0 &= ^(1 << i)
	}
	w1 := d.words[1]
	for w1 != 0 {
		i := bits.TrailingZeros64(w1)
		indices = append(indices, i+64)
		w1 &= ^(1 << i)
	}
	return indices
}

// PackChangedNodes extracts the changed nodes from a page in diff order.
func (d *PageDiff) PackChangedNodes(page *RawPage) []Node {
	d.assertNotCleared()
	indices := d.ChangedIndices()
	nodes := make([]Node, len(indices))
	for i, idx := range indices {
		nodes[i] = page.NodeAt(idx)
	}
	return nodes
}

// UnpackChangedNodes applies the changed nodes to a page according to the diff.
func (d *PageDiff) UnpackChangedNodes(nodes []Node, page *RawPage) {
	indices := d.ChangedIndices()
	if len(nodes) != len(indices) {
		panic("pagediff: node count mismatch")
	}
	for i, idx := range indices {
		page.SetNodeAt(idx, nodes[i])
	}
}

// Encode serializes the PageDiff to 16 bytes (two uint64, little-endian).
func (d PageDiff) Encode() [16]byte {
	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[0:8], d.words[0])
	binary.LittleEndian.PutUint64(buf[8:16], d.words[1])
	return buf
}

// DecodePageDiff deserializes a PageDiff from 16 bytes.
func DecodePageDiff(buf [16]byte) PageDiff {
	return PageDiff{
		words: [2]uint64{
			binary.LittleEndian.Uint64(buf[0:8]),
			binary.LittleEndian.Uint64(buf[8:16]),
		},
	}
}

func (d *PageDiff) assertNotCleared() {
	if d.IsCleared() {
		panic("pagediff: operation not valid on cleared diff")
	}
}

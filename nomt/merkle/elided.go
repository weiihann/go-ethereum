// Package merkle implements the in-memory batch update engine for the NOMT
// binary merkle trie. It processes sorted key-value changes and produces
// updated pages plus a new root hash.
package merkle

import "encoding/binary"

// ElidedChildren is a 64-bit bitfield tracking which of a page's 64 child
// pages are elided (not stored on disk and reconstructed on-the-fly).
type ElidedChildren struct {
	elided uint64
}

// NewElidedChildren returns an empty ElidedChildren with no children elided.
func NewElidedChildren() ElidedChildren {
	return ElidedChildren{}
}

// ElidedChildrenFromBytes decodes an ElidedChildren from its 8-byte
// little-endian representation.
func ElidedChildrenFromBytes(raw [8]byte) ElidedChildren {
	return ElidedChildren{elided: binary.LittleEndian.Uint64(raw[:])}
}

// ElidedChildrenFromUint64 wraps a raw uint64 bitfield.
func ElidedChildrenFromUint64(v uint64) ElidedChildren {
	return ElidedChildren{elided: v}
}

// ToBytes encodes the ElidedChildren as 8 bytes (little-endian).
func (e *ElidedChildren) ToBytes() [8]byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], e.elided)
	return buf
}

// SetElide marks or clears the elided flag for the given child index (0-63).
func (e *ElidedChildren) SetElide(childIndex uint8, elide bool) {
	if elide {
		e.elided |= 1 << childIndex
	} else {
		e.elided &^= 1 << childIndex
	}
}

// IsElided reports whether the child at the given index is elided.
func (e *ElidedChildren) IsElided(childIndex uint8) bool {
	return (e.elided>>childIndex)&1 == 1
}

// Raw returns the underlying uint64 bitfield.
func (e *ElidedChildren) Raw() uint64 {
	return e.elided
}

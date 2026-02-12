package core

import (
	"errors"
	"math/big"
)

// MaxPageDepth is the maximum depth of the page tree (6 bits × 42 = 252 ≈ 256).
const MaxPageDepth = 42

// MaxChildIndex is the maximum child index value (63 = 2^6 - 1).
const MaxChildIndex = NumChildren - 1

var (
	// ErrInvalidPageIDBytes indicates the bytes cannot form a valid PageID.
	ErrInvalidPageIDBytes = errors.New("invalid page ID bytes")

	// ErrPageIDOverflow indicates the PageID is at the maximum depth.
	ErrPageIDOverflow = errors.New("page ID overflow: at maximum depth")

	// highestEncoded42 is the encoded representation of the highest valid
	// page ID at layer 42.
	highestEncoded42 *big.Int

	bigOne = big.NewInt(1)
	big63  = big.NewInt(63)
)

func init() {
	// Compute the encoded value of the highest valid page ID: path=[63]*42.
	// Using shift-then-add: for each limb, shift left 6 then add (63+1).
	highestEncoded42 = new(big.Int)
	for range MaxPageDepth {
		highestEncoded42.Lsh(highestEncoded42, 6)
		highestEncoded42.Add(highestEncoded42, big.NewInt(64))
	}
}

// PageID identifies a page in the page tree. It is a sequence of child
// indices (each 0-63) representing the path from the root page.
type PageID struct {
	path []uint8
}

// RootPageID returns the root page ID (empty path, depth 0).
func RootPageID() PageID {
	return PageID{}
}

// NewPageID creates a PageID from a path slice. Each element must be 0-63.
func NewPageID(path []uint8) PageID {
	p := make([]uint8, len(path))
	copy(p, path)
	return PageID{path: p}
}

// Depth returns the depth of this page in the page tree. Root is 0.
func (id PageID) Depth() int {
	return len(id.path)
}

// IsRoot reports whether this is the root page.
func (id PageID) IsRoot() bool {
	return len(id.path) == 0
}

// ChildIndexAt returns the child index at the given depth level.
func (id PageID) ChildIndexAt(level int) uint8 {
	return id.path[level]
}

// Path returns a copy of the path slice.
func (id PageID) Path() []uint8 {
	p := make([]uint8, len(id.path))
	copy(p, id.path)
	return p
}

// Encode produces the 256-bit disambiguated representation of the PageID.
//
// The encoding repeatedly shifts left by 6 bits, adds (childIndex + 1),
// then shifts left by 6 more. This produces a unique, ordered fixed-width
// representation.
func (id PageID) Encode() [32]byte {
	if len(id.path) < 10 {
		// Fast path: fits in u64 (6×10 = 60 bits max).
		// Uses shift-then-add order (NOT Rust's add-then-shift which has
		// a trailing shift bug). See memory/pageid-encoding.md.
		var word uint64
		for _, limb := range id.path {
			word <<= 6
			word += uint64(limb) + 1
		}
		var buf [32]byte
		buf[24] = byte(word >> 56)
		buf[25] = byte(word >> 48)
		buf[26] = byte(word >> 40)
		buf[27] = byte(word >> 32)
		buf[28] = byte(word >> 24)
		buf[29] = byte(word >> 16)
		buf[30] = byte(word >> 8)
		buf[31] = byte(word)
		return buf
	}

	// Slow path: use big.Int for deep pages.
	// Same shift-then-add order as fast path.
	val := new(big.Int)
	for _, limb := range id.path {
		val.Lsh(val, 6)
		val.Add(val, big.NewInt(int64(limb)+1))
	}

	var buf [32]byte
	b := val.Bytes()
	copy(buf[32-len(b):], b)
	return buf
}

// DecodePageID decodes a PageID from its 256-bit representation.
func DecodePageID(bytes [32]byte) (PageID, error) {
	val := new(big.Int).SetBytes(bytes[:])

	if val.Cmp(highestEncoded42) > 0 {
		return PageID{}, ErrInvalidPageIDBytes
	}

	if val.Sign() == 0 {
		return RootPageID(), nil
	}

	bitLen := val.BitLen()
	sextets := (bitLen + 5) / 6

	path := make([]uint8, 0, sextets)
	for i := 0; i < sextets-1; i++ {
		val.Sub(val, bigOne)
		x := new(big.Int).And(val, big63)
		path = append(path, uint8(x.Uint64()))
		val.Rsh(val, 6)
	}
	// Last sextet: only push if non-zero after subtracting 1.
	if val.Sign() != 0 {
		val.Sub(val, bigOne)
		path = append(path, uint8(val.Uint64()))
	}

	// Reverse to get most-significant first.
	for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
		path[i], path[j] = path[j], path[i]
	}

	return PageID{path: path}, nil
}

// ChildPageID returns the child PageID at the given child index (0-63).
func (id PageID) ChildPageID(childIndex uint8) (PageID, error) {
	if childIndex > MaxChildIndex {
		return PageID{}, ErrPageIDOverflow
	}
	if len(id.path) >= MaxPageDepth {
		return PageID{}, ErrPageIDOverflow
	}
	p := make([]uint8, len(id.path)+1)
	copy(p, id.path)
	p[len(id.path)] = childIndex
	return PageID{path: p}, nil
}

// ParentPageID returns the parent PageID. If this is the root, returns root.
func (id PageID) ParentPageID() PageID {
	if len(id.path) == 0 {
		return RootPageID()
	}
	p := make([]uint8, len(id.path)-1)
	copy(p, id.path[:len(id.path)-1])
	return PageID{path: p}
}

// IsDescendantOf reports whether this page is a descendant of other.
func (id PageID) IsDescendantOf(other PageID) bool {
	if len(id.path) < len(other.path) {
		return false
	}
	for i := range other.path {
		if id.path[i] != other.path[i] {
			return false
		}
	}
	return true
}

// Equal reports whether two PageIDs are the same.
func (id PageID) Equal(other PageID) bool {
	if len(id.path) != len(other.path) {
		return false
	}
	for i := range id.path {
		if id.path[i] != other.path[i] {
			return false
		}
	}
	return true
}

// MinKeyPath returns the minimum key path that could land in this page.
func (id PageID) MinKeyPath() KeyPath {
	var path KeyPath
	for i, childIndex := range id.path {
		setBitsInKeyPath(&path, i*6, childIndex)
	}
	// Remaining bits are already zero.
	return path
}

// MaxKeyPath returns the maximum key path that could land in this page.
func (id PageID) MaxKeyPath() KeyPath {
	var path KeyPath
	// Fill all with 1s first.
	for i := range path {
		path[i] = 0xFF
	}
	// Set the prefix bits from the page path.
	for i, childIndex := range id.path {
		setBitsInKeyPath(&path, i*6, childIndex)
	}
	return path
}

// setBitsInKeyPath writes a 6-bit child index into the key path at the given
// bit offset.
func setBitsInKeyPath(path *KeyPath, bitOffset int, childIndex uint8) {
	for b := 0; b < 6; b++ {
		bit := (childIndex >> (5 - b)) & 1
		byteIdx := (bitOffset + b) / 8
		bitIdx := 7 - ((bitOffset + b) % 8)
		if bit == 1 {
			path[byteIdx] |= 1 << bitIdx
		} else {
			path[byteIdx] &^= 1 << bitIdx
		}
	}
}

// PageIDsForKeyPath returns the sequence of PageIDs from root down to the
// deepest page containing the given key path.
func PageIDsForKeyPath(keyPath KeyPath) []PageID {
	ids := make([]PageID, 0, MaxPageDepth+1)
	current := RootPageID()
	ids = append(ids, current)

	for depth := 0; depth < MaxPageDepth; depth++ {
		bitStart := depth * 6
		childIndex := extractChildIndex(keyPath, bitStart)
		child, err := current.ChildPageID(childIndex)
		if err != nil {
			break
		}
		ids = append(ids, child)
		current = child
	}
	return ids
}

// extractChildIndex extracts a 6-bit child index from the key path at the
// given bit offset.
func extractChildIndex(keyPath KeyPath, bitOffset int) uint8 {
	var idx uint8
	for b := 0; b < 6; b++ {
		byteIdx := (bitOffset + b) / 8
		bitIdx := 7 - ((bitOffset + b) % 8)
		bit := (keyPath[byteIdx] >> bitIdx) & 1
		idx = (idx << 1) | bit
	}
	return idx
}

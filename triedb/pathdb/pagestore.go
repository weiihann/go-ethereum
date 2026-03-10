// Copyright 2025 The go-ethereum Authors
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

package pathdb

import (
	"encoding/binary"
	"fmt"
	"math/bits"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
)

const (
	PageDepth   = 6  // Trie levels packed per page.
	PageSlots   = 63 // 2^6 - 1 internal nodes per page.
	NodeSize    = 65 // Bytes per serialized InternalNode.
	PageVersion = 1  // Current page format version.

	nodeTypeInternal = 2 // Matches bintrie nodeTypeInternal constant.
)

// Page packs up to 63 InternalNodes (a depth-6 binary subtree)
// into a single database value, reducing disk reads by ~6x.
type Page struct {
	version byte
	bitmap  uint64            // 63 bits indicating populated slots.
	nodes   [PageSlots][]byte // Each slot holds a 65-byte blob or nil.
}

// NewPage creates an empty page with the current version.
func NewPage() *Page {
	return &Page{version: PageVersion}
}

// SetNode stores a node blob at the given slot index.
func (p *Page) SetNode(slot int, blob []byte) {
	if slot < 0 || slot >= PageSlots {
		return
	}
	p.nodes[slot] = blob
	p.bitmap |= 1 << uint(slot)
}

// GetNode returns the node blob at the given slot, or nil if empty.
func (p *Page) GetNode(slot int) []byte {
	if slot < 0 || slot >= PageSlots {
		return nil
	}
	if p.bitmap&(1<<uint(slot)) == 0 {
		return nil
	}
	return p.nodes[slot]
}

// DeleteNode clears the slot at the given index.
func (p *Page) DeleteNode(slot int) {
	if slot < 0 || slot >= PageSlots {
		return
	}
	p.nodes[slot] = nil
	p.bitmap &^= 1 << uint(slot)
}

// IsEmpty returns true when no slots are populated.
func (p *Page) IsEmpty() bool {
	return p.bitmap == 0
}

// PopCount returns the number of populated slots.
func (p *Page) PopCount() int {
	return popcount64(p.bitmap)
}

// Serialize encodes the page in compact format:
// [1B version][8B bitmap LE][N * 65B nodes in bitmap order]
func (p *Page) Serialize() []byte {
	count := p.PopCount()
	buf := make([]byte, 1+8+count*NodeSize)

	buf[0] = p.version
	binary.LittleEndian.PutUint64(buf[1:9], p.bitmap)

	off := 9
	for i := 0; i < PageSlots; i++ {
		if p.bitmap&(1<<uint(i)) == 0 {
			continue
		}
		copy(buf[off:off+NodeSize], p.nodes[i])
		off += NodeSize
	}
	return buf
}

// DeserializePage decodes a page from its compact encoding.
func DeserializePage(data []byte) (*Page, error) {
	if len(data) < 9 {
		return nil, fmt.Errorf(
			"page data too short: need at least 9 bytes, got %d",
			len(data),
		)
	}
	p := &Page{version: data[0]}
	p.bitmap = binary.LittleEndian.Uint64(data[1:9])

	count := popcount64(p.bitmap)
	expected := 9 + count*NodeSize
	if len(data) != expected {
		return nil, fmt.Errorf(
			"page data length mismatch: expected %d, got %d",
			expected, len(data),
		)
	}
	// Allocate a single backing buffer for all node blobs and slice into it,
	// reducing allocations from O(count) to O(1).
	backing := make([]byte, count*NodeSize)
	copy(backing, data[9:])
	idx := 0
	for i := 0; i < PageSlots; i++ {
		if p.bitmap&(1<<uint(i)) == 0 {
			continue
		}
		p.nodes[i] = backing[idx*NodeSize : (idx+1)*NodeSize]
		idx++
	}
	return p, nil
}

// PathToPageKey builds the PebbleDB key for the page containing
// the node at the given bit-path.
//
//	Account pages: "P" + bitpack(path[:pageLevel*PageDepth])
//	Storage pages: "Q" + owner(32B) + bitpack(path[:pageLevel*PageDepth])
func PathToPageKey(owner common.Hash, path []byte) []byte {
	pageLevel := len(path) / PageDepth
	prefixLen := pageLevel * PageDepth
	packed := BitPack(path[:prefixLen])

	if owner == (common.Hash{}) {
		key := make([]byte, 1+len(packed))
		key[0] = rawdb.TriePageAccountPrefix[0]
		copy(key[1:], packed)
		return key
	}
	key := make([]byte, 1+common.HashLength+len(packed))
	key[0] = rawdb.TriePageStoragePrefix[0]
	copy(key[1:1+common.HashLength], owner.Bytes())
	copy(key[1+common.HashLength:], packed)
	return key
}

// PathToSlot computes the binary-heap slot index for a node within
// its containing page.
//
// For a node whose local path within the page is [b0, b1, ..., bd-1]
// (d = len(path) mod PageDepth), the slot is:
//
//	slot = (2^d - 1) + bitsToInt(b0..bd-1)
//
// Slot 0 is the root of the depth-6 subtree. The valid range is [0, 62].
func PathToSlot(path []byte) int {
	pageLevel := len(path) / PageDepth
	localStart := pageLevel * PageDepth
	localBits := path[localStart:]

	d := len(localBits)
	if d == 0 {
		return 0
	}
	offset := (1 << uint(d)) - 1
	val := 0
	for i := 0; i < d; i++ {
		if localBits[i] != 0 {
			val |= 1 << uint(d-1-i)
		}
	}
	return offset + val
}

// SlotToLocalPath converts a binary-heap slot index back to the
// local bit-path within the page.
func SlotToLocalPath(slot int) []byte {
	if slot == 0 {
		return nil
	}
	d := 0
	s := slot
	for s > 0 {
		d++
		s = (s - 1) / 2
	}
	offset := (1 << uint(d)) - 1
	val := slot - offset

	bits := make([]byte, d)
	for i := d - 1; i >= 0; i-- {
		bits[i] = byte(val & 1)
		val >>= 1
	}
	return bits
}

// BitPack packs a one-byte-per-bit path into a compact bitstring
// (8 bits per byte). The output length is ceil(len(path)/8).
func BitPack(path []byte) []byte {
	if len(path) == 0 {
		return nil
	}
	out := make([]byte, (len(path)+7)/8)
	for i, b := range path {
		if b != 0 {
			out[i/8] |= 1 << uint(7-i%8)
		}
	}
	return out
}

// BitUnpack expands a packed bitstring back to one-byte-per-bit
// form with the given number of bits.
func BitUnpack(packed []byte, nbits int) []byte {
	out := make([]byte, nbits)
	for i := 0; i < nbits; i++ {
		if packed[i/8]&(1<<uint(7-i%8)) != 0 {
			out[i] = 1
		}
	}
	return out
}

// isInternalNode returns true if the blob represents a binary trie
// InternalNode (type byte == 2).
func isInternalNode(blob []byte) bool {
	return len(blob) > 0 && blob[0] == nodeTypeInternal
}

// ReadNodeFromPage reads the page containing the requested node,
// populates the clean cache with all sibling nodes, and returns
// the requested node blob.
//
// Falls back to individual node read when no page exists.
func ReadNodeFromPage(
	db ethdb.KeyValueReader,
	owner common.Hash,
	path []byte,
	clean *fastcache.Cache,
) ([]byte, error) {
	pageKey := PathToPageKey(owner, path)

	var pageBlob []byte
	if owner == (common.Hash{}) {
		pageBlob = rawdb.ReadAccountTriePage(db, pageKey[1:])
	} else {
		pageBlob = rawdb.ReadStorageTriePage(
			db, owner, pageKey[1+common.HashLength:],
		)
	}
	if pageBlob == nil {
		if owner == (common.Hash{}) {
			return rawdb.ReadAccountTrieNode(db, path), nil
		}
		return rawdb.ReadStorageTrieNode(db, owner, path), nil
	}

	page, err := DeserializePage(pageBlob)
	if err != nil {
		return nil, err
	}

	if clean != nil {
		pageLevel := len(path) / PageDepth
		prefixLen := pageLevel * PageDepth
		pagePrefix := path[:prefixLen]
		populateCleanCacheFromPage(page, owner, pagePrefix, clean)
	}

	slot := PathToSlot(path)
	return page.GetNode(slot), nil
}

// slotLocalPaths is a pre-computed table of local bit-paths for each
// of the 63 page slots. Avoids per-call allocation in SlotToLocalPath.
var slotLocalPaths [PageSlots][]byte

func init() {
	for i := 0; i < PageSlots; i++ {
		slotLocalPaths[i] = SlotToLocalPath(i)
	}
}

// populateCleanCacheFromPage inserts every populated node from the
// page into the clean cache, using a single reusable path buffer.
func populateCleanCacheFromPage(
	page *Page,
	owner common.Hash,
	pagePrefix []byte,
	clean *fastcache.Cache,
) {
	prefixLen := len(pagePrefix)
	// Max path length: prefix + PageDepth (6) bits.
	var pathBuf [256]byte
	copy(pathBuf[:prefixLen], pagePrefix)

	// Only iterate set bits via the bitmap.
	bm := page.bitmap
	for bm != 0 {
		slot := bits.TrailingZeros64(bm)
		bm &= bm - 1 // clear lowest set bit
		local := slotLocalPaths[slot]
		pathLen := prefixLen + len(local)
		copy(pathBuf[prefixLen:], local)
		nodePath := pathBuf[:pathLen]

		cacheKey := nodeCacheKey(owner, nodePath)
		clean.Set(cacheKey, page.nodes[slot])
	}
}

// slotToPath reconstructs the full node path from the page prefix
// and a slot index.
func slotToPath(pagePrefix []byte, slot int) []byte {
	local := slotLocalPaths[slot]
	full := make([]byte, len(pagePrefix)+len(local))
	copy(full, pagePrefix)
	copy(full[len(pagePrefix):], local)
	return full
}

// readOrCreatePage loads an existing page from disk for the given
// node path, or creates a new empty page.
func readOrCreatePage(
	db ethdb.KeyValueReader,
	owner common.Hash,
	path []byte,
) *Page {
	pageKey := PathToPageKey(owner, path)

	var pageBlob []byte
	if owner == (common.Hash{}) {
		pageBlob = rawdb.ReadAccountTriePage(db, pageKey[1:])
	} else {
		pageBlob = rawdb.ReadStorageTriePage(
			db, owner, pageKey[1+common.HashLength:],
		)
	}
	if pageBlob != nil {
		page, err := DeserializePage(pageBlob)
		if err == nil {
			return page
		}
	}
	return NewPage()
}

// writeTriePage writes a serialized page to the batch.
func writeTriePage(
	batch ethdb.KeyValueWriter,
	owner common.Hash,
	pageKey string,
	data []byte,
) {
	keyBytes := []byte(pageKey)
	if owner == (common.Hash{}) {
		rawdb.WriteAccountTriePage(batch, keyBytes[1:], data)
	} else {
		rawdb.WriteStorageTriePage(
			batch, owner, keyBytes[1+common.HashLength:], data,
		)
	}
}

// deleteTriePage removes a page from the batch.
func deleteTriePage(
	batch ethdb.KeyValueWriter,
	owner common.Hash,
	pageKey string,
) {
	keyBytes := []byte(pageKey)
	if owner == (common.Hash{}) {
		rawdb.DeleteAccountTriePage(batch, keyBytes[1:])
	} else {
		rawdb.DeleteStorageTriePage(
			batch, owner, keyBytes[1+common.HashLength:],
		)
	}
}

// pageKeyPrefix extracts the page prefix (original bit-path) from
// a page key string, for use when populating the clean cache.
func pageKeyPrefix(
	owner common.Hash,
	pageKey string,
) []byte {
	keyBytes := []byte(pageKey)
	var packed []byte
	if owner == (common.Hash{}) {
		packed = keyBytes[1:]
	} else {
		packed = keyBytes[1+common.HashLength:]
	}
	return BitUnpack(packed, len(packed)*8)
}

// popcount64 counts set bits in a uint64.
func popcount64(x uint64) int {
	x = x - ((x >> 1) & 0x5555555555555555)
	x = (x & 0x3333333333333333) + ((x >> 2) & 0x3333333333333333)
	x = (x + (x >> 4)) & 0x0f0f0f0f0f0f0f0f
	return int((x * 0x0101010101010101) >> 56)
}

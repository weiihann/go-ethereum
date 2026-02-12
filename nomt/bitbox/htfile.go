// Package bitbox implements an on-disk open-addressing hash table that maps
// PageIDs to 4096-byte pages. It is the storage backend for the NOMT trie.
package bitbox

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/ethereum/go-ethereum/nomt/core"
)

const (
	// pageSize is the size of a disk page.
	pageSize = core.PageSize // 4096

	// metaBytesPerPage is the number of meta bytes that fit in one page.
	metaBytesPerPage = pageSize

	// headerSize is the size of the HT file header in bytes.
	// Layout: [seed 16] [capacity 8] [occupied 8] = 32 bytes, padded to
	// one full page.
	headerSize = pageSize

	// seedOffset is the offset of the 16-byte seed in the header.
	seedOffset = 0
	// capacityOffset is the offset of the 8-byte capacity in the header.
	capacityOffset = 16
	// occupiedOffset is the offset of the 8-byte occupied count.
	occupiedOffset = 24
)

// HTOffsets holds precomputed file offsets for the hash table file layout.
//
// File layout:
//
//	[header: 1 page] [meta pages: ceil(capacity/4096)] [data pages: capacity * 4096]
type HTOffsets struct {
	// Capacity is the number of buckets in the hash table.
	Capacity uint64
	// MetaPages is ceil(Capacity / 4096).
	MetaPages uint64
}

// NewHTOffsets creates an HTOffsets for the given capacity.
func NewHTOffsets(capacity uint64) HTOffsets {
	return HTOffsets{
		Capacity:  capacity,
		MetaPages: (capacity + metaBytesPerPage - 1) / metaBytesPerPage,
	}
}

// MetaByteOffset returns the file offset for the meta byte of a given bucket.
func (o *HTOffsets) MetaByteOffset(bucket uint64) int64 {
	return int64(headerSize) + int64(bucket)
}

// DataPageOffset returns the file offset for the data page of a given bucket.
func (o *HTOffsets) DataPageOffset(bucket uint64) int64 {
	dataStart := int64(headerSize) + int64(o.MetaPages)*pageSize
	return dataStart + int64(bucket)*pageSize
}

// TotalFileSize returns the total size of the HT file in bytes.
func (o *HTOffsets) TotalFileSize() int64 {
	return int64(headerSize) + int64(o.MetaPages)*pageSize +
		int64(o.Capacity)*pageSize
}

// CreateHTFile creates a new hash table file with the given capacity and seed.
// The file is pre-allocated to its full size.
func CreateHTFile(path string, capacity uint64, seed [16]byte) (
	*os.File, HTOffsets, error,
) {
	offsets := NewHTOffsets(capacity)

	f, err := os.Create(path)
	if err != nil {
		return nil, offsets, fmt.Errorf("bitbox: create HT file: %w", err)
	}

	// Pre-allocate.
	totalSize := offsets.TotalFileSize()
	if err := f.Truncate(totalSize); err != nil {
		f.Close()
		return nil, offsets, fmt.Errorf("bitbox: truncate HT file: %w", err)
	}

	// Write header.
	var header [headerSize]byte
	copy(header[seedOffset:], seed[:])
	binary.LittleEndian.PutUint64(header[capacityOffset:], capacity)
	binary.LittleEndian.PutUint64(header[occupiedOffset:], 0)

	if _, err := f.WriteAt(header[:], 0); err != nil {
		f.Close()
		return nil, offsets, fmt.Errorf("bitbox: write header: %w", err)
	}

	return f, offsets, nil
}

// OpenHTFile opens an existing hash table file and reads its header.
func OpenHTFile(path string) (
	*os.File, HTOffsets, [16]byte, uint64, error,
) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, HTOffsets{}, [16]byte{}, 0,
			fmt.Errorf("bitbox: open HT file: %w", err)
	}

	var header [headerSize]byte
	if _, err := f.ReadAt(header[:], 0); err != nil {
		f.Close()
		return nil, HTOffsets{}, [16]byte{}, 0,
			fmt.Errorf("bitbox: read header: %w", err)
	}

	var seed [16]byte
	copy(seed[:], header[seedOffset:seedOffset+16])
	capacity := binary.LittleEndian.Uint64(header[capacityOffset:])
	occupied := binary.LittleEndian.Uint64(header[occupiedOffset:])
	offsets := NewHTOffsets(capacity)

	return f, offsets, seed, occupied, nil
}

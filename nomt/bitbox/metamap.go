package bitbox

import (
	"fmt"
	"os"
)

// Meta byte constants.
const (
	// MetaEmpty marks an empty bucket.
	MetaEmpty byte = 0x00
	// MetaTombstone marks a deleted bucket (still probed through).
	MetaTombstone byte = 0x7F
)

// IsOccupied reports whether a meta byte indicates an occupied bucket.
// Occupied bytes have bit 7 set (value >= 0x80).
func IsOccupied(b byte) bool {
	return b&0x80 != 0
}

// IsEmpty reports whether a meta byte indicates an empty bucket.
func IsEmpty(b byte) bool {
	return b == MetaEmpty
}

// IsTombstone reports whether a meta byte indicates a tombstone.
func IsTombstone(b byte) bool {
	return b == MetaTombstone
}

// MakeOccupied creates an occupied meta byte from a hash value.
// It takes the top 7 bits of the hash and sets bit 7 to 1.
func MakeOccupied(hash uint64) byte {
	return 0x80 | byte(hash>>57)
}

// TagMatches reports whether an occupied meta byte could match a given hash.
func TagMatches(metaByte byte, hash uint64) bool {
	return IsOccupied(metaByte) && metaByte == MakeOccupied(hash)
}

// MetaMap holds an in-memory copy of all meta bytes for the hash table.
type MetaMap struct {
	data  []byte
	dirty []bool // per meta-page dirty tracking
}

// NewMetaMap creates a MetaMap for the given capacity with all empty buckets.
func NewMetaMap(capacity uint64) *MetaMap {
	metaPages := (capacity + metaBytesPerPage - 1) / metaBytesPerPage
	return &MetaMap{
		data:  make([]byte, capacity),
		dirty: make([]bool, metaPages),
	}
}

// LoadMetaMap reads all meta bytes from the HT file into memory.
func LoadMetaMap(f *os.File, offsets HTOffsets) (*MetaMap, error) {
	mm := NewMetaMap(offsets.Capacity)

	// Read all meta bytes at once.
	metaRegionSize := int64(offsets.MetaPages) * pageSize
	buf := make([]byte, metaRegionSize)
	if _, err := f.ReadAt(buf, int64(headerSize)); err != nil {
		return nil, fmt.Errorf("bitbox: load meta map: %w", err)
	}

	// Copy only the capacity-many bytes (the rest is padding).
	copy(mm.data, buf[:offsets.Capacity])
	return mm, nil
}

// Get returns the meta byte for a bucket.
func (m *MetaMap) Get(bucket uint64) byte {
	return m.data[bucket]
}

// Set writes a meta byte for a bucket and marks the containing page dirty.
func (m *MetaMap) Set(bucket uint64, value byte) {
	m.data[bucket] = value
	m.dirty[bucket/metaBytesPerPage] = true
}

// DirtyMetaPages returns the indices of meta pages that have been modified
// since the last call to ClearDirty.
func (m *MetaMap) DirtyMetaPages() []uint64 {
	pages := make([]uint64, 0, len(m.dirty))
	for i, d := range m.dirty {
		if d {
			pages = append(pages, uint64(i))
		}
	}
	return pages
}

// ClearDirty resets all dirty flags.
func (m *MetaMap) ClearDirty() {
	for i := range m.dirty {
		m.dirty[i] = false
	}
}

// WriteMetaPage writes a single meta page (identified by index) to the file.
func (m *MetaMap) WriteMetaPage(
	f *os.File, pageIdx uint64,
) error {
	var buf [pageSize]byte
	start := pageIdx * metaBytesPerPage
	end := min(start+metaBytesPerPage, uint64(len(m.data)))
	copy(buf[:], m.data[start:end])

	offset := int64(headerSize) + int64(pageIdx)*pageSize
	if _, err := f.WriteAt(buf[:], offset); err != nil {
		return fmt.Errorf("bitbox: write meta page %d: %w", pageIdx, err)
	}
	return nil
}

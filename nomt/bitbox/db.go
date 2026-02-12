package bitbox

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/nomt/core"
)

// DB is the Bitbox on-disk hash table for storing trie pages.
type DB struct {
	file     *os.File
	offsets  HTOffsets
	metaMap  *MetaMap
	seed     [16]byte
	capacity uint64
	occupied atomic.Int64
}

// Create creates a new Bitbox database at the given path.
// Capacity must be a power of 2.
func Create(path string, capacity uint64, seed [16]byte) (*DB, error) {
	if capacity == 0 || capacity&(capacity-1) != 0 {
		return nil, fmt.Errorf("bitbox: capacity must be a power of 2")
	}

	f, offsets, err := CreateHTFile(path, capacity, seed)
	if err != nil {
		return nil, err
	}

	mm := NewMetaMap(capacity)

	db := &DB{
		file:     f,
		offsets:  offsets,
		metaMap:  mm,
		seed:     seed,
		capacity: capacity,
	}
	return db, nil
}

// Open opens an existing Bitbox database.
func Open(path string) (*DB, error) {
	f, offsets, seed, occupied, err := OpenHTFile(path)
	if err != nil {
		return nil, err
	}

	mm, err := LoadMetaMap(f, offsets)
	if err != nil {
		f.Close()
		return nil, err
	}

	db := &DB{
		file:     f,
		offsets:  offsets,
		metaMap:  mm,
		seed:     seed,
		capacity: offsets.Capacity,
	}
	db.occupied.Store(int64(occupied))
	return db, nil
}

// Close closes the database file.
func (db *DB) Close() error {
	return db.file.Close()
}

// Seed returns the hash seed.
func (db *DB) Seed() [16]byte {
	return db.seed
}

// Capacity returns the total number of buckets.
func (db *DB) Capacity() uint64 {
	return db.capacity
}

// Occupied returns the number of occupied buckets.
func (db *DB) Occupied() int64 {
	return db.occupied.Load()
}

// LoadPage reads a page from the hash table by probing for its PageID.
// Returns the page, the bucket index where it was found, and whether it exists.
func (db *DB) LoadPage(pageID core.PageID) (
	*core.RawPage, uint64, bool, error,
) {
	hash := HashPageID(db.seed, pageID)
	probe := NewProbeSequence(hash, db.capacity)
	encodedID := pageID.Encode()

	for range db.capacity {
		bucket := probe.Bucket()
		meta := db.metaMap.Get(bucket)

		if IsEmpty(meta) {
			// Definitely not in the table.
			return nil, 0, false, nil
		}

		if IsTombstone(meta) {
			probe.Next()
			continue
		}

		if !TagMatches(meta, hash) {
			probe.Next()
			continue
		}

		// Tag matches — read the data page to confirm.
		page, err := db.readDataPage(bucket)
		if err != nil {
			return nil, 0, false, err
		}

		storedID := page.PageIDBytes()
		if storedID == encodedID {
			return page, bucket, true, nil
		}

		probe.Next()
	}

	return nil, 0, false, nil
}

// StorePage writes a page to the hash table. If the page already exists
// (by probing), it is overwritten in-place. Otherwise, a new bucket is
// allocated.
func (db *DB) StorePage(pageID core.PageID, page *core.RawPage) (
	uint64, error,
) {
	// Ensure the encoded PageID is in the page data.
	encodedID := pageID.Encode()
	page.SetPageIDBytes(encodedID)

	hash := HashPageID(db.seed, pageID)
	probe := NewProbeSequence(hash, db.capacity)
	metaByte := MakeOccupied(hash)

	var firstTombstone int64 = -1

	for range db.capacity {
		bucket := probe.Bucket()
		meta := db.metaMap.Get(bucket)

		if IsEmpty(meta) {
			// Use tombstone if we passed one, otherwise use this empty slot.
			target := bucket
			if firstTombstone >= 0 {
				target = uint64(firstTombstone)
			} else {
				db.occupied.Add(1)
			}
			db.metaMap.Set(target, metaByte)
			if err := db.writeDataPage(target, page); err != nil {
				return 0, err
			}
			return target, nil
		}

		if IsTombstone(meta) {
			if firstTombstone < 0 {
				firstTombstone = int64(bucket)
			}
			probe.Next()
			continue
		}

		if TagMatches(meta, hash) {
			// Check if this is the same page.
			existing, err := db.readDataPage(bucket)
			if err != nil {
				return 0, err
			}
			if existing.PageIDBytes() == encodedID {
				// Overwrite in-place.
				if err := db.writeDataPage(bucket, page); err != nil {
					return 0, err
				}
				return bucket, nil
			}
		}

		probe.Next()
	}

	return 0, fmt.Errorf("bitbox: hash table full")
}

// DeletePage removes a page from the hash table by setting its meta byte
// to tombstone.
func (db *DB) DeletePage(pageID core.PageID) (bool, error) {
	hash := HashPageID(db.seed, pageID)
	probe := NewProbeSequence(hash, db.capacity)
	encodedID := pageID.Encode()

	for range db.capacity {
		bucket := probe.Bucket()
		meta := db.metaMap.Get(bucket)

		if IsEmpty(meta) {
			return false, nil
		}

		if IsTombstone(meta) {
			probe.Next()
			continue
		}

		if TagMatches(meta, hash) {
			existing, err := db.readDataPage(bucket)
			if err != nil {
				return false, err
			}
			if existing.PageIDBytes() == encodedID {
				db.metaMap.Set(bucket, MetaTombstone)
				db.occupied.Add(-1)
				return true, nil
			}
		}

		probe.Next()
	}

	return false, nil
}

// FlushMeta writes all dirty meta pages to disk and updates the header.
func (db *DB) FlushMeta() error {
	for _, pageIdx := range db.metaMap.DirtyMetaPages() {
		if err := db.metaMap.WriteMetaPage(db.file, pageIdx); err != nil {
			return err
		}
	}
	db.metaMap.ClearDirty()

	// Update occupied count in header.
	var buf [8]byte
	occ := max(db.occupied.Load(), 0)
	binary.LittleEndian.PutUint64(buf[:], uint64(occ))
	if _, err := db.file.WriteAt(buf[:], occupiedOffset); err != nil {
		return fmt.Errorf("bitbox: update occupied count: %w", err)
	}

	return nil
}

// Sync flushes all pending data to disk.
func (db *DB) Sync() error {
	if err := db.FlushMeta(); err != nil {
		return err
	}
	return db.file.Sync()
}

// --- internal I/O ---

func (db *DB) readDataPage(bucket uint64) (*core.RawPage, error) {
	page := new(core.RawPage)
	offset := db.offsets.DataPageOffset(bucket)
	if _, err := db.file.ReadAt(page[:], offset); err != nil {
		return nil, fmt.Errorf("bitbox: read data page at bucket %d: %w",
			bucket, err)
	}
	return page, nil
}

func (db *DB) writeDataPage(bucket uint64, page *core.RawPage) error {
	offset := db.offsets.DataPageOffset(bucket)
	if _, err := db.file.WriteAt(page[:], offset); err != nil {
		return fmt.Errorf("bitbox: write data page at bucket %d: %w",
			bucket, err)
	}
	return nil
}

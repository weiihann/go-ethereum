package bitbox

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/nomt/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- HT File Layout Tests ---

func TestHTOffsetsMetaByteOffset(t *testing.T) {
	offsets := NewHTOffsets(8192)
	assert.Equal(t, int64(pageSize), offsets.MetaByteOffset(0))
	assert.Equal(t, int64(pageSize+1), offsets.MetaByteOffset(1))
}

func TestHTOffsetsDataPageOffset(t *testing.T) {
	// capacity=4096 → 1 meta page
	offsets := NewHTOffsets(4096)
	assert.Equal(t, uint64(1), offsets.MetaPages)

	// Data starts at: header(4096) + 1 meta page(4096) = 8192
	assert.Equal(t, int64(8192), offsets.DataPageOffset(0))
	assert.Equal(t, int64(8192+4096), offsets.DataPageOffset(1))
}

func TestHTOffsetsTotalFileSize(t *testing.T) {
	offsets := NewHTOffsets(4096)
	// header(4096) + 1 meta page(4096) + 4096 data pages * 4096
	expected := int64(4096 + 4096 + 4096*4096)
	assert.Equal(t, expected, offsets.TotalFileSize())
}

func TestHTOffsetsMetaPagesRoundup(t *testing.T) {
	offsets := NewHTOffsets(5000)
	assert.Equal(t, uint64(2), offsets.MetaPages)
}

func TestCreateOpenHTFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.ht")

	seed := HashSeedFromUint64(42, 99)
	f, offsets, err := CreateHTFile(path, 1024, seed)
	require.NoError(t, err)
	assert.Equal(t, uint64(1024), offsets.Capacity)
	f.Close()

	f2, offsets2, seed2, occ, err := OpenHTFile(path)
	require.NoError(t, err)
	defer f2.Close()

	assert.Equal(t, seed, seed2)
	assert.Equal(t, uint64(1024), offsets2.Capacity)
	assert.Equal(t, uint64(0), occ)
}

// --- Meta Byte Tests ---

func TestMetaByteEncoding(t *testing.T) {
	assert.True(t, IsEmpty(MetaEmpty))
	assert.False(t, IsOccupied(MetaEmpty))
	assert.False(t, IsTombstone(MetaEmpty))

	assert.True(t, IsTombstone(MetaTombstone))
	assert.False(t, IsEmpty(MetaTombstone))
	assert.False(t, IsOccupied(MetaTombstone))

	occupied := MakeOccupied(0xFFFFFFFFFFFFFFFF)
	assert.True(t, IsOccupied(occupied))
	assert.False(t, IsEmpty(occupied))
	assert.False(t, IsTombstone(occupied))
}

func TestMetaByteTagMatching(t *testing.T) {
	hash := uint64(0xABCDEF1234567890)
	meta := MakeOccupied(hash)
	assert.True(t, TagMatches(meta, hash))

	// Different high bits should not match.
	differentHash := uint64(0x1234EF1234567890)
	assert.False(t, TagMatches(meta, differentHash))
}

func TestMetaMapSetGet(t *testing.T) {
	mm := NewMetaMap(8192)
	assert.Equal(t, MetaEmpty, mm.Get(0))

	mm.Set(100, MakeOccupied(12345))
	assert.True(t, IsOccupied(mm.Get(100)))
}

func TestMetaMapDirtyTracking(t *testing.T) {
	mm := NewMetaMap(8192) // 2 meta pages
	assert.Empty(t, mm.DirtyMetaPages())

	mm.Set(0, MetaTombstone)    // page 0
	mm.Set(5000, MetaTombstone) // page 1

	dirty := mm.DirtyMetaPages()
	assert.Len(t, dirty, 2)
	assert.Contains(t, dirty, uint64(0))
	assert.Contains(t, dirty, uint64(1))

	mm.ClearDirty()
	assert.Empty(t, mm.DirtyMetaPages())
}

// --- Probe Sequence Tests ---

func TestProbeSequenceInitial(t *testing.T) {
	p := NewProbeSequence(42, 1024)
	assert.Equal(t, uint64(42%1024), p.Bucket())
	assert.Equal(t, uint64(42), p.Hash())
}

func TestProbeSequenceTriangular(t *testing.T) {
	p := NewProbeSequence(0, 16) // initial bucket = 0
	assert.Equal(t, uint64(0), p.Bucket())

	p.Next() // step=1 → (0+1)%16 = 1
	assert.Equal(t, uint64(1), p.Bucket())

	p.Next() // step=2 → (1+2)%16 = 3
	assert.Equal(t, uint64(3), p.Bucket())

	p.Next() // step=3 → (3+3)%16 = 6
	assert.Equal(t, uint64(6), p.Bucket())

	p.Next() // step=4 → (6+4)%16 = 10
	assert.Equal(t, uint64(10), p.Bucket())
}

func TestProbeSequenceVisitsAll(t *testing.T) {
	// With power-of-2 capacity, triangular probing should visit all buckets.
	capacity := uint64(16)
	p := NewProbeSequence(0, capacity)

	visited := make(map[uint64]bool, capacity)
	for range capacity {
		visited[p.Bucket()] = true
		p.Next()
	}

	assert.Equal(t, int(capacity), len(visited),
		"triangular probing should visit all buckets")
}

func TestHashPageID(t *testing.T) {
	seed := HashSeedFromUint64(1, 2)
	root := core.RootPageID()
	h1 := HashPageID(seed, root)
	h2 := HashPageID(seed, root)
	assert.Equal(t, h1, h2, "same inputs should produce same hash")

	// Different seed should produce different hash.
	seed2 := HashSeedFromUint64(3, 4)
	h3 := HashPageID(seed2, root)
	assert.NotEqual(t, h1, h3)
}

// --- DB Integration Tests ---

func TestDBCreateAndOpen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bitbox")

	seed := HashSeedFromUint64(1, 2)
	db, err := Create(path, 1024, seed)
	require.NoError(t, err)
	assert.Equal(t, uint64(1024), db.Capacity())
	assert.Equal(t, int64(0), db.Occupied())
	require.NoError(t, db.Sync())
	require.NoError(t, db.Close())

	db2, err := Open(path)
	require.NoError(t, err)
	defer db2.Close()
	assert.Equal(t, seed, db2.Seed())
	assert.Equal(t, uint64(1024), db2.Capacity())
}

func TestDBStoreAndLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bitbox")

	seed := HashSeedFromUint64(1, 2)
	db, err := Create(path, 1024, seed)
	require.NoError(t, err)
	defer db.Close()

	// Store a page.
	rootID := core.RootPageID()
	page := new(core.RawPage)
	page.SetNodeAt(0, core.Node{0x42})

	bucket, err := db.StorePage(rootID, page)
	require.NoError(t, err)
	assert.Equal(t, int64(1), db.Occupied())

	// Load it back.
	loaded, loadBucket, found, err := db.LoadPage(rootID)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, bucket, loadBucket)
	assert.Equal(t, core.Node{0x42}, loaded.NodeAt(0))
}

func TestDBStoreOverwrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bitbox")

	seed := HashSeedFromUint64(1, 2)
	db, err := Create(path, 1024, seed)
	require.NoError(t, err)
	defer db.Close()

	rootID := core.RootPageID()
	page1 := new(core.RawPage)
	page1.SetNodeAt(0, core.Node{0x01})
	_, err = db.StorePage(rootID, page1)
	require.NoError(t, err)

	// Overwrite with new data.
	page2 := new(core.RawPage)
	page2.SetNodeAt(0, core.Node{0x02})
	_, err = db.StorePage(rootID, page2)
	require.NoError(t, err)

	// Should still only have 1 occupied.
	assert.Equal(t, int64(1), db.Occupied())

	loaded, _, found, err := db.LoadPage(rootID)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, core.Node{0x02}, loaded.NodeAt(0))
}

func TestDBDelete(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bitbox")

	seed := HashSeedFromUint64(1, 2)
	db, err := Create(path, 1024, seed)
	require.NoError(t, err)
	defer db.Close()

	rootID := core.RootPageID()
	page := new(core.RawPage)
	_, err = db.StorePage(rootID, page)
	require.NoError(t, err)

	deleted, err := db.DeletePage(rootID)
	require.NoError(t, err)
	assert.True(t, deleted)
	assert.Equal(t, int64(0), db.Occupied())

	_, _, found, err := db.LoadPage(rootID)
	require.NoError(t, err)
	assert.False(t, found)
}

func TestDBDeleteNonexistent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bitbox")

	seed := HashSeedFromUint64(1, 2)
	db, err := Create(path, 1024, seed)
	require.NoError(t, err)
	defer db.Close()

	rootID := core.RootPageID()
	deleted, err := db.DeletePage(rootID)
	require.NoError(t, err)
	assert.False(t, deleted)
}

func TestDBLoadMiss(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bitbox")

	seed := HashSeedFromUint64(1, 2)
	db, err := Create(path, 1024, seed)
	require.NoError(t, err)
	defer db.Close()

	rootID := core.RootPageID()
	_, _, found, err := db.LoadPage(rootID)
	require.NoError(t, err)
	assert.False(t, found)
}

func TestDBMultiplePages(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bitbox")

	seed := HashSeedFromUint64(1, 2)
	db, err := Create(path, 1024, seed)
	require.NoError(t, err)
	defer db.Close()

	rootID := core.RootPageID()
	childID, err := rootID.ChildPageID(0)
	require.NoError(t, err)
	childID2, err := rootID.ChildPageID(1)
	require.NoError(t, err)

	// Store 3 pages.
	for i, pid := range []core.PageID{rootID, childID, childID2} {
		page := new(core.RawPage)
		page.SetNodeAt(0, core.Node{byte(i + 1)})
		_, err := db.StorePage(pid, page)
		require.NoError(t, err)
	}

	assert.Equal(t, int64(3), db.Occupied())

	// Load each one.
	for i, pid := range []core.PageID{rootID, childID, childID2} {
		loaded, _, found, err := db.LoadPage(pid)
		require.NoError(t, err)
		assert.True(t, found, "page %d", i)
		assert.Equal(t, core.Node{byte(i + 1)}, loaded.NodeAt(0))
	}
}

func TestDBPersistAndReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bitbox")

	seed := HashSeedFromUint64(1, 2)
	db, err := Create(path, 1024, seed)
	require.NoError(t, err)

	rootID := core.RootPageID()
	page := new(core.RawPage)
	page.SetNodeAt(0, core.Node{0xAB})
	_, err = db.StorePage(rootID, page)
	require.NoError(t, err)

	require.NoError(t, db.Sync())
	require.NoError(t, db.Close())

	// Reopen and verify.
	db2, err := Open(path)
	require.NoError(t, err)
	defer db2.Close()

	loaded, _, found, err := db2.LoadPage(rootID)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, core.Node{0xAB}, loaded.NodeAt(0))
}

func TestDBCapacityMustBePowerOf2(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bitbox")
	seed := HashSeedFromUint64(1, 2)

	_, err := Create(path, 1000, seed)
	assert.Error(t, err)
	// Cleanup any partial file.
	os.Remove(path)
}

func TestDBDeleteAndReinsert(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bitbox")

	seed := HashSeedFromUint64(1, 2)
	db, err := Create(path, 1024, seed)
	require.NoError(t, err)
	defer db.Close()

	rootID := core.RootPageID()

	// Insert → delete → insert should work.
	page1 := new(core.RawPage)
	page1.SetNodeAt(0, core.Node{0x01})
	_, err = db.StorePage(rootID, page1)
	require.NoError(t, err)

	_, err = db.DeletePage(rootID)
	require.NoError(t, err)

	page2 := new(core.RawPage)
	page2.SetNodeAt(0, core.Node{0x02})
	_, err = db.StorePage(rootID, page2)
	require.NoError(t, err)

	loaded, _, found, err := db.LoadPage(rootID)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, core.Node{0x02}, loaded.NodeAt(0))
}

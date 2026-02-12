package bitbox

import (
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/nomt/core"
	"github.com/ethereum/go-ethereum/nomt/merkle"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- WAL Builder/Reader Tests ---

func TestWALEmptyRoundTrip(t *testing.T) {
	b := NewWALBuilder()
	data := b.Finish(42)

	// Should be padded to page boundary.
	assert.Equal(t, 0, len(data)%pageSize)

	seqn, entries, err := ReadWAL(data)
	require.NoError(t, err)
	assert.Equal(t, uint32(42), seqn)
	assert.Empty(t, entries)
}

func TestWALClearEntryRoundTrip(t *testing.T) {
	b := NewWALBuilder()
	b.AddClear(123)
	b.AddClear(456)
	data := b.Finish(1)

	seqn, entries, err := ReadWAL(data)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), seqn)
	require.Len(t, entries, 2)

	assert.Equal(t, WALEntryClear, entries[0].Kind)
	assert.Equal(t, uint64(123), entries[0].ClearBucket)
	assert.Equal(t, uint64(456), entries[1].ClearBucket)
}

func TestWALUpdateEntryRoundTrip(t *testing.T) {
	var pageID [32]byte
	pageID[0] = 0xAB

	var diff core.PageDiff
	diff.SetChanged(5)
	diff.SetChanged(70)

	nodes := []core.Node{{0x01}, {0x02}}

	b := NewWALBuilder()
	b.AddUpdate(pageID, diff, nodes, 0xFF, 99)
	data := b.Finish(7)

	seqn, entries, err := ReadWAL(data)
	require.NoError(t, err)
	assert.Equal(t, uint32(7), seqn)
	require.Len(t, entries, 1)

	e := entries[0]
	assert.Equal(t, WALEntryUpdate, e.Kind)
	assert.Equal(t, pageID, e.PageID)
	assert.True(t, e.Diff.IsChanged(5))
	assert.True(t, e.Diff.IsChanged(70))
	require.Len(t, e.ChangedNodes, 2)
	assert.Equal(t, core.Node{0x01}, e.ChangedNodes[0])
	assert.Equal(t, core.Node{0x02}, e.ChangedNodes[1])
	assert.Equal(t, uint64(0xFF), e.ElidedChildren)
	assert.Equal(t, uint64(99), e.UpdateBucket)
}

func TestWALMixedEntries(t *testing.T) {
	b := NewWALBuilder()
	b.AddClear(10)

	var pid [32]byte
	var diff core.PageDiff
	diff.SetChanged(0)
	b.AddUpdate(pid, diff, []core.Node{{0xAA}}, 0, 20)

	b.AddClear(30)
	data := b.Finish(100)

	_, entries, err := ReadWAL(data)
	require.NoError(t, err)
	require.Len(t, entries, 3)
	assert.Equal(t, WALEntryClear, entries[0].Kind)
	assert.Equal(t, WALEntryUpdate, entries[1].Kind)
	assert.Equal(t, WALEntryClear, entries[2].Kind)
}

func TestReadWALEmpty(t *testing.T) {
	seqn, entries, err := ReadWAL(nil)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), seqn)
	assert.Nil(t, entries)
}

func TestWALFilePersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	b := NewWALBuilder()
	b.AddClear(42)
	data := b.Finish(5)

	require.NoError(t, WriteWALFile(path, data))

	loaded, err := ReadWALFile(path)
	require.NoError(t, err)
	assert.Equal(t, data, loaded)

	require.NoError(t, TruncateWALFile(path))
	loaded2, err := ReadWALFile(path)
	require.NoError(t, err)
	assert.Empty(t, loaded2)
}

// --- Sync Controller Tests ---

func TestFullSyncCycle(t *testing.T) {
	dir := t.TempDir()
	htPath := filepath.Join(dir, "test.bitbox")
	walPath := filepath.Join(dir, "test.wal")

	seed := HashSeedFromUint64(1, 2)
	db, err := Create(htPath, 1024, seed)
	require.NoError(t, err)
	defer db.Close()

	rootID := core.RootPageID()
	page := new(core.RawPage)
	page.SetNodeAt(0, core.Node{0xAA})

	var diff core.PageDiff
	diff.SetChanged(0)

	updates := []merkle.UpdatedPage{{
		PageID: rootID,
		Page:   page,
		Diff:   diff,
	}}

	require.NoError(t, db.FullSync(walPath, 1, updates))

	// Verify page is persisted.
	loaded, _, found, err := db.LoadPage(rootID)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, core.Node{0xAA}, loaded.NodeAt(0))
}

// --- Recovery Tests ---

func TestRecoverFromWAL(t *testing.T) {
	dir := t.TempDir()
	htPath := filepath.Join(dir, "test.bitbox")
	walPath := filepath.Join(dir, "test.wal")

	seed := HashSeedFromUint64(1, 2)

	// Create DB and write a WAL but don't commit Phase 3.
	db, err := Create(htPath, 1024, seed)
	require.NoError(t, err)

	rootID := core.RootPageID()
	page := new(core.RawPage)
	page.SetNodeAt(0, core.Node{0xBB})

	var diff core.PageDiff
	diff.SetChanged(0)

	updates := []merkle.UpdatedPage{{
		PageID: rootID,
		Page:   page,
		Diff:   diff,
	}}

	// Phase 1 + 2 only (simulate crash before Phase 3).
	plan, err := db.BeginSync(walPath, 5, updates)
	require.NoError(t, err)
	require.NoError(t, db.WriteWAL(walPath, plan))
	db.Close()

	// Reopen and recover.
	db2, err := Open(htPath)
	require.NoError(t, err)
	defer db2.Close()

	seqn, err := db2.Recover(walPath)
	require.NoError(t, err)
	assert.Equal(t, uint32(5), seqn)

	// Verify the page was recovered.
	loaded, _, found, err := db2.LoadPage(rootID)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, core.Node{0xBB}, loaded.NodeAt(0))
}

func TestRecoverNoWAL(t *testing.T) {
	dir := t.TempDir()
	htPath := filepath.Join(dir, "test.bitbox")
	walPath := filepath.Join(dir, "test.wal")

	seed := HashSeedFromUint64(1, 2)
	db, err := Create(htPath, 1024, seed)
	require.NoError(t, err)
	defer db.Close()

	seqn, err := db.Recover(walPath)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), seqn, "no recovery needed")
}

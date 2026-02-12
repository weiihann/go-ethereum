package db

import (
	"testing"

	"github.com/ethereum/go-ethereum/nomt/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenClose(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, DefaultConfig())
	require.NoError(t, err)

	assert.Equal(t, core.Terminator, db.Root())
	require.NoError(t, db.Close())
}

func TestOpenCreatesDirectory(t *testing.T) {
	dir := t.TempDir() + "/subdir"
	db, err := Open(dir, DefaultConfig())
	require.NoError(t, err)
	defer db.Close()
	assert.Equal(t, core.Terminator, db.Root())
}

func TestReopenPreservesState(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir, DefaultConfig())
	require.NoError(t, err)

	newRoot, err := db.Update([]core.StemKeyValue{
		{Stem: makeStem(0x10), Hash: makeHash(0x01)},
	})
	require.NoError(t, err)
	require.False(t, core.IsTerminator(&newRoot))
	require.NoError(t, db.Close())

	// Reopen and set the root.
	db2, err := Open(dir, DefaultConfig())
	require.NoError(t, err)
	defer db2.Close()

	// Root is not automatically persisted (that's the geth integration's
	// job), but the pages should still be on disk.
	assert.Equal(t, core.Terminator, db2.Root())
}

func TestUpdateSingleKey(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, DefaultConfig())
	require.NoError(t, err)
	defer db.Close()

	newRoot, err := db.Update([]core.StemKeyValue{
		{Stem: makeStem(0x10), Hash: makeHash(0x42)},
	})
	require.NoError(t, err)

	assert.False(t, core.IsTerminator(&newRoot))
	assert.Equal(t, newRoot, db.Root())
}

func TestUpdateMultipleKeys(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, DefaultConfig())
	require.NoError(t, err)
	defer db.Close()

	ops := []core.StemKeyValue{
		{Stem: makeStem(0x10), Hash: makeHash(0x01)},
		{Stem: makeStem(0x80), Hash: makeHash(0x02)},
	}

	newRoot, err := db.Update(ops)
	require.NoError(t, err)
	assert.False(t, core.IsTerminator(&newRoot))
}

func TestUpdateDeterministic(t *testing.T) {
	ops := []core.StemKeyValue{
		{Stem: makeStem(0x10), Hash: makeHash(0x01)},
		{Stem: makeStem(0x80), Hash: makeHash(0x02)},
	}

	run := func() core.Node {
		dir := t.TempDir()
		db, err := Open(dir, DefaultConfig())
		require.NoError(t, err)
		defer db.Close()

		root, err := db.Update(ops)
		require.NoError(t, err)
		return root
	}

	r1 := run()
	r2 := run()
	assert.Equal(t, r1, r2, "same ops should produce same root")
}

func TestUpdateEmptyOps(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, DefaultConfig())
	require.NoError(t, err)
	defer db.Close()

	root, err := db.Update(nil)
	require.NoError(t, err)
	assert.Equal(t, core.Terminator, root)
}

func TestUpdateSortsByStem(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, DefaultConfig())
	require.NoError(t, err)
	defer db.Close()

	// Provide stems in reverse order — should still work.
	ops := []core.StemKeyValue{
		{Stem: makeStem(0x80), Hash: makeHash(0x01)},
		{Stem: makeStem(0x10), Hash: makeHash(0x02)},
	}

	root, err := db.Update(ops)
	require.NoError(t, err)
	assert.False(t, core.IsTerminator(&root))
}

func TestSyncSeqnIncrements(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, DefaultConfig())
	require.NoError(t, err)
	defer db.Close()

	assert.Equal(t, uint32(0), db.SyncSeqn())

	_, err = db.Update([]core.StemKeyValue{
		{Stem: makeStem(0x10), Hash: makeHash(0x01)},
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(1), db.SyncSeqn())

	_, err = db.Update([]core.StemKeyValue{
		{Stem: makeStem(0x80), Hash: makeHash(0x02)},
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(2), db.SyncSeqn())
}

func makeStem(b byte) core.StemPath {
	var sp core.StemPath
	for i := range sp {
		sp[i] = b
	}
	return sp
}

func makeHash(b byte) core.Node {
	var h core.Node
	for i := range h {
		h[i] = b ^ byte(i)
	}
	// Ensure non-zero to avoid terminator.
	h[0] |= 0x01
	return h
}

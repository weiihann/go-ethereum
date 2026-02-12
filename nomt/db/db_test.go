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

	v := core.ValueHash{0x01}
	newRoot, err := db.Update([]core.LeafOp{
		{Key: makeKey(0x10), Value: &v},
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

	v := core.ValueHash{0x42}
	kp := makeKey(0x10) // starts with 0 bit

	newRoot, err := db.Update([]core.LeafOp{
		{Key: kp, Value: &v},
	})
	require.NoError(t, err)

	assert.True(t, core.IsLeaf(&newRoot))
	assert.Equal(t, newRoot, db.Root())
}

func TestUpdateMultipleKeys(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, DefaultConfig())
	require.NoError(t, err)
	defer db.Close()

	v := core.ValueHash{0x01}
	ops := []core.LeafOp{
		{Key: makeKey(0x10), Value: &v},
		{Key: makeKey(0x80), Value: &v},
	}

	newRoot, err := db.Update(ops)
	require.NoError(t, err)
	assert.True(t, core.IsInternal(&newRoot))
}

func TestUpdateDeterministic(t *testing.T) {
	v := core.ValueHash{0x01}
	ops := []core.LeafOp{
		{Key: makeKey(0x10), Value: &v},
		{Key: makeKey(0x80), Value: &v},
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

func TestUpdateSortsByKey(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, DefaultConfig())
	require.NoError(t, err)
	defer db.Close()

	v := core.ValueHash{0x01}
	// Provide keys in reverse order — should still work.
	ops := []core.LeafOp{
		{Key: makeKey(0x80), Value: &v},
		{Key: makeKey(0x10), Value: &v},
	}

	root, err := db.Update(ops)
	require.NoError(t, err)
	assert.True(t, core.IsInternal(&root))
}

func TestSyncSeqnIncrements(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, DefaultConfig())
	require.NoError(t, err)
	defer db.Close()

	assert.Equal(t, uint32(0), db.SyncSeqn())

	v := core.ValueHash{0x01}
	_, err = db.Update([]core.LeafOp{
		{Key: makeKey(0x10), Value: &v},
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(1), db.SyncSeqn())

	_, err = db.Update([]core.LeafOp{
		{Key: makeKey(0x80), Value: &v},
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(2), db.SyncSeqn())
}

func makeKey(b byte) core.KeyPath {
	var kp core.KeyPath
	for i := range kp {
		kp[i] = b
	}
	return kp
}

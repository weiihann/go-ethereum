package nomttrie

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/nomtdb"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestBackend(t *testing.T) *nomtdb.Database {
	t.Helper()
	dir := t.TempDir()
	diskdb := rawdb.NewMemoryDatabase()
	config := &triedb.Config{
		NomtDB: &nomtdb.Config{
			DataDir:    dir,
			HTCapacity: 1024,
		},
	}
	tdb := triedb.NewDatabase(diskdb, config)
	t.Cleanup(func() { tdb.Close() })
	return tdb.NomtBackend()
}

func TestNewEmptyTrie(t *testing.T) {
	backend := newTestBackend(t)
	tr, err := New(common.Hash{}, backend)
	require.NoError(t, err)

	// Empty trie should hash to zero (NOMT Terminator).
	root := tr.Hash()
	assert.Equal(t, common.Hash{}, root)
}

func TestUpdateSingleAccount(t *testing.T) {
	backend := newTestBackend(t)
	tr, err := New(common.Hash{}, backend)
	require.NoError(t, err)

	addr := common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678")
	acc := &types.StateAccount{
		Nonce:    42,
		Balance:  uint256.NewInt(1000),
		Root:     types.EmptyRootHash,
		CodeHash: types.EmptyCodeHash.Bytes(),
	}

	require.NoError(t, tr.UpdateAccount(addr, acc, 0))
	root := tr.Hash()
	assert.NotEqual(t, common.Hash{}, root, "root should be non-zero after insert")
}

func TestUpdateTwoAccounts(t *testing.T) {
	backend := newTestBackend(t)
	tr, err := New(common.Hash{}, backend)
	require.NoError(t, err)

	addr1 := common.HexToAddress("0x1111111111111111111111111111111111111111")
	addr2 := common.HexToAddress("0x2222222222222222222222222222222222222222")
	acc := &types.StateAccount{
		Nonce:    1,
		Balance:  uint256.NewInt(100),
		Root:     types.EmptyRootHash,
		CodeHash: types.EmptyCodeHash.Bytes(),
	}

	require.NoError(t, tr.UpdateAccount(addr1, acc, 0))
	require.NoError(t, tr.UpdateAccount(addr2, acc, 0))

	root := tr.Hash()
	assert.NotEqual(t, common.Hash{}, root)
}

func TestDeterministicRoot(t *testing.T) {
	addr := common.HexToAddress("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
	acc := &types.StateAccount{
		Nonce:    7,
		Balance:  uint256.NewInt(999),
		Root:     types.EmptyRootHash,
		CodeHash: types.EmptyCodeHash.Bytes(),
	}

	run := func() common.Hash {
		backend := newTestBackend(t)
		tr, err := New(common.Hash{}, backend)
		require.NoError(t, err)
		require.NoError(t, tr.UpdateAccount(addr, acc, 0))
		return tr.Hash()
	}

	r1 := run()
	r2 := run()
	assert.Equal(t, r1, r2, "same ops should produce same root")
}

func TestCommitReturnsRoot(t *testing.T) {
	backend := newTestBackend(t)
	tr, err := New(common.Hash{}, backend)
	require.NoError(t, err)

	addr := common.HexToAddress("0xaaaa")
	acc := &types.StateAccount{
		Nonce:    1,
		Balance:  uint256.NewInt(1),
		Root:     types.EmptyRootHash,
		CodeHash: types.EmptyCodeHash.Bytes(),
	}
	require.NoError(t, tr.UpdateAccount(addr, acc, 0))

	root, nodeSet := tr.Commit(false)
	assert.NotEqual(t, common.Hash{}, root)
	assert.NotNil(t, nodeSet)
}

func TestDeleteAccount(t *testing.T) {
	backend := newTestBackend(t)
	tr, err := New(common.Hash{}, backend)
	require.NoError(t, err)

	addr := common.HexToAddress("0xbbbb")
	acc := &types.StateAccount{
		Nonce:    1,
		Balance:  uint256.NewInt(100),
		Root:     types.EmptyRootHash,
		CodeHash: types.EmptyCodeHash.Bytes(),
	}

	require.NoError(t, tr.UpdateAccount(addr, acc, 0))
	root1 := tr.Hash()
	assert.NotEqual(t, common.Hash{}, root1)

	// DeleteAccount accumulates a nil-value LeafOp. Verify it's queued.
	require.NoError(t, tr.DeleteAccount(addr))
	assert.True(t, tr.dirty)
	assert.Len(t, tr.pending, 1)
}

func TestUpdateStorage(t *testing.T) {
	backend := newTestBackend(t)
	tr, err := New(common.Hash{}, backend)
	require.NoError(t, err)

	addr := common.HexToAddress("0xcccc")
	slot := common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000001")
	value := common.Hex2Bytes("00000000000000000000000000000000000000000000000000000000000000ff")

	require.NoError(t, tr.UpdateStorage(addr, slot, value))
	root := tr.Hash()
	assert.NotEqual(t, common.Hash{}, root)
}

func TestStorageKeyPathDeterminism(t *testing.T) {
	addr := common.HexToAddress("0xdddd")
	slot := common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000001")

	kp1 := storageKeyPath(addr, slot)
	kp2 := storageKeyPath(addr, slot)
	assert.Equal(t, kp1, kp2, "same inputs should produce same keypath")

	// Different slot should produce different keypath.
	slot2 := common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000002")
	kp3 := storageKeyPath(addr, slot2)
	assert.NotEqual(t, kp1, kp3)
}

func TestAccountKeyPathMatchesCryptoKeccak(t *testing.T) {
	addr := common.HexToAddress("0xeeee")
	expected := crypto.Keccak256Hash(addr.Bytes())

	backend := newTestBackend(t)
	tr, err := New(common.Hash{}, backend)
	require.NoError(t, err)

	acc := &types.StateAccount{
		Nonce:    1,
		Balance:  uint256.NewInt(1),
		Root:     types.EmptyRootHash,
		CodeHash: types.EmptyCodeHash.Bytes(),
	}
	require.NoError(t, tr.UpdateAccount(addr, acc, 0))

	// Pending op should have keypath = keccak256(addr).
	require.Len(t, tr.pending, 1)
	assert.Equal(t, [32]byte(expected), tr.pending[0].Key)

	// After Hash(), pending should be flushed.
	tr.Hash()
	assert.Len(t, tr.pending, 0)
}

func TestIsVerkle(t *testing.T) {
	backend := newTestBackend(t)
	tr, err := New(common.Hash{}, backend)
	require.NoError(t, err)
	assert.False(t, tr.IsVerkle())
}

func TestCopy(t *testing.T) {
	backend := newTestBackend(t)
	tr, err := New(common.Hash{}, backend)
	require.NoError(t, err)

	addr := common.HexToAddress("0xffff")
	acc := &types.StateAccount{
		Nonce:    5,
		Balance:  uint256.NewInt(500),
		Root:     types.EmptyRootHash,
		CodeHash: types.EmptyCodeHash.Bytes(),
	}
	require.NoError(t, tr.UpdateAccount(addr, acc, 0))

	cp := tr.Copy()
	assert.Equal(t, len(tr.pending), len(cp.pending))
	assert.Equal(t, tr.root, cp.root)
	assert.Equal(t, tr.dirty, cp.dirty)
}

func TestMultipleHashCalls(t *testing.T) {
	backend := newTestBackend(t)
	tr, err := New(common.Hash{}, backend)
	require.NoError(t, err)

	addr1 := common.HexToAddress("0x1111")
	addr2 := common.HexToAddress("0x2222")
	acc := &types.StateAccount{
		Nonce:    1,
		Balance:  uint256.NewInt(1),
		Root:     types.EmptyRootHash,
		CodeHash: types.EmptyCodeHash.Bytes(),
	}

	// First update + hash.
	require.NoError(t, tr.UpdateAccount(addr1, acc, 0))
	root1 := tr.Hash()
	assert.NotEqual(t, common.Hash{}, root1)

	// Second update + hash (simulating per-transaction root computation).
	require.NoError(t, tr.UpdateAccount(addr2, acc, 0))
	root2 := tr.Hash()
	assert.NotEqual(t, common.Hash{}, root2)
	assert.NotEqual(t, root1, root2, "root should change after second update")
}

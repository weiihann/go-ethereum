package krogan

import (
	"math/big"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test helper - maintains block number to hash mapping
type testChain struct {
	hashes map[uint64]common.Hash
	mu     sync.Mutex
}

func newTestChain() *testChain {
	return &testChain{
		hashes: make(map[uint64]common.Hash),
	}
}

func (tc *testChain) createBlock(number uint64, parentHash common.Hash) (*BlockData, common.Hash) {
	header := &types.Header{
		Number:     new(big.Int).SetUint64(number),
		ParentHash: parentHash,
	}
	block := types.NewBlock(header, &types.Body{}, nil, nil)
	hash := block.Hash()

	tc.mu.Lock()
	tc.hashes[number] = hash
	tc.mu.Unlock()

	return &BlockData{
		Block:    block,
		Receipts: nil,
		Accounts: map[common.Hash]*types.SlimAccount{
			common.HexToHash("0x01"): {
				Nonce:    number,
				Balance:  uint256.NewInt(1),
				Root:     []byte("root1"),
				CodeHash: []byte("code1"),
			},
		},
		Storages: map[common.Hash]map[common.Hash]common.Hash{
			common.HexToHash("0x01"): {
				common.HexToHash("0x02"): common.BytesToHash([]byte("storage1")),
			},
		},
		Codes: map[common.Hash][]byte{
			common.HexToHash("0x03"): []byte("code1"),
		},
	}, hash
}

func (tc *testChain) getHash(number uint64) common.Hash {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.hashes[number]
}

func TestNewChainWindow(t *testing.T) {
	buffer := NewChainWindow(1024)

	require.NotNil(t, buffer)
	assert.Equal(t, uint64(1024), buffer.capacity)
	assert.Equal(t, uint64(0), buffer.head)
	assert.Equal(t, uint64(0), buffer.tail)
	assert.NotNil(t, buffer.numToBlock)
	assert.NotNil(t, buffer.hashToBlock)
	assert.Equal(t, 0, len(buffer.numToBlock))
	assert.Equal(t, 0, len(buffer.hashToBlock))
}

func TestInsertBlock_SingleBlock(t *testing.T) {
	buffer := NewChainWindow(10)
	tc := newTestChain()

	data, hash := tc.createBlock(1, common.Hash{})
	err := buffer.InsertBlock(data)
	require.NoError(t, err)

	// Verify head and tail
	assert.Equal(t, uint64(1), buffer.head)
	assert.Equal(t, uint64(1), buffer.tail)

	// Verify block exists
	block, err := buffer.getBlockByNumber(1)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), block.Number())
	assert.Equal(t, hash, block.Hash())
	assert.Equal(t, common.Hash{}, block.ParentHash())
	assert.Nil(t, block.parent)
	assert.Nil(t, block.child)
}

func TestInsertBlock_SequentialBlocks(t *testing.T) {
	buffer := NewChainWindow(10)
	tc := newTestChain()

	// Insert blocks 1-5 sequentially
	for i := uint64(1); i <= 5; i++ {
		var parentHash common.Hash
		if i > 1 {
			parentHash = tc.getHash(i - 1)
		}
		data, _ := tc.createBlock(i, parentHash)
		err := buffer.InsertBlock(data)
		require.NoError(t, err)
	}

	// Verify head and tail
	assert.Equal(t, uint64(5), buffer.head)
	assert.Equal(t, uint64(1), buffer.tail)

	// Verify chain linkage (block 3)
	block3, err := buffer.getBlockByNumber(3)
	require.NoError(t, err)
	assert.NotNil(t, block3.parent)
	assert.Equal(t, uint64(2), block3.parent.Number())
	assert.NotNil(t, block3.child)
	assert.Equal(t, uint64(4), block3.child.Number())
}

func TestInsertBlock_Duplicate(t *testing.T) {
	buffer := NewChainWindow(10)
	tc := newTestChain()

	data, _ := tc.createBlock(1, common.Hash{})

	// Insert block twice
	err := buffer.InsertBlock(data)
	require.NoError(t, err)

	err = buffer.InsertBlock(data)
	require.NoError(t, err)

	// Should still have only one block
	assert.Equal(t, 1, len(buffer.numToBlock))
	assert.Equal(t, 1, len(buffer.hashToBlock))
}

func TestInsertBlock_Reorg_ReplaceAtSameHeight(t *testing.T) {
	buffer := NewChainWindow(10)
	tc := newTestChain()

	// Insert blocks 1-5
	for i := uint64(1); i <= 5; i++ {
		var parentHash common.Hash
		if i > 1 {
			parentHash = tc.getHash(i - 1)
		}
		data, _ := tc.createBlock(i, parentHash)
		err := buffer.InsertBlock(data)
		require.NoError(t, err)
	}

	// Reorg at height 3 (different hash)
	// Create a different block at the same height by using a different parent hash
	newData, newHash3 := tc.createBlock(3, common.HexToHash("0xdifferentparent"))
	err := buffer.InsertBlock(newData)
	require.NoError(t, err)

	// Verify blocks 4-5 are out of bounds (truncated)
	_, err = buffer.getBlockByNumber(4)
	assert.ErrorIs(t, err, errBlockNotFound)

	_, err = buffer.getBlockByNumber(5)
	assert.ErrorIs(t, err, errBlockNotFound)

	// Verify new block 3 exists (replaced the old one)
	block3, err := buffer.getBlockByNumber(3)
	require.NoError(t, err)
	assert.Equal(t, newHash3, block3.Hash())
	assert.Nil(t, block3.child)

	// Verify head is now block 3
	assert.Equal(t, uint64(3), buffer.head)
}

func TestEviction_BufferFull(t *testing.T) {
	buffer := NewChainWindow(5)
	tc := newTestChain()

	// Insert 5 blocks (fill buffer)
	for i := uint64(1); i <= 5; i++ {
		var parentHash common.Hash
		if i > 1 {
			parentHash = tc.getHash(i - 1)
		}
		data, _ := tc.createBlock(i, parentHash)
		err := buffer.InsertBlock(data)
		require.NoError(t, err)
	}

	assert.Equal(t, uint64(1), buffer.tail)
	assert.Equal(t, uint64(5), buffer.head)
	assert.Equal(t, 5, len(buffer.numToBlock))

	// Insert block 6 (should evict block 1)
	data, _ := tc.createBlock(6, tc.getHash(5))
	err := buffer.InsertBlock(data)
	require.NoError(t, err)

	// Verify block 1 is evicted
	assert.Equal(t, uint64(2), buffer.tail)
	assert.Equal(t, uint64(6), buffer.head)
	assert.Equal(t, 5, len(buffer.numToBlock))

	_, err = buffer.getBlockByNumber(1)
	assert.ErrorIs(t, err, errBlockNotFound)

	// Verify block 2's parent is nil (was evicted)
	block2, err := buffer.getBlockByNumber(2)
	require.NoError(t, err)
	assert.Nil(t, block2.parent)
}

func TestEviction_MultipleBlocks(t *testing.T) {
	buffer := NewChainWindow(5)
	tc := newTestChain()

	// Insert 10 blocks (will evict first 5)
	for i := uint64(1); i <= 10; i++ {
		var parentHash common.Hash
		if i > 1 {
			parentHash = tc.getHash(i - 1)
		}
		data, _ := tc.createBlock(i, parentHash)
		err := buffer.InsertBlock(data)
		require.NoError(t, err)
	}

	// Only last 5 blocks should remain
	assert.Equal(t, uint64(6), buffer.tail)
	assert.Equal(t, uint64(10), buffer.head)
	assert.Equal(t, 5, len(buffer.numToBlock))

	// Verify blocks 6-10 exist
	for i := uint64(6); i <= 10; i++ {
		block, err := buffer.getBlockByNumber(i)
		require.NoError(t, err)
		assert.Equal(t, i, block.Number())
	}
}

func TestGetBlockByHash(t *testing.T) {
	buffer := NewChainWindow(10)
	tc := newTestChain()

	// Insert blocks
	for i := uint64(1); i <= 5; i++ {
		var parentHash common.Hash
		if i > 1 {
			parentHash = tc.getHash(i - 1)
		}
		data, _ := tc.createBlock(i, parentHash)
		err := buffer.InsertBlock(data)
		require.NoError(t, err)
	}

	// Get by hash
	block, err := buffer.getBlockByHash(tc.getHash(3))
	require.NoError(t, err)
	assert.Equal(t, uint64(3), block.Number())
	assert.Equal(t, tc.getHash(3), block.Hash())

	// Get non-existent hash
	_, err = buffer.getBlockByHash(common.HexToHash("0xnonexistent"))
	assert.ErrorIs(t, err, errBlockNotFound)
}

func TestChainTraversal_Backwards(t *testing.T) {
	buffer := NewChainWindow(10)
	tc := newTestChain()

	// Insert blocks 1-5
	for i := uint64(1); i <= 5; i++ {
		var parentHash common.Hash
		if i > 1 {
			parentHash = tc.getHash(i - 1)
		}
		data, _ := tc.createBlock(i, parentHash)
		err := buffer.InsertBlock(data)
		require.NoError(t, err)
	}

	// Walk backwards from block 5 using parent pointers
	visited := []uint64{}
	block, err := buffer.getBlockByNumber(5)
	require.NoError(t, err)

	current := block
	for current != nil {
		visited = append(visited, current.Number())
		current = current.parent
	}

	assert.Equal(t, []uint64{5, 4, 3, 2, 1}, visited)
}

func TestChainTraversal_Forward(t *testing.T) {
	buffer := NewChainWindow(10)
	tc := newTestChain()

	// Insert blocks 1-5
	for i := uint64(1); i <= 5; i++ {
		var parentHash common.Hash
		if i > 1 {
			parentHash = tc.getHash(i - 1)
		}
		data, _ := tc.createBlock(i, parentHash)
		err := buffer.InsertBlock(data)
		require.NoError(t, err)
	}

	// Walk forward from block 2 using child pointers
	visited := []uint64{}
	block, err := buffer.getBlockByNumber(2)
	require.NoError(t, err)

	current := block
	for current != nil {
		visited = append(visited, current.Number())
		current = current.child
	}

	assert.Equal(t, []uint64{2, 3, 4, 5}, visited)
}

func TestIsFull(t *testing.T) {
	buffer := NewChainWindow(5)
	tc := newTestChain()

	assert.False(t, buffer.isFull())

	// Insert 4 blocks
	for i := uint64(1); i <= 4; i++ {
		var parentHash common.Hash
		if i > 1 {
			parentHash = tc.getHash(i - 1)
		}
		data, _ := tc.createBlock(i, parentHash)
		err := buffer.InsertBlock(data)
		require.NoError(t, err)
	}

	assert.False(t, buffer.isFull())

	// Insert 5th block
	data, _ := tc.createBlock(5, tc.getHash(4))
	err := buffer.InsertBlock(data)
	require.NoError(t, err)

	assert.True(t, buffer.isFull())
}

func TestBlockData_AllFields(t *testing.T) {
	buffer := NewChainWindow(10)

	accounts := map[common.Hash]*types.SlimAccount{
		common.HexToHash("0xacc1"): {
			Nonce:    uint64(1),
			Balance:  uint256.NewInt(1),
			Root:     []byte("root1"),
			CodeHash: []byte("code1"),
		},
	}
	storages := map[common.Hash]map[common.Hash]common.Hash{
		common.HexToHash("0xacc1"): {
			common.HexToHash("0xslot1"): common.BytesToHash([]byte("storage_1")),
		},
	}
	codes := map[common.Hash][]byte{
		common.HexToHash("0xcode1"): []byte("bytecode_1"),
	}

	header := &types.Header{Number: big.NewInt(1)}
	block := types.NewBlock(header, &types.Body{}, nil, nil)

	data := &BlockData{
		Block:    block,
		Accounts: accounts,
		Storages: storages,
		Codes:    codes,
	}

	err := buffer.InsertBlock(data)
	require.NoError(t, err)

	// Retrieve and verify all fields
	slot, err := buffer.getBlockByNumber(1)
	require.NoError(t, err)
	assert.Equal(t, accounts, slot.data.Accounts)
	assert.Equal(t, storages, slot.data.Storages)
	assert.Equal(t, codes, slot.data.Codes)
}

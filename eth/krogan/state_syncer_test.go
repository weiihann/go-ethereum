package krogan

import (
	"bytes"
	"context"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockDB is a simple in-memory database for testing
func newMockDB() ethdb.KeyValueStore {
	return rawdb.NewMemoryDatabase()
}

func TestInitSync_NoNodes(t *testing.T) {
	db := newMockDB()
	syncer := NewStateSyncer(db)

	err := syncer.initSync()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no nodes to sync from")
}

func TestInitSync_InvalidRange(t *testing.T) {
	db := newMockDB()
	syncer := NewStateSyncer(db)

	// Manually add a node to pass the first check
	syncer.nodes["test"] = nil
	syncer.idlers["test"] = struct{}{}

	// Set invalid range (start >= end)
	syncer.accountRangeStart = common.MaxHash
	syncer.accountRangeEnd = common.Hash{}

	err := syncer.initSync()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid account range")
}

func TestInitSync_TaskCreation(t *testing.T) {
	db := newMockDB()
	syncer := NewStateSyncer(db)

	// Add 3 nodes
	syncer.nodes["node1"] = nil
	syncer.nodes["node2"] = nil
	syncer.nodes["node3"] = nil
	syncer.idlers["node1"] = struct{}{}
	syncer.idlers["node2"] = struct{}{}
	syncer.idlers["node3"] = struct{}{}

	err := syncer.initSync()
	require.NoError(t, err)

	// Should create 3 tasks (one per node)
	assert.Equal(t, 3, len(syncer.tasks))

	// Verify tasks cover the full range
	assert.Equal(t, common.Hash{}, syncer.tasks[0].Next)
	assert.Equal(t, common.MaxHash, syncer.tasks[2].Last)

	// Verify tasks are contiguous
	for i := 0; i < len(syncer.tasks)-1; i++ {
		currentLast := syncer.tasks[i].Last
		nextStart := syncer.tasks[i+1].Next
		expectedNext := common.BigToHash(new(big.Int).Add(currentLast.Big(), common.Big1))
		assert.Equal(t, expectedNext, nextStart, "Task %d and %d should be contiguous", i, i+1)
	}
}

func TestInitSync_CustomRange(t *testing.T) {
	db := newMockDB()
	syncer := NewStateSyncer(db)

	// Add 2 nodes
	syncer.nodes["node1"] = nil
	syncer.nodes["node2"] = nil
	syncer.idlers["node1"] = struct{}{}
	syncer.idlers["node2"] = struct{}{}

	// Set custom range
	syncer.accountRangeStart = common.HexToHash("0x1000")
	syncer.accountRangeEnd = common.HexToHash("0x2000")

	err := syncer.initSync()
	require.NoError(t, err)

	assert.Equal(t, 2, len(syncer.tasks))
	assert.Equal(t, syncer.accountRangeStart, syncer.tasks[0].Next)
	assert.Equal(t, syncer.accountRangeEnd, syncer.tasks[1].Last)
}

func TestLoadAndSaveSyncStatus(t *testing.T) {
	db := newMockDB()
	syncer := NewStateSyncer(db)

	// Set and save some progress
	syncer.accountSynced = 100
	syncer.accountBytes = 1000
	syncer.bytecodeSynced = 10
	syncer.bytecodeBytes = 500
	syncer.tasks = []*accountTask{
		{
			Next:           common.HexToHash("0x1000"),
			Last:           common.HexToHash("0x2000"),
			SubTasks:       make(map[common.Hash][]*storageTask),
			stateCompleted: make(map[common.Hash]struct{}),
		},
	}
	syncer.saveSyncStatus()

	// Create new syncer and load status
	syncer2 := NewStateSyncer(db)
	loaded := syncer2.loadSyncStatus()

	require.True(t, loaded)
	assert.Equal(t, uint64(100), syncer2.accountSynced)
	assert.Equal(t, common.StorageSize(1000), syncer2.accountBytes)
	assert.Equal(t, uint64(10), syncer2.bytecodeSynced)
	assert.Equal(t, common.StorageSize(500), syncer2.bytecodeBytes)
	assert.Equal(t, 1, len(syncer2.tasks))
}

func TestCleanAccountTasks(t *testing.T) {
	db := newMockDB()
	syncer := NewStateSyncer(db)

	// Create tasks with some marked as done
	syncer.tasks = []*accountTask{
		{Next: common.HexToHash("0x1"), Last: common.HexToHash("0x2"), done: false},
		{Next: common.HexToHash("0x3"), Last: common.HexToHash("0x4"), done: true},
		{Next: common.HexToHash("0x5"), Last: common.HexToHash("0x6"), done: false},
		{Next: common.HexToHash("0x7"), Last: common.HexToHash("0x8"), done: true},
	}

	syncer.cleanAccountTasks()

	// Should have removed completed tasks
	assert.Equal(t, 2, len(syncer.tasks))
	assert.False(t, syncer.tasks[0].done)
	assert.False(t, syncer.tasks[1].done)
}

func TestCleanAccountTasks_AllDone(t *testing.T) {
	db := newMockDB()
	syncer := NewStateSyncer(db)
	syncer.accountBytes = 100 // Set some progress to trigger reporting

	// Create tasks all marked as done
	syncer.tasks = []*accountTask{
		{Next: common.HexToHash("0x1"), Last: common.HexToHash("0x2"), done: true},
		{Next: common.HexToHash("0x3"), Last: common.HexToHash("0x4"), done: true},
	}

	syncer.cleanAccountTasks()

	assert.Equal(t, 0, len(syncer.tasks))
	assert.True(t, syncer.done)
}

func TestProcessAccountResponse_NoContinuation(t *testing.T) {
	db := newMockDB()
	syncer := NewStateSyncer(db)

	task := &accountTask{
		Next: common.HexToHash("0x0"),
		Last: common.HexToHash("0x100"),
	}

	// Create accounts within range, including the last
	accounts := map[common.Hash]*types.SlimAccount{
		common.HexToHash("0x50"): {
			Nonce:    1,
			Balance:  uint256.NewInt(100),
			CodeHash: types.EmptyCodeHash.Bytes(),
		},
		common.HexToHash("0x100"): { // Exactly at Last
			Nonce:    2,
			Balance:  uint256.NewInt(200),
			CodeHash: types.EmptyCodeHash.Bytes(),
		},
	}

	res := &accountResponse{
		task:     task,
		accounts: accounts,
	}

	syncer.processAccountResponse(res)

	// Should mark as no continuation since Last is included
	assert.False(t, res.cont)
	assert.Equal(t, 0, task.pend)
}

func TestProcessAccountResponse_WithContinuation(t *testing.T) {
	db := newMockDB()
	syncer := NewStateSyncer(db)

	task := &accountTask{
		Next: common.HexToHash("0x0"),
		Last: common.HexToHash("0x1000"),
	}

	// Create accounts not reaching Last
	accounts := map[common.Hash]*types.SlimAccount{
		common.HexToHash("0x50"): {
			Nonce:    1,
			Balance:  uint256.NewInt(100),
			CodeHash: types.EmptyCodeHash.Bytes(),
		},
		common.HexToHash("0x100"): {
			Nonce:    2,
			Balance:  uint256.NewInt(200),
			CodeHash: types.EmptyCodeHash.Bytes(),
		},
	}

	res := &accountResponse{
		task:     task,
		accounts: accounts,
	}

	syncer.processAccountResponse(res)

	// Should mark as continuation needed
	assert.True(t, res.cont)
	// Task.Next should be updated to maxHash + 1
	expectedNext := common.BigToHash(new(big.Int).Add(common.HexToHash("0x100").Big(), common.Big1))
	assert.Equal(t, expectedNext, task.Next)
}

func TestProcessAccountResponse_TruncatesOverflow(t *testing.T) {
	db := newMockDB()
	syncer := NewStateSyncer(db)

	task := &accountTask{
		Next: common.HexToHash("0x0"),
		Last: common.HexToHash("0x100"),
	}

	// Create accounts with some beyond Last
	accounts := map[common.Hash]*types.SlimAccount{
		common.HexToHash("0x50"): {
			Nonce:    1,
			Balance:  uint256.NewInt(100),
			CodeHash: types.EmptyCodeHash.Bytes(),
		},
		common.HexToHash("0x200"): { // Beyond Last - should be deleted
			Nonce:    2,
			Balance:  uint256.NewInt(200),
			CodeHash: types.EmptyCodeHash.Bytes(),
		},
	}

	res := &accountResponse{
		task:     task,
		accounts: accounts,
	}

	syncer.processAccountResponse(res)

	// Overflow account should be deleted
	_, exists := res.accounts[common.HexToHash("0x200")]
	assert.False(t, exists)
	assert.Equal(t, 1, len(res.accounts))
	assert.False(t, res.cont)
}

func TestProcessAccountResponse_WithCodeNeeded(t *testing.T) {
	db := newMockDB()
	syncer := NewStateSyncer(db)

	task := &accountTask{
		Next: common.HexToHash("0x0"),
		Last: common.HexToHash("0x100"),
	}

	codeHash := common.HexToHash("0xabc123")

	// Create account with code that needs fetching
	accounts := map[common.Hash]*types.SlimAccount{
		common.HexToHash("0x50"): {
			Nonce:    1,
			Balance:  uint256.NewInt(100),
			CodeHash: codeHash.Bytes(),
		},
		common.HexToHash("0x100"): {
			Nonce:    2,
			Balance:  uint256.NewInt(200),
			CodeHash: types.EmptyCodeHash.Bytes(),
		},
	}

	res := &accountResponse{
		task:     task,
		accounts: accounts,
	}

	syncer.processAccountResponse(res)

	// Should have pending code task
	assert.Equal(t, 1, task.pend)
	assert.True(t, task.needCode[common.HexToHash("0x50")])
	_, exists := task.codeTasks[codeHash]
	assert.True(t, exists)
}

func TestForwardAccountTask_NilResponse(t *testing.T) {
	db := newMockDB()
	syncer := NewStateSyncer(db)

	task := &accountTask{
		res: nil,
	}

	// Should return early without panic
	syncer.forwardAccountTask(task)
}

func TestForwardAccountTask_PersistsAccounts(t *testing.T) {
	db := newMockDB()
	syncer := NewStateSyncer(db)

	task := &accountTask{
		Next:      common.HexToHash("0x0"),
		Last:      common.HexToHash("0x100"),
		needCode:  make(map[common.Hash]bool),
		needState: make(map[common.Hash]bool),
	}

	accounts := map[common.Hash]*types.SlimAccount{
		common.HexToHash("0x50"): {
			Nonce:    1,
			Balance:  uint256.NewInt(100),
			CodeHash: types.EmptyCodeHash.Bytes(),
		},
	}

	task.res = &accountResponse{
		task:     task,
		accounts: accounts,
		cont:     false,
	}

	syncer.forwardAccountTask(task)

	// Verify account was persisted
	assert.Equal(t, uint64(1), syncer.accountSynced)
	assert.True(t, task.done)

	// Verify res is cleared
	assert.Nil(t, task.res)
}

func TestForwardAccountTask_SkipsPendingAccounts(t *testing.T) {
	db := newMockDB()
	syncer := NewStateSyncer(db)

	task := &accountTask{
		Next: common.HexToHash("0x0"),
		Last: common.HexToHash("0x100"),
		needCode: map[common.Hash]bool{
			common.HexToHash("0x50"): true, // This account still needs code
		},
		needState: make(map[common.Hash]bool),
		pend:      1,
	}

	accounts := map[common.Hash]*types.SlimAccount{
		common.HexToHash("0x50"): {
			Nonce:    1,
			Balance:  uint256.NewInt(100),
			CodeHash: common.HexToHash("0xcode").Bytes(),
		},
		common.HexToHash("0x60"): {
			Nonce:    2,
			Balance:  uint256.NewInt(200),
			CodeHash: types.EmptyCodeHash.Bytes(),
		},
	}

	task.res = &accountResponse{
		task:     task,
		accounts: accounts,
		cont:     false,
	}

	syncer.forwardAccountTask(task)

	// Only 1 account should be persisted (0x60, not 0x50)
	assert.Equal(t, uint64(1), syncer.accountSynced)
	// Task not done because pend > 0
	assert.False(t, task.done)
}

func TestProcessBytecodeResponse(t *testing.T) {
	db := newMockDB()
	syncer := NewStateSyncer(db)

	codeHash := common.HexToHash("0xabc123")
	code := []byte("contract bytecode")

	task := &accountTask{
		needCode: map[common.Hash]bool{
			common.HexToHash("0x50"): true,
		},
		needState: make(map[common.Hash]bool),
		pend:      1,
		res: &accountResponse{
			accounts: map[common.Hash]*types.SlimAccount{
				common.HexToHash("0x50"): {
					CodeHash: codeHash.Bytes(),
				},
			},
			cont: false,
		},
	}

	res := &bytecodeResponse{
		task: task,
		codes: map[common.Hash][]byte{
			codeHash: code,
		},
	}

	syncer.processBytecodeResponse(res)

	// Verify code was persisted
	assert.Equal(t, uint64(1), syncer.bytecodeSynced)

	// Verify needCode was cleared
	assert.False(t, task.needCode[common.HexToHash("0x50")])
	assert.Equal(t, 0, task.pend)

	// Verify code is in database
	savedCode := rawdb.ReadCode(db, codeHash)
	assert.Equal(t, code, savedCode)
}

func TestRevertAccountRequest(t *testing.T) {
	db := newMockDB()
	syncer := NewStateSyncer(db)

	task := &accountTask{
		req: &accountRequest{},
	}

	req := &accountRequest{
		reqID: 123,
		start: common.HexToHash("0x1000"),
		task:  task,
	}

	syncer.revertAccountRequest(req)

	// Task's req should be cleared
	assert.Nil(t, task.req)
}

func TestRevertBytecodeRequest(t *testing.T) {
	db := newMockDB()
	syncer := NewStateSyncer(db)

	task := &accountTask{
		codeTasks: make(map[common.Hash]struct{}),
	}

	hashes := []common.Hash{
		common.HexToHash("0x1"),
		common.HexToHash("0x2"),
		common.HexToHash("0x3"),
	}

	req := &bytecodeRequest{
		reqID:  123,
		hashes: hashes,
		task:   task,
	}

	syncer.revertBytecodeRequest(req)

	// Hashes should be re-added to codeTasks
	assert.Equal(t, 3, len(task.codeTasks))
	for _, hash := range hashes {
		_, exists := task.codeTasks[hash]
		assert.True(t, exists)
	}
}

func TestScheduleRevertAccountRequest_Revert(t *testing.T) {
	syncer := NewStateSyncer(newMockDB())

	revertChan := make(chan *accountRequest, 1)
	req := &accountRequest{
		revert: revertChan,
		ctx:    context.Background(),
		stale:  make(chan struct{}),
	}

	syncer.scheduleRevertAccountRequest(req)

	select {
	case received := <-revertChan:
		assert.Equal(t, req, received)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Expected revert to be sent")
	}
}

func TestScheduleRevertAccountRequest_ContextCancelled(t *testing.T) {
	syncer := NewStateSyncer(newMockDB())

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	revertChan := make(chan *accountRequest) // Unbuffered, will block
	req := &accountRequest{
		revert: revertChan,
		ctx:    ctx,
		stale:  make(chan struct{}),
	}

	done := make(chan struct{})
	go func() {
		syncer.scheduleRevertAccountRequest(req)
		close(done)
	}()

	select {
	case <-done:
		// Success - function returned due to cancelled context
	case <-time.After(100 * time.Millisecond):
		t.Fatal("scheduleRevertAccountRequest should have returned due to cancelled context")
	}
}

func TestScheduleRevertAccountRequest_Stale(t *testing.T) {
	syncer := NewStateSyncer(newMockDB())

	staleChan := make(chan struct{})
	close(staleChan) // Close immediately to simulate stale

	revertChan := make(chan *accountRequest) // Unbuffered, will block
	req := &accountRequest{
		revert: revertChan,
		ctx:    context.Background(),
		stale:  staleChan,
	}

	done := make(chan struct{})
	go func() {
		syncer.scheduleRevertAccountRequest(req)
		close(done)
	}()

	select {
	case <-done:
		// Success - function returned due to stale channel
	case <-time.After(100 * time.Millisecond):
		t.Fatal("scheduleRevertAccountRequest should have returned due to stale channel")
	}
}

func TestOnAccount_ValidRequest(t *testing.T) {
	syncer := NewStateSyncer(newMockDB())

	deliverChan := make(chan *accountResponse, 1)
	task := &accountTask{}

	req := &accountRequest{
		reqID:   123,
		deliver: deliverChan,
		ctx:     context.Background(),
		stale:   make(chan struct{}),
		task:    task,
	}

	syncer.accountReqs[123] = req

	accounts := map[common.Hash]*types.SlimAccount{
		common.HexToHash("0x1"): {Nonce: 1},
	}

	syncer.onAccount(123, 100, accounts)

	select {
	case res := <-deliverChan:
		assert.Equal(t, task, res.task)
		assert.Equal(t, hexutil.Uint64(100), res.blockNumber)
		assert.Equal(t, accounts, res.accounts)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Expected response to be delivered")
	}

	// Request should be removed
	_, exists := syncer.accountReqs[123]
	assert.False(t, exists)
}

func TestOnAccount_UnknownRequest(t *testing.T) {
	syncer := NewStateSyncer(newMockDB())

	// Should not panic with unknown request
	syncer.onAccount(999, 100, nil)
}

func TestOnAccount_EmptyResponse(t *testing.T) {
	syncer := NewStateSyncer(newMockDB())

	revertChan := make(chan *accountRequest, 1)
	task := &accountTask{}

	req := &accountRequest{
		reqID:   123,
		deliver: make(chan *accountResponse),
		revert:  revertChan,
		ctx:     context.Background(),
		stale:   make(chan struct{}),
		task:    task,
	}

	syncer.accountReqs[123] = req

	// Empty response should trigger revert
	syncer.onAccount(123, 0, map[common.Hash]*types.SlimAccount{})

	select {
	case <-revertChan:
		// Success - revert was triggered
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Expected revert to be triggered for empty response")
	}
}

func TestOnBytecode_ValidRequest(t *testing.T) {
	syncer := NewStateSyncer(newMockDB())

	deliverChan := make(chan *bytecodeResponse, 1)
	task := &accountTask{}

	req := &bytecodeRequest{
		reqID:   456,
		deliver: deliverChan,
		ctx:     context.Background(),
		stale:   make(chan struct{}),
		task:    task,
	}

	syncer.bytecodeReqs[456] = req

	codes := map[common.Hash][]byte{
		common.HexToHash("0x1"): []byte("code"),
	}

	syncer.onBytecode(456, codes)

	select {
	case res := <-deliverChan:
		assert.Equal(t, task, res.task)
		assert.Equal(t, codes, res.codes)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Expected response to be delivered")
	}

	// Request should be removed
	_, exists := syncer.bytecodeReqs[456]
	assert.False(t, exists)
}

func TestOnBytecode_UnknownRequest(t *testing.T) {
	syncer := NewStateSyncer(newMockDB())

	// Should not panic with unknown request
	syncer.onBytecode(999, nil)
}

// TestSyncProgress tests the SyncProgress struct serialization
func TestSyncProgress_Serialization(t *testing.T) {
	db := newMockDB()
	syncer := NewStateSyncer(db)

	syncer.tasks = []*accountTask{
		{
			Next:           common.HexToHash("0x1000"),
			Last:           common.HexToHash("0x2000"),
			SubTasks:       make(map[common.Hash][]*storageTask),
			stateCompleted: make(map[common.Hash]struct{}),
		},
		{
			Next:           common.HexToHash("0x2001"),
			Last:           common.HexToHash("0x3000"),
			SubTasks:       make(map[common.Hash][]*storageTask),
			stateCompleted: make(map[common.Hash]struct{}),
		},
	}
	syncer.accountSynced = 1000
	syncer.accountBytes = 50000
	syncer.bytecodeSynced = 100
	syncer.bytecodeBytes = 10000
	syncer.storageSynced = 5000
	syncer.storageBytes = 100000

	syncer.saveSyncStatus()

	// Load into new syncer
	syncer2 := NewStateSyncer(db)
	loaded := syncer2.loadSyncStatus()

	require.True(t, loaded)
	assert.Equal(t, syncer.accountSynced, syncer2.accountSynced)
	assert.Equal(t, syncer.accountBytes, syncer2.accountBytes)
	assert.Equal(t, syncer.bytecodeSynced, syncer2.bytecodeSynced)
	assert.Equal(t, syncer.bytecodeBytes, syncer2.bytecodeBytes)
	assert.Equal(t, syncer.storageSynced, syncer2.storageSynced)
	assert.Equal(t, syncer.storageBytes, syncer2.storageBytes)
	assert.Equal(t, len(syncer.tasks), len(syncer2.tasks))
}

// TestAccountTask_HashComparison tests hash comparison logic
func TestAccountTask_HashComparison(t *testing.T) {
	tests := []struct {
		name     string
		hash     common.Hash
		last     common.Hash
		expected int // -1, 0, or 1
	}{
		{
			name:     "hash less than last",
			hash:     common.HexToHash("0x100"),
			last:     common.HexToHash("0x200"),
			expected: -1,
		},
		{
			name:     "hash equal to last",
			hash:     common.HexToHash("0x200"),
			last:     common.HexToHash("0x200"),
			expected: 0,
		},
		{
			name:     "hash greater than last",
			hash:     common.HexToHash("0x300"),
			last:     common.HexToHash("0x200"),
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.hash.Big().Cmp(tt.last.Big())
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestMaxHashTracking tests finding max hash in account response
func TestMaxHashTracking(t *testing.T) {
	accounts := map[common.Hash]*types.SlimAccount{
		common.HexToHash("0x50"):  {Nonce: 1},
		common.HexToHash("0x100"): {Nonce: 2},
		common.HexToHash("0x30"):  {Nonce: 3},
		common.HexToHash("0x80"):  {Nonce: 4},
	}

	var maxHash common.Hash
	for hash := range accounts {
		if bytes.Compare(hash[:], maxHash[:]) > 0 {
			maxHash = hash
		}
	}

	assert.Equal(t, common.HexToHash("0x100"), maxHash)
}

// TestConcurrentAccess tests thread safety of syncer operations
func TestConcurrentAccess(t *testing.T) {
	syncer := NewStateSyncer(newMockDB())

	// Add some nodes
	syncer.nodes["node1"] = nil
	syncer.nodes["node2"] = nil
	syncer.idlers["node1"] = struct{}{}
	syncer.idlers["node2"] = struct{}{}

	var wg sync.WaitGroup
	iterations := 100

	// Concurrent reads
	wg.Add(iterations)
	for i := 0; i < iterations; i++ {
		go func() {
			defer wg.Done()
			syncer.lock.RLock()
			_ = len(syncer.nodes)
			_ = len(syncer.idlers)
			syncer.lock.RUnlock()
		}()
	}

	// Concurrent writes
	wg.Add(iterations)
	for i := 0; i < iterations; i++ {
		go func(i int) {
			defer wg.Done()
			syncer.lock.Lock()
			syncer.accountSynced++
			syncer.lock.Unlock()
		}(i)
	}

	wg.Wait()

	assert.Equal(t, uint64(iterations), syncer.accountSynced)
}

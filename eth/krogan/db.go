package krogan

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie/utils"
	"github.com/ethereum/go-ethereum/triedb"
)

const (
	// Number of codehash->size associations to keep.
	codeSizeCacheSize = 1_000_000 // 4 megabytes in total

	// Cache size granted for caching clean code.
	codeCacheSize = 256 * 1024 * 1024
)

var (
	ErrDBFunctionNotSupported = errors.New("db function not supported by krogan")
)

// Database key prefixes
var (
	accountPrefix    = []byte("account:")
	storagePrefix    = []byte("storage:")
	codePrefix       = []byte("code:")
	metadataPrefix   = []byte("meta:")
	checkpointPrefix = []byte("checkpoint:")
)

// Metadata keys
var (
	syncBaseBlockKey     = []byte("syncBaseBlock")
	syncCompleteKey      = []byte("syncComplete")
	lastAccountHashKey   = []byte("lastAccountHash")
	syncStartBlockKey    = []byte("syncStartBlock")
	accountRangeStartKey = []byte("accountRangeStart")
	accountRangeEndKey   = []byte("accountRangeEnd")
)

// Key generation helpers
func accountKey(addrHash common.Hash) []byte {
	return append(accountPrefix, addrHash.Bytes()...)
}

func storageKey(addrHash, slotHash common.Hash) []byte {
	return append(append(storagePrefix, addrHash.Bytes()...), slotHash.Bytes()...)
}

func codeKey(codeHash common.Hash) []byte {
	return append(codePrefix, codeHash.Bytes()...)
}

func metadataKey(key []byte) []byte {
	return append(metadataPrefix, key...)
}

func checkpointKey(key []byte) []byte {
	return append(checkpointPrefix, key...)
}

type KroganDB struct {
	disk  ethdb.KeyValueStore
	chain *ChainWindow

	codeCache     *lru.SizeConstrainedCache[common.Hash, []byte]
	codeSizeCache *lru.Cache[common.Hash, int]

	// Sync state tracking
	syncBaseBlock atomic.Uint64
}

func NewKroganDB(chain *ChainWindow, disk ethdb.KeyValueStore) *KroganDB {
	return &KroganDB{
		disk:          disk,
		chain:         chain,
		codeCache:     lru.NewSizeConstrainedCache[common.Hash, []byte](codeCacheSize),
		codeSizeCache: lru.NewCache[common.Hash, int](codeSizeCacheSize),
	}
}

func (k *KroganDB) Reader(root common.Hash) (state.Reader, error) {
	return newReader(k.chain, k.disk, k.codeCache, k.codeSizeCache), nil
}

func (k *KroganDB) InsertBlock(data *BlockData) error {
	return k.chain.InsertBlock(data)
}

func (k *KroganDB) OpenTrie(root common.Hash) (state.Trie, error) {
	return nil, ErrDBFunctionNotSupported
}

func (k *KroganDB) OpenStorageTrie(stateRoot common.Hash, address common.Address, root common.Hash, trie state.Trie) (state.Trie, error) {
	return nil, ErrDBFunctionNotSupported
}

func (k *KroganDB) PointCache() *utils.PointCache {
	panic(ErrDBFunctionNotSupported)
}

func (k *KroganDB) TrieDB() *triedb.Database {
	panic(ErrDBFunctionNotSupported)
}

func (k *KroganDB) Snapshot() *snapshot.Tree {
	panic(ErrDBFunctionNotSupported)
}

// WriteAccount writes an account to disk. RLP-encoded account data.
func (k *KroganDB) WriteAccount(addrHash common.Hash, accountRLP []byte) error {
	return k.disk.Put(accountKey(addrHash), accountRLP)
}

// ReadAccount reads an account from disk. Returns RLP-encoded account data.
// Returns empty bytes if not found.
func (k *KroganDB) ReadAccount(addrHash common.Hash) ([]byte, error) {
	data, err := k.disk.Get(accountKey(addrHash))
	if err != nil {
		return nil, nil // Key not found, return empty
	}
	return data, nil
}

// WriteAccountAtBlock writes an account with block number check to avoid overwriting newer state.
// Only writes if blockNum >= syncBaseBlock.
func (k *KroganDB) WriteAccountAtBlock(addrHash common.Hash, accountRLP []byte, blockNum uint64) error {
	if blockNum < k.syncBaseBlock.Load() {
		// Skip writing older state
		return nil
	}
	return k.WriteAccount(addrHash, accountRLP)
}

// WriteStorage writes a storage slot to disk. RLP-encoded storage value.
func (k *KroganDB) WriteStorage(addrHash, slotHash common.Hash, valueRLP []byte) error {
	return k.disk.Put(storageKey(addrHash, slotHash), valueRLP)
}

// ReadStorage reads a storage slot from disk. Returns RLP-encoded storage value.
// Returns empty bytes if not found.
func (k *KroganDB) ReadStorage(addrHash, slotHash common.Hash) ([]byte, error) {
	data, err := k.disk.Get(storageKey(addrHash, slotHash))
	if err != nil {
		return nil, nil // Key not found, return empty
	}
	return data, nil
}

// WriteStorageAtBlock writes a storage slot with block number check.
// Only writes if blockNum >= syncBaseBlock.
func (k *KroganDB) WriteStorageAtBlock(addrHash, slotHash common.Hash, valueRLP []byte, blockNum uint64) error {
	if blockNum < k.syncBaseBlock.Load() {
		// Skip writing older state
		return nil
	}
	return k.WriteStorage(addrHash, slotHash, valueRLP)
}

// WriteCode writes contract bytecode to disk.
func (k *KroganDB) WriteCode(codeHash common.Hash, code []byte) error {
	if err := k.disk.Put(codeKey(codeHash), code); err != nil {
		return fmt.Errorf("failed to write code: %w", err)
	}

	// Update caches
	k.codeCache.Add(codeHash, code)
	k.codeSizeCache.Add(codeHash, len(code))
	return nil
}

// ReadCode reads contract bytecode from disk or cache.
// Returns empty bytes if not found.
func (k *KroganDB) ReadCode(codeHash common.Hash) ([]byte, error) {
	// Check cache first
	if code, ok := k.codeCache.Get(codeHash); ok {
		return code, nil
	}

	// Read from disk
	code, err := k.disk.Get(codeKey(codeHash))
	if err != nil {
		return nil, nil // Key not found, return empty
	}

	// Update caches
	k.codeCache.Add(codeHash, code)
	k.codeSizeCache.Add(codeHash, len(code))
	return code, nil
}

// GetSyncBaseBlock returns the current sync base block number.
func (k *KroganDB) GetSyncBaseBlock() uint64 {
	return k.syncBaseBlock.Load()
}

// SetSyncBaseBlock updates the sync base block number atomically and persists it.
func (k *KroganDB) SetSyncBaseBlock(blockNum uint64) error {
	k.syncBaseBlock.Store(blockNum)
	return k.writeMetadataUint64(syncBaseBlockKey, blockNum)
}

// LoadSyncBaseBlock loads the sync base block from disk on startup.
// Returns 0 if not found.
func (k *KroganDB) LoadSyncBaseBlock() (uint64, error) {
	blockNum, err := k.readMetadataUint64(syncBaseBlockKey)
	if err != nil {
		return 0, nil // Key not found, return 0
	}
	k.syncBaseBlock.Store(blockNum)
	return blockNum, nil
}

// UpdateCheckpoint saves sync checkpoint (last synced account hash and block number).
func (k *KroganDB) UpdateCheckpoint(lastAccountHash common.Hash, blockNum uint64) error {
	batch := k.disk.NewBatch()

	// Write last account hash
	if err := batch.Put(checkpointKey(lastAccountHashKey), lastAccountHash.Bytes()); err != nil {
		return fmt.Errorf("failed to write checkpoint account hash: %w", err)
	}

	// Write checkpoint block number
	blockBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(blockBytes, blockNum)
	if err := batch.Put(checkpointKey(syncStartBlockKey), blockBytes); err != nil {
		return fmt.Errorf("failed to write checkpoint block: %w", err)
	}

	return batch.Write()
}

// LoadCheckpoint loads the last checkpoint from disk.
// Returns zero values if not found.
func (k *KroganDB) LoadCheckpoint() (lastAccountHash common.Hash, blockNum uint64, err error) {
	// Read last account hash
	hashBytes, err := k.disk.Get(checkpointKey(lastAccountHashKey))
	if err != nil {
		return common.Hash{}, 0, nil // Key not found, return zero values
	}
	lastAccountHash = common.BytesToHash(hashBytes)

	// Read checkpoint block number
	blockBytes, err := k.disk.Get(checkpointKey(syncStartBlockKey))
	if err != nil {
		return lastAccountHash, 0, nil // Key not found, return partial data
	}
	blockNum = binary.BigEndian.Uint64(blockBytes)

	return lastAccountHash, blockNum, nil
}

// SetSyncComplete marks the initial state sync as complete.
func (k *KroganDB) SetSyncComplete(finalizedBlock uint64) error {
	batch := k.disk.NewBatch()

	// Mark sync complete
	if err := batch.Put(metadataKey(syncCompleteKey), []byte{1}); err != nil {
		return fmt.Errorf("failed to write sync complete flag: %w", err)
	}

	// Store finalized block
	if err := k.SetSyncBaseBlock(finalizedBlock); err != nil {
		return err
	}

	return batch.Write()
}

// IsSyncComplete returns true if initial state sync has completed.
// Returns false if not found.
func (k *KroganDB) IsSyncComplete() (bool, error) {
	data, err := k.disk.Get(metadataKey(syncCompleteKey))
	if err != nil {
		return false, nil // Key not found, return false
	}
	return len(data) > 0 && data[0] == 1, nil
}

// SaveAccountRange stores the configured account range for this node.
func (k *KroganDB) SaveAccountRange(start, end common.Hash) error {
	batch := k.disk.NewBatch()

	if err := batch.Put(metadataKey(accountRangeStartKey), start.Bytes()); err != nil {
		return fmt.Errorf("failed to write account range start: %w", err)
	}

	if err := batch.Put(metadataKey(accountRangeEndKey), end.Bytes()); err != nil {
		return fmt.Errorf("failed to write account range end: %w", err)
	}

	return batch.Write()
}

// LoadAccountRange loads the configured account range from disk.
// Returns zero values if not found.
func (k *KroganDB) LoadAccountRange() (start, end common.Hash, err error) {
	startBytes, err := k.disk.Get(metadataKey(accountRangeStartKey))
	if err != nil {
		return common.Hash{}, common.Hash{}, nil // Key not found, return zero values
	}
	start = common.BytesToHash(startBytes)

	endBytes, err := k.disk.Get(metadataKey(accountRangeEndKey))
	if err != nil {
		return start, common.Hash{}, nil // Key not found, return partial data
	}
	end = common.BytesToHash(endBytes)

	return start, end, nil
}

// writeMetadataUint64 is a helper to write uint64 metadata.
func (k *KroganDB) writeMetadataUint64(key []byte, value uint64) error {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, value)
	return k.disk.Put(metadataKey(key), bytes)
}

// readMetadataUint64 is a helper to read uint64 metadata.
// Returns 0 if not found.
func (k *KroganDB) readMetadataUint64(key []byte) (uint64, error) {
	bytes, err := k.disk.Get(metadataKey(key))
	if err != nil {
		return 0, err // Key not found, return 0
	}
	if len(bytes) != 8 {
		return 0, fmt.Errorf("invalid metadata uint64 length: %d", len(bytes))
	}
	return binary.BigEndian.Uint64(bytes), nil
}

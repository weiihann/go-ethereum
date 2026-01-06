// Copyright 2025 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package live

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/tracers"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/holiman/uint256"
)

func init() {
	tracers.LiveDirectory.Register("statesize", newStateSizeTracer)
}

const (
	// Key format: 8-byte block number (big-endian) + 32-byte state root
	keySize = 8 + 32

	// Batch size for persisting updates
	batchSize = 10000
)

// Metadata key prefix (uses 0xFF to sort after all block keys)
var (
	metaKeyProgress = []byte{0xFF, 'p', 'r', 'o', 'g', 'r', 'e', 's', 's'}
)

// progressData stores the last persisted state for integrity checking.
type progressData struct {
	BlockNumber uint64
	StateRoot   common.Hash
}

// stateSizeTracerConfig is the configuration for the statesize tracer.
type stateSizeTracerConfig struct {
	// Path is the directory where the Pebble database will be stored.
	Path string `json:"path"`
}

// StateUpdateRLP is the RLP-serializable version of tracing.StateUpdate.
// RLP doesn't support maps directly, so we convert to slices.
type StateUpdateRLP struct {
	OriginRoot  common.Hash
	Root        common.Hash
	BlockNumber uint64

	AccountChanges []AccountChangeEntry
	StorageChanges []StorageChangeEntry
	CodeChanges    []CodeChangeEntry
	TrieChanges    []TrieChangeEntry
}

// AccountChangeEntry represents a single account change for RLP encoding.
type AccountChangeEntry struct {
	Address common.Address
	Prev    *StateAccountRLP `rlp:"nil"`
	New     *StateAccountRLP `rlp:"nil"`
}

// StateAccountRLP is an RLP-compatible version of types.StateAccount.
type StateAccountRLP struct {
	Nonce    uint64
	Balance  *uint256.Int
	Root     common.Hash
	CodeHash []byte
}

// StorageChangeEntry represents storage changes for a single address.
type StorageChangeEntry struct {
	Address common.Address
	Slots   []SlotChange
}

// SlotChange represents a single storage slot change.
type SlotChange struct {
	Key  common.Hash
	Prev common.Hash
	New  common.Hash
}

// CodeChangeEntry represents a code change for RLP encoding.
type CodeChangeEntry struct {
	Address common.Address
	Prev    *ContractCodeRLP `rlp:"nil"`
	New     *ContractCodeRLP `rlp:"nil"`
}

// ContractCodeRLP is an RLP-compatible version of tracing.ContractCode.
type ContractCodeRLP struct {
	Hash   common.Hash
	Code   []byte
	Exists bool
}

// TrieChangeEntry represents trie changes for a single owner.
type TrieChangeEntry struct {
	Owner common.Hash
	Nodes []TrieNodeEntry
}

// TrieNodeEntry represents a single trie node change.
type TrieNodeEntry struct {
	Path string
	Prev *TrieNodeRLP `rlp:"nil"`
	New  *TrieNodeRLP `rlp:"nil"`
}

// TrieNodeRLP is an RLP-compatible version of trienode.Node.
type TrieNodeRLP struct {
	Blob []byte
	Hash common.Hash
}

// pendingUpdate holds an update waiting to be persisted.
type pendingUpdate struct {
	key  []byte
	data []byte
}

type stateSizeTracer struct {
	mu sync.Mutex
	db *pebble.DB

	// Progress tracking
	progress      *progressData // last persisted progress (nil if empty DB)
	firstUpdate   bool          // true until first update is validated
	pendingCount  uint64        // number of updates in pending batch
	pendingBuffer []pendingUpdate

	// Last update in current batch (for progress tracking)
	lastBatchBlock uint64
	lastBatchRoot  common.Hash

	closeOnce sync.Once
}

func newStateSizeTracer(cfg json.RawMessage) (*tracing.Hooks, error) {
	var config stateSizeTracerConfig
	if err := json.Unmarshal(cfg, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}
	if config.Path == "" {
		return nil, errors.New("statesize tracer path is required")
	}

	// Open Pebble database with Zstd compression for best compression ratio
	opts := &pebble.Options{
		// Use Zstd compression at all levels for maximum compression
		Levels: []pebble.LevelOptions{
			{TargetFileSize: 2 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10), Compression: sstable.ZstdCompression},
			{TargetFileSize: 4 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10), Compression: sstable.ZstdCompression},
			{TargetFileSize: 8 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10), Compression: sstable.ZstdCompression},
			{TargetFileSize: 16 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10), Compression: sstable.ZstdCompression},
			{TargetFileSize: 32 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10), Compression: sstable.ZstdCompression},
			{TargetFileSize: 64 * 1024 * 1024, FilterPolicy: bloom.FilterPolicy(10), Compression: sstable.ZstdCompression},
			{TargetFileSize: 128 * 1024 * 1024, Compression: sstable.ZstdCompression},
		},
		// Moderate cache size - state updates are write-heavy
		Cache: pebble.NewCache(128 * 1024 * 1024), // 128MB cache
	}

	db, err := pebble.Open(config.Path, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	t := &stateSizeTracer{
		db:            db,
		firstUpdate:   true,
		pendingBuffer: make([]pendingUpdate, 0, batchSize),
	}

	// Load progress from database
	progress, err := t.loadProgress()
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to load progress: %w", err)
	}
	t.progress = progress

	if progress != nil {
		log.Info("Statesize tracer opened existing database",
			"path", config.Path,
			"lastBlock", progress.BlockNumber,
			"lastRoot", progress.StateRoot.Hex())
	} else {
		log.Info("Statesize tracer created new database", "path", config.Path)
	}

	return &tracing.Hooks{
		OnStateUpdate: t.onStateUpdate,
		OnClose:       t.onClose,
	}, nil
}

// makeKey creates a database key from block number and state root.
// Key format: 8-byte block number (big-endian) + 32-byte state root
// Big-endian ensures lexicographic ordering matches numeric ordering.
func makeKey(blockNumber uint64, stateRoot common.Hash) []byte {
	key := make([]byte, keySize)
	binary.BigEndian.PutUint64(key[:8], blockNumber)
	copy(key[8:], stateRoot[:])
	return key
}

// parseKey extracts block number and state root from a database key.
func parseKey(key []byte) (uint64, common.Hash) {
	blockNumber := binary.BigEndian.Uint64(key[:8])
	var stateRoot common.Hash
	copy(stateRoot[:], key[8:])
	return blockNumber, stateRoot
}

// loadProgress loads the persisted progress from the database.
func (t *stateSizeTracer) loadProgress() (*progressData, error) {
	data, closer, err := t.db.Get(metaKeyProgress)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil // No progress yet
		}
		return nil, err
	}
	defer closer.Close()

	var progress progressData
	if err := rlp.DecodeBytes(data, &progress); err != nil {
		return nil, fmt.Errorf("failed to decode progress: %w", err)
	}
	return &progress, nil
}

// saveProgress saves the current progress to the database within a batch.
func (t *stateSizeTracer) saveProgress(batch *pebble.Batch, blockNumber uint64, stateRoot common.Hash) error {
	progress := progressData{
		BlockNumber: blockNumber,
		StateRoot:   stateRoot,
	}
	data, err := rlp.EncodeToBytes(&progress)
	if err != nil {
		return err
	}
	return batch.Set(metaKeyProgress, data, nil)
}

// lookupStateRoot checks if a state root exists for a given block number.
func (t *stateSizeTracer) lookupStateRoot(blockNumber uint64, stateRoot common.Hash) (bool, error) {
	key := makeKey(blockNumber, stateRoot)
	_, closer, err := t.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	closer.Close()
	return true, nil
}

// flushBatch persists all pending updates and updates progress.
func (t *stateSizeTracer) flushBatch() error {
	if len(t.pendingBuffer) == 0 {
		return nil
	}

	batch := t.db.NewBatch()
	defer batch.Close()

	// Write all pending updates
	for _, update := range t.pendingBuffer {
		if err := batch.Set(update.key, update.data, nil); err != nil {
			return fmt.Errorf("failed to add update to batch: %w", err)
		}
	}

	// Update progress
	if err := t.saveProgress(batch, t.lastBatchBlock, t.lastBatchRoot); err != nil {
		return fmt.Errorf("failed to save progress: %w", err)
	}

	// Commit with sync for durability
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}

	// Update in-memory progress
	t.progress = &progressData{
		BlockNumber: t.lastBatchBlock,
		StateRoot:   t.lastBatchRoot,
	}

	log.Info("Persisted state updates batch",
		"count", len(t.pendingBuffer),
		"lastBlock", t.lastBatchBlock,
		"lastRoot", t.lastBatchRoot.Hex())

	// Clear pending buffer
	t.pendingBuffer = t.pendingBuffer[:0]
	t.pendingCount = 0

	return nil
}

func (t *stateSizeTracer) onStateUpdate(update *tracing.StateUpdate) {
	if update == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// First update validation: check data integrity
	if t.firstUpdate {
		t.firstUpdate = false

		if t.progress != nil {
			// We have existing data - validate continuity
			// The incoming update's OriginRoot should match our last persisted StateRoot
			// OR the incoming block should be building on an existing record

			// Check if parent state exists in DB
			parentExists, err := t.lookupStateRoot(update.BlockNumber-1, update.OriginRoot)
			if err != nil {
				log.Crit("Failed to lookup parent state",
					"block", update.BlockNumber,
					"originRoot", update.OriginRoot.Hex(),
					"err", err)
				os.Exit(1)
			}

			if !parentExists {
				// Check if this is a reorg starting from an earlier block
				if update.BlockNumber <= t.progress.BlockNumber {
					// This is acceptable - it's a reorg, we'll handle it below
					log.Warn("Detected reorg on restart",
						"progressBlock", t.progress.BlockNumber,
						"incomingBlock", update.BlockNumber)
				} else {
					// Data corruption: we have progress but parent doesn't exist
					log.Crit("Data corruption detected: parent state not found in database",
						"incomingBlock", update.BlockNumber,
						"originRoot", update.OriginRoot.Hex(),
						"lastPersistedBlock", t.progress.BlockNumber,
						"lastPersistedRoot", t.progress.StateRoot.Hex())
					os.Exit(1)
				}
			}
		}
	}

	// Validate block continuity
	if t.progress != nil {
		expectedBlock := t.progress.BlockNumber + 1 + t.pendingCount
		if update.BlockNumber != expectedBlock {
			if update.BlockNumber > expectedBlock {
				log.Crit("Block gap detected (missing blocks)",
					"expected", expectedBlock,
					"got", update.BlockNumber)
				os.Exit(1)
			}
			// Block is earlier than expected - should have been handled by reorg above
			log.Crit("Unexpected block number",
				"expected", expectedBlock,
				"got", update.BlockNumber)
			os.Exit(1)
		}
	}

	// Convert and encode the update
	msg := convertStateUpdateToRLP(update)
	data, err := rlp.EncodeToBytes(msg)
	if err != nil {
		log.Crit("Failed to RLP encode state update", "block", update.BlockNumber, "err", err)
		os.Exit(1)
	}

	// Add to pending buffer
	key := makeKey(update.BlockNumber, update.Root)
	t.pendingBuffer = append(t.pendingBuffer, pendingUpdate{
		key:  key,
		data: data,
	})
	t.pendingCount++
	t.lastBatchBlock = update.BlockNumber
	t.lastBatchRoot = update.Root

	// Check if we should flush
	if t.pendingCount >= batchSize {
		if err := t.flushBatch(); err != nil {
			log.Crit("Failed to flush batch", "err", err)
			os.Exit(1)
		}
	}

	// Log progress periodically
	totalProcessed := uint64(0)
	if t.progress != nil {
		totalProcessed = t.progress.BlockNumber + 1
	}
	totalProcessed += t.pendingCount
	if totalProcessed%batchSize == 0 {
		log.Info("State updates progress",
			"processed", totalProcessed,
			"pending", t.pendingCount,
			"lastBlock", update.BlockNumber)
	}
}

// convertStateUpdateToRLP converts a tracing.StateUpdate to RLP-serializable format.
func convertStateUpdateToRLP(update *tracing.StateUpdate) *StateUpdateRLP {
	msg := &StateUpdateRLP{
		OriginRoot:  update.OriginRoot,
		Root:        update.Root,
		BlockNumber: update.BlockNumber,
	}

	// Convert account changes (map -> slice)
	if len(update.AccountChanges) > 0 {
		msg.AccountChanges = make([]AccountChangeEntry, 0, len(update.AccountChanges))
		for addr, change := range update.AccountChanges {
			msg.AccountChanges = append(msg.AccountChanges, AccountChangeEntry{
				Address: addr,
				Prev:    convertStateAccountToRLP(change.Prev),
				New:     convertStateAccountToRLP(change.New),
			})
		}
	}

	// Convert storage changes (nested map -> slice of slices)
	if len(update.StorageChanges) > 0 {
		msg.StorageChanges = make([]StorageChangeEntry, 0, len(update.StorageChanges))
		for addr, slots := range update.StorageChanges {
			entry := StorageChangeEntry{
				Address: addr,
				Slots:   make([]SlotChange, 0, len(slots)),
			}
			for slot, change := range slots {
				entry.Slots = append(entry.Slots, SlotChange{
					Key:  slot,
					Prev: change.Prev,
					New:  change.New,
				})
			}
			msg.StorageChanges = append(msg.StorageChanges, entry)
		}
	}

	// Convert code changes
	if len(update.CodeChanges) > 0 {
		msg.CodeChanges = make([]CodeChangeEntry, 0, len(update.CodeChanges))
		for addr, change := range update.CodeChanges {
			msg.CodeChanges = append(msg.CodeChanges, CodeChangeEntry{
				Address: addr,
				Prev:    convertContractCodeToRLP(change.Prev),
				New:     convertContractCodeToRLP(change.New),
			})
		}
	}

	// Convert trie changes
	if len(update.TrieChanges) > 0 {
		msg.TrieChanges = make([]TrieChangeEntry, 0, len(update.TrieChanges))
		for owner, nodes := range update.TrieChanges {
			entry := TrieChangeEntry{
				Owner: owner,
				Nodes: make([]TrieNodeEntry, 0, len(nodes)),
			}
			for path, change := range nodes {
				entry.Nodes = append(entry.Nodes, TrieNodeEntry{
					Path: path,
					Prev: convertTrieNodeToRLP(change.Prev),
					New:  convertTrieNodeToRLP(change.New),
				})
			}
			msg.TrieChanges = append(msg.TrieChanges, entry)
		}
	}

	return msg
}

func convertStateAccountToRLP(acct *types.StateAccount) *StateAccountRLP {
	if acct == nil {
		return nil
	}
	return &StateAccountRLP{
		Nonce:    acct.Nonce,
		Balance:  acct.Balance,
		Root:     acct.Root,
		CodeHash: acct.CodeHash,
	}
}

func convertContractCodeToRLP(code *tracing.ContractCode) *ContractCodeRLP {
	if code == nil {
		return nil
	}
	return &ContractCodeRLP{
		Hash:   code.Hash,
		Code:   code.Code,
		Exists: code.Exists,
	}
}

func convertTrieNodeToRLP(node *trienode.Node) *TrieNodeRLP {
	if node == nil {
		return nil
	}
	return &TrieNodeRLP{
		Blob: node.Blob,
		Hash: node.Hash,
	}
}

func (t *stateSizeTracer) onClose() {
	t.closeOnce.Do(func() {
		t.mu.Lock()
		defer t.mu.Unlock()

		// Flush any remaining pending updates
		if len(t.pendingBuffer) > 0 {
			if err := t.flushBatch(); err != nil {
				log.Error("Failed to flush remaining batch on close", "err", err)
			}
		}

		if t.db != nil {
			var lastBlock uint64
			if t.progress != nil {
				lastBlock = t.progress.BlockNumber
			}
			log.Info("Closing statesize tracer database",
				"lastPersistedBlock", lastBlock)
			if err := t.db.Close(); err != nil {
				log.Error("Failed to close database", "err", err)
			}
			t.db = nil
		}
	})
}

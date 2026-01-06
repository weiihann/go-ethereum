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
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

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
	// batchSize is the number of blocks per batch file.
	batchSize = 5000
)

// stateSizeTracerConfig is the configuration for the statesize tracer.
type stateSizeTracerConfig struct {
	// OutputDir is the directory where batch files will be stored.
	OutputDir string `json:"outputDir"`
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

// batchFile represents an open batch file with its gzip writer.
type batchFile struct {
	file       *os.File
	gzipWriter *gzip.Writer
	bufWriter  *bufio.Writer
	count      int    // number of updates written
	lastBlock  uint64 // last block number written to this batch
}

type stateSizeTracer struct {
	mu        sync.Mutex
	outputDir string

	// currentBatch tracks the currently open batch file.
	currentBatch *batchFile
	batchNumber  uint64
	initialized  bool
	lastBlock    uint64 // last block number processed across all batches

	closeOnce sync.Once
}

func newStateSizeTracer(cfg json.RawMessage) (*tracing.Hooks, error) {
	var config stateSizeTracerConfig
	if err := json.Unmarshal(cfg, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}
	if config.OutputDir == "" {
		return nil, errors.New("statesize tracer outputDir is required")
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(config.OutputDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	t := &stateSizeTracer{
		outputDir: config.OutputDir,
	}

	log.Info("Starting statesize tracer", "outputDir", config.OutputDir, "batchSize", batchSize)

	return &tracing.Hooks{
		OnStateUpdate: t.onStateUpdate,
		OnClose:       t.onClose,
	}, nil
}

// batchFileName returns the filename for a given batch number.
func batchFileName(batchNumber uint64) string {
	startBlock := batchNumber * batchSize
	endBlock := startBlock + batchSize - 1
	return fmt.Sprintf("%d_%d.rlp.gz", startBlock, endBlock)
}

// getBatchNumber returns the batch number for a given block number.
func getBatchNumber(blockNumber uint64) uint64 {
	return blockNumber / batchSize
}

// openBatchFile opens or creates a batch file for the given batch number.
// If the file exists, it reads existing data and prepares for appending.
// Corrupted files are automatically recovered (valid records are preserved).
// If starting from an earlier block (reorg/restart), truncates to that point.
func (t *stateSizeTracer) openBatchFile(batchNum uint64, expectedBlock uint64) (*batchFile, error) {
	filename := filepath.Join(t.outputDir, batchFileName(batchNum))

	var existingUpdates []*StateUpdateRLP
	var lastBlockInFile uint64

	// Check if file exists and read existing data
	if _, err := os.Stat(filename); err == nil {
		existingUpdates, err = readExistingBatch(filename)
		if err != nil {
			return nil, fmt.Errorf("failed to read existing batch file: %w", err)
		}
		if len(existingUpdates) > 0 {
			lastBlockInFile = existingUpdates[len(existingUpdates)-1].BlockNumber
			log.Info("Found existing batch file",
				"file", filename,
				"existingUpdates", len(existingUpdates),
				"lastBlock", lastBlockInFile)

			// Handle different scenarios:
			// 1. expectedBlock == lastBlock+1: normal continuation
			// 2. expectedBlock == lastBlock: crash recovery, re-process same block
			// 3. expectedBlock < lastBlock: reorg/restart from earlier block, truncate
			// 4. expectedBlock > lastBlock+1: gap in blocks, error

			if expectedBlock > lastBlockInFile+1 {
				return nil, fmt.Errorf("block gap detected: expected block %d but last recorded block is %d (missing blocks)",
					expectedBlock, lastBlockInFile)
			}

			if expectedBlock <= lastBlockInFile {
				// Truncate: keep only updates before expectedBlock
				truncateIdx := 0
				for i, update := range existingUpdates {
					if update.BlockNumber >= expectedBlock {
						truncateIdx = i
						break
					}
					truncateIdx = i + 1
				}

				removedCount := len(existingUpdates) - truncateIdx
				existingUpdates = existingUpdates[:truncateIdx]

				if len(existingUpdates) > 0 {
					lastBlockInFile = existingUpdates[len(existingUpdates)-1].BlockNumber
				} else {
					lastBlockInFile = 0
				}

				log.Warn("Truncating batch file (reorg/restart from earlier block)",
					"file", filename,
					"expectedBlock", expectedBlock,
					"removedRecords", removedCount,
					"remainingRecords", len(existingUpdates),
					"newLastBlock", lastBlockInFile)
			}
		}
	}

	// Create/overwrite the file (we'll write existing + new data)
	file, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create batch file: %w", err)
	}

	gzWriter, _ := gzip.NewWriterLevel(file, gzip.BestCompression)
	bufWriter := bufio.NewWriterSize(gzWriter, 256*1024) // 256KB buffer

	bf := &batchFile{
		file:       file,
		gzipWriter: gzWriter,
		bufWriter:  bufWriter,
		count:      0,
		lastBlock:  lastBlockInFile,
	}

	// Write back existing updates
	for _, update := range existingUpdates {
		if err := bf.writeUpdate(update); err != nil {
			bf.close()
			return nil, fmt.Errorf("failed to write existing update: %w", err)
		}
		bf.lastBlock = update.BlockNumber
	}

	return bf, nil
}

// writeUpdate writes a single RLP-encoded update with length prefix.
func (bf *batchFile) writeUpdate(update *StateUpdateRLP) error {
	// RLP encode the update
	data, err := rlp.EncodeToBytes(update)
	if err != nil {
		return fmt.Errorf("failed to RLP encode: %w", err)
	}

	// Write 4-byte length prefix (big-endian)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
	if _, err := bf.bufWriter.Write(lenBuf[:]); err != nil {
		return err
	}

	// Write the RLP data
	if _, err := bf.bufWriter.Write(data); err != nil {
		return err
	}

	bf.count++
	return nil
}

// readExistingBatch reads all valid updates from an existing batch file.
// If the file is corrupted (e.g., from a crash), it recovers as many complete records as possible.
func readExistingBatch(filename string) ([]*StateUpdateRLP, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		// Corrupted gzip header - file is unusable
		log.Warn("Batch file has corrupted gzip header, starting fresh", "file", filename)
		return nil, nil
	}
	defer gzReader.Close()

	bufReader := bufio.NewReaderSize(gzReader, 256*1024)
	var updates []*StateUpdateRLP

	for {
		// Read 4-byte length prefix
		var lenBuf [4]byte
		n, err := io.ReadFull(bufReader, lenBuf[:])
		if err != nil {
			if err == io.EOF {
				break
			}
			if err == io.ErrUnexpectedEOF {
				// Partial length prefix - truncated during crash
				log.Warn("Batch file truncated at length prefix, recovering",
					"file", filename, "recoveredRecords", len(updates), "bytesRead", n)
				break
			}
			// Other read error (e.g., gzip checksum error)
			log.Warn("Batch file read error, recovering",
				"file", filename, "recoveredRecords", len(updates), "err", err)
			break
		}
		dataLen := binary.BigEndian.Uint32(lenBuf[:])

		// Sanity check: reject obviously invalid lengths (> 100MB)
		if dataLen > 100*1024*1024 {
			log.Warn("Batch file has invalid record length, recovering",
				"file", filename, "recoveredRecords", len(updates), "invalidLen", dataLen)
			break
		}

		// Read the RLP data
		data := make([]byte, dataLen)
		if _, err := io.ReadFull(bufReader, data); err != nil {
			// Truncated record - crash happened mid-write
			log.Warn("Batch file truncated at record data, recovering",
				"file", filename, "recoveredRecords", len(updates), "err", err)
			break
		}

		// Decode RLP
		var update StateUpdateRLP
		if err := rlp.DecodeBytes(data, &update); err != nil {
			// Corrupted RLP data
			log.Warn("Batch file has corrupted RLP record, recovering",
				"file", filename, "recoveredRecords", len(updates), "err", err)
			break
		}
		updates = append(updates, &update)
	}

	return updates, nil
}

// close closes the batch file and its writers.
func (bf *batchFile) close() error {
	if bf == nil {
		return nil
	}

	var errs []error

	if bf.bufWriter != nil {
		if err := bf.bufWriter.Flush(); err != nil {
			errs = append(errs, err)
		}
	}

	if bf.gzipWriter != nil {
		if err := bf.gzipWriter.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if bf.file != nil {
		if err := bf.file.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing batch file: %v", errs)
	}
	return nil
}

func (t *stateSizeTracer) onStateUpdate(update *tracing.StateUpdate) {
	if update == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	batchNum := getBatchNumber(update.BlockNumber)

	// Check for reorg: if we receive an earlier block, close current batch and reopen
	if t.initialized && update.BlockNumber <= t.lastBlock {
		log.Warn("Reorg detected, rewinding",
			"lastBlock", t.lastBlock,
			"newBlock", update.BlockNumber)
		// Close current batch - it will be truncated when reopened
		if t.currentBatch != nil {
			if err := t.currentBatch.close(); err != nil {
				log.Error("Failed to close batch file", "err", err)
			}
			t.currentBatch = nil
		}
	}

	// Validate block continuity (skip for first block, reorg already handled above)
	if t.initialized && t.currentBatch != nil && update.BlockNumber != t.lastBlock+1 {
		log.Crit("Block gap detected (missing blocks)",
			"expected", t.lastBlock+1,
			"got", update.BlockNumber)
		return
	}

	// Check if we need to switch to a new batch
	if t.initialized && t.currentBatch != nil && batchNum != t.batchNumber {
		// Close current batch
		log.Info("Closing batch file",
			"batch", t.batchNumber,
			"updates", t.currentBatch.count)
		if err := t.currentBatch.close(); err != nil {
			log.Error("Failed to close batch file", "err", err)
		}
		t.currentBatch = nil
	}

	// Open batch file if needed
	if t.currentBatch == nil {
		bf, err := t.openBatchFile(batchNum, update.BlockNumber)
		if err != nil {
			log.Crit("Failed to open batch file", "batch", batchNum, "err", err)
			return
		}
		t.currentBatch = bf
		t.batchNumber = batchNum
		t.initialized = true
		log.Info("Opened batch file", "batch", batchNum, "file", batchFileName(batchNum))
	}

	// Convert and write the update
	msg := convertStateUpdateToRLP(update)
	if err := t.currentBatch.writeUpdate(msg); err != nil {
		log.Error("Failed to write state update", "block", update.BlockNumber, "err", err)
		return
	}
	t.currentBatch.lastBlock = update.BlockNumber
	t.lastBlock = update.BlockNumber

	// Log progress periodically
	if t.currentBatch.count%batchSize == 0 {
		log.Info("State updates progress",
			"batch", t.batchNumber,
			"updates", t.currentBatch.count,
			"lastBlock", t.currentBatch.lastBlock)
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

		// Close any open batch file
		if t.currentBatch != nil {
			log.Info("Closing final batch file",
				"batch", t.batchNumber,
				"updates", t.currentBatch.count)
			if err := t.currentBatch.close(); err != nil {
				log.Error("Failed to close batch file", "err", err)
			}
			t.currentBatch = nil
		}

		log.Info("Statesize tracer closed")
	})
}

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
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/tracers"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

func init() {
	tracers.LiveDirectory.Register("statesize", newStateSizeTracer)
}

const (
	// batchSize is the number of blocks per batch file.
	batchSize = 2000
)

// stateSizeTracerConfig is the configuration for the statesize tracer.
type stateSizeTracerConfig struct {
	// OutputDir is the directory where batch files will be stored.
	OutputDir string `json:"outputDir"`
}

// StateUpdateJSON is the JSON-serializable version of tracing.StateUpdate.
type StateUpdateJSON struct {
	OriginRoot  common.Hash `json:"originRoot"`
	Root        common.Hash `json:"root"`
	BlockNumber uint64      `json:"blockNumber"`

	AccountChanges map[common.Address]*AccountChangeJSON                 `json:"accountChanges,omitempty"`
	StorageChanges map[common.Address]map[common.Hash]*StorageChangeJSON `json:"storageChanges,omitempty"`
	CodeChanges    map[common.Address]*CodeChangeJSON                    `json:"codeChanges,omitempty"`
	TrieChanges    map[common.Hash]map[string]*TrieNodeChangeJSON        `json:"trieChanges,omitempty"`
}

// AccountChangeJSON is the JSON-serializable version of tracing.AccountChange.
type AccountChangeJSON struct {
	Prev *StateAccountJSON `json:"prev,omitempty"`
	New  *StateAccountJSON `json:"new,omitempty"`
}

// StateAccountJSON is a JSON-serializable version of types.StateAccount.
type StateAccountJSON struct {
	Nonce    uint64        `json:"nonce"`
	Balance  *hexutil.Big  `json:"balance"`
	Root     common.Hash   `json:"root"`
	CodeHash hexutil.Bytes `json:"codeHash"`
}

// StorageChangeJSON is the JSON-serializable version of tracing.StorageChange.
type StorageChangeJSON struct {
	Prev common.Hash `json:"prev"`
	New  common.Hash `json:"new"`
}

// CodeChangeJSON is the JSON-serializable version of tracing.CodeChange.
type CodeChangeJSON struct {
	Prev *ContractCodeJSON `json:"prev,omitempty"`
	New  *ContractCodeJSON `json:"new,omitempty"`
}

// ContractCodeJSON is the JSON-serializable version of tracing.ContractCode.
type ContractCodeJSON struct {
	Hash   common.Hash   `json:"hash"`
	Code   hexutil.Bytes `json:"code"`
	Exists bool          `json:"exists"`
}

// TrieNodeChangeJSON is the JSON-serializable version of tracing.TrieNodeChange.
type TrieNodeChangeJSON struct {
	Prev *TrieNodeJSON `json:"prev,omitempty"`
	New  *TrieNodeJSON `json:"new,omitempty"`
}

// TrieNodeJSON is the JSON-serializable version of trienode.Node.
type TrieNodeJSON struct {
	Blob hexutil.Bytes `json:"blob"`
	Hash common.Hash   `json:"hash"`
}

// batchFile represents an open batch file with its gzip writer.
type batchFile struct {
	file       *os.File
	gzipWriter *gzip.Writer
	encoder    *json.Encoder
	count      int // number of updates written
}

type stateSizeTracer struct {
	mu        sync.Mutex
	outputDir string

	// currentBatch tracks the currently open batch file.
	// Key is the batch number (blockNumber / batchSize).
	currentBatch *batchFile
	batchNumber  uint64
	initialized  bool

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
	return fmt.Sprintf("state_updates_%d_%d.json.gz", startBlock, endBlock)
}

// getBatchNumber returns the batch number for a given block number.
func getBatchNumber(blockNumber uint64) uint64 {
	return blockNumber / batchSize
}

// openBatchFile opens or creates a batch file for the given batch number.
// If the file exists, it reads existing data and prepares for appending.
func (t *stateSizeTracer) openBatchFile(batchNum uint64) (*batchFile, error) {
	filename := filepath.Join(t.outputDir, batchFileName(batchNum))

	var existingUpdates []*StateUpdateJSON

	// Check if file exists and read existing data
	if _, err := os.Stat(filename); err == nil {
		existingUpdates, err = t.readExistingBatch(filename)
		if err != nil {
			return nil, fmt.Errorf("failed to read existing batch file: %w", err)
		}
		log.Info("Found existing batch file", "file", filename, "existingUpdates", len(existingUpdates))
	}

	// Create/overwrite the file (we'll write existing + new data)
	file, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create batch file: %w", err)
	}

	gzWriter, _ := gzip.NewWriterLevel(file, gzip.BestCompression)
	encoder := json.NewEncoder(gzWriter)

	bf := &batchFile{
		file:       file,
		gzipWriter: gzWriter,
		encoder:    encoder,
		count:      0,
	}

	// Write back existing updates
	for _, update := range existingUpdates {
		if err := encoder.Encode(update); err != nil {
			bf.close()
			return nil, fmt.Errorf("failed to write existing update: %w", err)
		}
		bf.count++
	}

	return bf, nil
}

// readExistingBatch reads all updates from an existing batch file.
func (t *stateSizeTracer) readExistingBatch(filename string) ([]*StateUpdateJSON, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return nil, err
	}
	defer gzReader.Close()

	var updates []*StateUpdateJSON
	decoder := json.NewDecoder(gzReader)

	for decoder.More() {
		var update StateUpdateJSON
		if err := decoder.Decode(&update); err != nil {
			return nil, err
		}
		updates = append(updates, &update)
	}

	return updates, nil
}

// close closes the batch file and its gzip writer.
func (bf *batchFile) close() error {
	if bf == nil {
		return nil
	}

	var errs []error

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

	// Check if we need to switch to a new batch
	if t.initialized && batchNum != t.batchNumber {
		// Close current batch
		if t.currentBatch != nil {
			log.Info("Closing batch file",
				"batch", t.batchNumber,
				"updates", t.currentBatch.count)
			if err := t.currentBatch.close(); err != nil {
				log.Error("Failed to close batch file", "err", err)
			}
			t.currentBatch = nil
		}
	}

	// Open batch file if needed
	if t.currentBatch == nil {
		bf, err := t.openBatchFile(batchNum)
		if err != nil {
			log.Error("Failed to open batch file", "batch", batchNum, "err", err)
			return
		}
		t.currentBatch = bf
		t.batchNumber = batchNum
		t.initialized = true
		log.Info("Opened batch file", "batch", batchNum, "file", batchFileName(batchNum))
	}

	// Convert and write the update
	msg := convertStateUpdate(update)
	if err := t.currentBatch.encoder.Encode(msg); err != nil {
		log.Error("Failed to write state update", "block", update.BlockNumber, "err", err)
		return
	}
	t.currentBatch.count++

	// Log progress periodically
	if t.currentBatch.count%100 == 0 {
		log.Info("State updates progress",
			"batch", t.batchNumber,
			"updates", t.currentBatch.count,
			"lastBlock", update.BlockNumber)
	}
}

// convertStateUpdate converts a tracing.StateUpdate to a JSON-serializable format.
func convertStateUpdate(update *tracing.StateUpdate) *StateUpdateJSON {
	msg := &StateUpdateJSON{
		OriginRoot:  update.OriginRoot,
		Root:        update.Root,
		BlockNumber: update.BlockNumber,
	}

	// Convert account changes
	if len(update.AccountChanges) > 0 {
		msg.AccountChanges = make(map[common.Address]*AccountChangeJSON, len(update.AccountChanges))
		for addr, change := range update.AccountChanges {
			msg.AccountChanges[addr] = &AccountChangeJSON{
				Prev: convertStateAccount(change.Prev),
				New:  convertStateAccount(change.New),
			}
		}
	}

	// Convert storage changes
	if len(update.StorageChanges) > 0 {
		msg.StorageChanges = make(map[common.Address]map[common.Hash]*StorageChangeJSON, len(update.StorageChanges))
		for addr, slots := range update.StorageChanges {
			msg.StorageChanges[addr] = make(map[common.Hash]*StorageChangeJSON, len(slots))
			for slot, change := range slots {
				msg.StorageChanges[addr][slot] = &StorageChangeJSON{
					Prev: change.Prev,
					New:  change.New,
				}
			}
		}
	}

	// Convert code changes
	if len(update.CodeChanges) > 0 {
		msg.CodeChanges = make(map[common.Address]*CodeChangeJSON, len(update.CodeChanges))
		for addr, change := range update.CodeChanges {
			msg.CodeChanges[addr] = &CodeChangeJSON{
				Prev: convertContractCode(change.Prev),
				New:  convertContractCode(change.New),
			}
		}
	}

	// Convert trie changes
	if len(update.TrieChanges) > 0 {
		msg.TrieChanges = make(map[common.Hash]map[string]*TrieNodeChangeJSON, len(update.TrieChanges))
		for owner, nodes := range update.TrieChanges {
			msg.TrieChanges[owner] = make(map[string]*TrieNodeChangeJSON, len(nodes))
			for path, change := range nodes {
				msg.TrieChanges[owner][path] = &TrieNodeChangeJSON{
					Prev: convertTrieNode(change.Prev),
					New:  convertTrieNode(change.New),
				}
			}
		}
	}

	return msg
}

func convertStateAccount(acct *types.StateAccount) *StateAccountJSON {
	if acct == nil {
		return nil
	}
	return &StateAccountJSON{
		Nonce:    acct.Nonce,
		Balance:  (*hexutil.Big)(acct.Balance.ToBig()),
		Root:     acct.Root,
		CodeHash: acct.CodeHash,
	}
}

func convertContractCode(code *tracing.ContractCode) *ContractCodeJSON {
	if code == nil {
		return nil
	}
	return &ContractCodeJSON{
		Hash:   code.Hash,
		Code:   code.Code,
		Exists: code.Exists,
	}
}

func convertTrieNode(node *trienode.Node) *TrieNodeJSON {
	if node == nil {
		return nil
	}
	return &TrieNodeJSON{
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

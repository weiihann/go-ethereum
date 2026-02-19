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
	"encoding/json"
	"slices"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/tracers"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
)

func init() {
	tracers.LiveDirectory.Register("statesize", newStateSizeTracer)
}

// Database key size constants matching core/state/state_sizer.go
var (
	accountKeySize            = int64(len(rawdb.SnapshotAccountPrefix) + common.HashLength)
	storageKeySize            = int64(len(rawdb.SnapshotStoragePrefix) + common.HashLength*2)
	accountTrienodePrefixSize = int64(len(rawdb.TrieNodeAccountPrefix))
	storageTrienodePrefixSize = int64(len(rawdb.TrieNodeStoragePrefix) + common.HashLength)
	codeKeySize               = int64(len(rawdb.CodePrefix) + common.HashLength)
)

// depthStats holds node count and byte size for a single depth level.
type depthStats struct {
	Count int64
	Bytes int64
}

// stateSizeDelta represents state size delta for a single block.
type stateSizeDelta struct {
	AccountDelta              int64
	AccountBytesDelta         int64
	AccountTrienodeDelta      int64
	AccountTrienodeBytesDelta int64
	ContractCodeDelta         int64
	ContractCodeBytesDelta    int64
	StorageDelta              int64
	StorageBytesDelta         int64
	StorageTrienodeDelta      int64
	StorageTrienodeBytesDelta int64
}

// JSON log output types following the "Slow block" pattern in blockchain_stats.go.
// These structs control the exact JSON format captured by the sentry-logs Vector pipeline.

type stateMetricsLog struct {
	Level           string            `json:"level"`
	Msg             string            `json:"msg"`
	BlockNumber     uint64            `json:"block_number"`
	StateRoot       string            `json:"state_root"`
	ParentStateRoot string            `json:"parent_state_root"`
	Delta           stateMetricsDelta `json:"delta"`
	Depth           stateMetricsDepth `json:"depth"`
}

type stateMetricsDelta struct {
	Account              int64 `json:"account"`
	AccountBytes         int64 `json:"account_bytes"`
	AccountTrienode      int64 `json:"account_trienode"`
	AccountTrienodeBytes int64 `json:"account_trienode_bytes"`
	ContractCode         int64 `json:"contract_code"`
	ContractCodeBytes    int64 `json:"contract_code_bytes"`
	Storage              int64 `json:"storage"`
	StorageBytes         int64 `json:"storage_bytes"`
	StorageTrienode      int64 `json:"storage_trienode"`
	StorageTrienodeBytes int64 `json:"storage_trienode_bytes"`
}

type stateMetricsDepth struct {
	TotalAccountWrittenNodes uint64           `json:"total_account_written_nodes"`
	TotalAccountWrittenBytes uint64           `json:"total_account_written_bytes"`
	TotalAccountDeletedNodes uint64           `json:"total_account_deleted_nodes"`
	TotalAccountDeletedBytes uint64           `json:"total_account_deleted_bytes"`
	TotalStorageWrittenNodes uint64           `json:"total_storage_written_nodes"`
	TotalStorageWrittenBytes uint64           `json:"total_storage_written_bytes"`
	TotalStorageDeletedNodes uint64           `json:"total_storage_deleted_nodes"`
	TotalStorageDeletedBytes uint64           `json:"total_storage_deleted_bytes"`
	AccountWrittenNodes      map[uint8]uint64 `json:"account_written_nodes"`
	AccountWrittenBytes      map[uint8]uint64 `json:"account_written_bytes"`
	AccountDeletedNodes      map[uint8]uint64 `json:"account_deleted_nodes"`
	AccountDeletedBytes      map[uint8]uint64 `json:"account_deleted_bytes"`
	StorageWrittenNodes      map[uint8]uint64 `json:"storage_written_nodes"`
	StorageWrittenBytes      map[uint8]uint64 `json:"storage_written_bytes"`
	StorageDeletedNodes      map[uint8]uint64 `json:"storage_deleted_nodes"`
	StorageDeletedBytes      map[uint8]uint64 `json:"storage_deleted_bytes"`
}

type stateSizeTracer struct{}

func newStateSizeTracer(cfg json.RawMessage) (*tracing.Hooks, error) {
	t := &stateSizeTracer{}

	log.Info("State size tracer initialized (sentry-logs mode)")

	return &tracing.Hooks{
		OnStateUpdate: t.onStateUpdate,
	}, nil
}

func (s *stateSizeTracer) onStateUpdate(update *tracing.StateUpdate) {
	if update == nil {
		return
	}

	// Calculate state size delta and depth stats
	delta, accountDepthCreated, storageDepthCreated, accountDepthDeleted, storageDepthDeleted := calculateStateSizeDelta(update)

	// Build depth maps (only non-zero entries)
	depth := buildDepthMetrics(accountDepthCreated, storageDepthCreated, accountDepthDeleted, storageDepthDeleted)

	// Build and emit JSON log (same pattern as logSlow in blockchain_stats.go)
	entry := stateMetricsLog{
		Level:           "info",
		Msg:             "State metrics",
		BlockNumber:     update.BlockNumber,
		StateRoot:       update.Root.Hex(),
		ParentStateRoot: update.OriginRoot.Hex(),
		Delta: stateMetricsDelta{
			Account:              delta.AccountDelta,
			AccountBytes:         delta.AccountBytesDelta,
			AccountTrienode:      delta.AccountTrienodeDelta,
			AccountTrienodeBytes: delta.AccountTrienodeBytesDelta,
			ContractCode:         delta.ContractCodeDelta,
			ContractCodeBytes:    delta.ContractCodeBytesDelta,
			Storage:              delta.StorageDelta,
			StorageBytes:         delta.StorageBytesDelta,
			StorageTrienode:      delta.StorageTrienodeDelta,
			StorageTrienodeBytes: delta.StorageTrienodeBytesDelta,
		},
		Depth: depth,
	}

	jsonBytes, err := json.Marshal(entry)
	if err != nil {
		log.Error("Failed to marshal state metrics log", "error", err)
		return
	}
	log.Info(string(jsonBytes))
}

// buildDepthMetrics converts [65]depthStats arrays into map-based metrics with totals.
func buildDepthMetrics(
	accountCreated, storageCreated, accountDeleted, storageDeleted [65]depthStats,
) stateMetricsDepth {
	d := stateMetricsDepth{
		AccountWrittenNodes: make(map[uint8]uint64, 10),
		AccountWrittenBytes: make(map[uint8]uint64, 10),
		AccountDeletedNodes: make(map[uint8]uint64, 10),
		AccountDeletedBytes: make(map[uint8]uint64, 10),
		StorageWrittenNodes: make(map[uint8]uint64, 10),
		StorageWrittenBytes: make(map[uint8]uint64, 10),
		StorageDeletedNodes: make(map[uint8]uint64, 10),
		StorageDeletedBytes: make(map[uint8]uint64, 10),
	}

	for i := range 65 {
		depth := uint8(i)

		if accountCreated[i].Count > 0 {
			d.AccountWrittenNodes[depth] = uint64(accountCreated[i].Count)
			d.TotalAccountWrittenNodes += uint64(accountCreated[i].Count)
		}
		if accountCreated[i].Bytes > 0 {
			d.AccountWrittenBytes[depth] = uint64(accountCreated[i].Bytes)
			d.TotalAccountWrittenBytes += uint64(accountCreated[i].Bytes)
		}
		if accountDeleted[i].Count > 0 {
			d.AccountDeletedNodes[depth] = uint64(accountDeleted[i].Count)
			d.TotalAccountDeletedNodes += uint64(accountDeleted[i].Count)
		}
		if accountDeleted[i].Bytes > 0 {
			d.AccountDeletedBytes[depth] = uint64(accountDeleted[i].Bytes)
			d.TotalAccountDeletedBytes += uint64(accountDeleted[i].Bytes)
		}
		if storageCreated[i].Count > 0 {
			d.StorageWrittenNodes[depth] = uint64(storageCreated[i].Count)
			d.TotalStorageWrittenNodes += uint64(storageCreated[i].Count)
		}
		if storageCreated[i].Bytes > 0 {
			d.StorageWrittenBytes[depth] = uint64(storageCreated[i].Bytes)
			d.TotalStorageWrittenBytes += uint64(storageCreated[i].Bytes)
		}
		if storageDeleted[i].Count > 0 {
			d.StorageDeletedNodes[depth] = uint64(storageDeleted[i].Count)
			d.TotalStorageDeletedNodes += uint64(storageDeleted[i].Count)
		}
		if storageDeleted[i].Bytes > 0 {
			d.StorageDeletedBytes[depth] = uint64(storageDeleted[i].Bytes)
			d.TotalStorageDeletedBytes += uint64(storageDeleted[i].Bytes)
		}
	}
	return d
}

// calculateStateSizeDelta computes the state size delta from a state update.
// It returns the delta and depth stats (count + bytes) for account/storage trie nodes (created and deleted).
func calculateStateSizeDelta(update *tracing.StateUpdate) (
	delta stateSizeDelta,
	accountDepthCreated, storageDepthCreated, accountDepthDeleted, storageDepthDeleted [65]depthStats,
) {
	// Calculate account size changes
	for _, change := range update.AccountChanges {
		prevLen := slimAccountSize(change.Prev)
		newLen := slimAccountSize(change.New)

		switch {
		case prevLen > 0 && newLen == 0:
			delta.AccountDelta--
			delta.AccountBytesDelta -= accountKeySize + int64(prevLen)
		case prevLen == 0 && newLen > 0:
			delta.AccountDelta++
			delta.AccountBytesDelta += accountKeySize + int64(newLen)
		default:
			delta.AccountBytesDelta += int64(newLen - prevLen)
		}
	}

	// Calculate storage size changes
	for _, slots := range update.StorageChanges {
		for _, change := range slots {
			prevLen := len(encodeStorageValue(change.Prev))
			newLen := len(encodeStorageValue(change.New))

			switch {
			case prevLen > 0 && newLen == 0:
				delta.StorageDelta--
				delta.StorageBytesDelta -= storageKeySize + int64(prevLen)
			case prevLen == 0 && newLen > 0:
				delta.StorageDelta++
				delta.StorageBytesDelta += storageKeySize + int64(newLen)
			default:
				delta.StorageBytesDelta += int64(newLen - prevLen)
			}
		}
	}

	// Calculate trie node size changes and depth counts
	for owner, nodes := range update.TrieChanges {
		var (
			keyPrefix int64
			isAccount = owner == (common.Hash{})
		)
		if isAccount {
			keyPrefix = accountTrienodePrefixSize
		} else {
			keyPrefix = storageTrienodePrefixSize
		}

		// Calculate depth stats for created/modified and deleted nodes
		createdStats, deletedStats := calculateDepthStatsByType(nodes)

		for path, change := range nodes {
			var prevLen, newLen int
			if change.Prev != nil {
				prevLen = len(change.Prev.Blob)
			}
			if change.New != nil {
				newLen = len(change.New.Blob)
			}
			keySize := keyPrefix + int64(len(path))

			switch {
			case prevLen > 0 && newLen == 0:
				if isAccount {
					delta.AccountTrienodeDelta--
					delta.AccountTrienodeBytesDelta -= keySize + int64(prevLen)
				} else {
					delta.StorageTrienodeDelta--
					delta.StorageTrienodeBytesDelta -= keySize + int64(prevLen)
				}
			case prevLen == 0 && newLen > 0:
				if isAccount {
					delta.AccountTrienodeDelta++
					delta.AccountTrienodeBytesDelta += keySize + int64(newLen)
				} else {
					delta.StorageTrienodeDelta++
					delta.StorageTrienodeBytesDelta += keySize + int64(newLen)
				}
			default:
				if isAccount {
					delta.AccountTrienodeBytesDelta += int64(newLen - prevLen)
				} else {
					delta.StorageTrienodeBytesDelta += int64(newLen - prevLen)
				}
			}
		}

		// Accumulate depth stats
		if isAccount {
			for i := range 65 {
				accountDepthCreated[i].Count += createdStats[i].Count
				accountDepthCreated[i].Bytes += createdStats[i].Bytes
				accountDepthDeleted[i].Count += deletedStats[i].Count
				accountDepthDeleted[i].Bytes += deletedStats[i].Bytes
			}
		} else {
			for i := range 65 {
				storageDepthCreated[i].Count += createdStats[i].Count
				storageDepthCreated[i].Bytes += createdStats[i].Bytes
				storageDepthDeleted[i].Count += deletedStats[i].Count
				storageDepthDeleted[i].Bytes += deletedStats[i].Bytes
			}
		}
	}

	// Calculate contract code size changes
	// Only count new codes that didn't exist before
	codeExists := make(map[common.Hash]struct{})
	for _, change := range update.CodeChanges {
		if change.New == nil {
			continue
		}
		// Skip if we've already counted this code hash or if it existed before
		if _, ok := codeExists[change.New.Hash]; ok || change.New.Exists {
			continue
		}
		delta.ContractCodeDelta++
		delta.ContractCodeBytesDelta += codeKeySize + int64(len(change.New.Code))
		codeExists[change.New.Hash] = struct{}{}
	}

	return
}

// encodeStorageValue RLP-encodes a storage value for size calculation.
func encodeStorageValue(val common.Hash) []byte {
	if val == (common.Hash{}) {
		return nil
	}
	blob, _ := rlp.EncodeToBytes(common.TrimLeftZeroes(val[:]))
	return blob
}

// slimAccountSize calculates the RLP-encoded size of an account in slim format.
func slimAccountSize(acct *types.StateAccount) int {
	if acct == nil {
		return 0
	}
	data := types.SlimAccountRLP(*acct)
	return len(data)
}

// calculateDepthStatsByType calculates the depth of each node and separates stats
// (count and bytes) into created/modified nodes and deleted nodes.
// - Created/Modified: nodes that exist after the update (New has data)
// - Deleted: nodes that existed before but don't exist after (Prev has data, New is empty)
func calculateDepthStatsByType(pathMap map[string]*tracing.TrieNodeChange) (created, deleted [65]depthStats) {
	n := len(pathMap)
	if n == 0 {
		return
	}

	// First, calculate depth for all nodes using the tree structure
	paths := make([]string, 0, n)
	for path := range pathMap {
		paths = append(paths, path)
	}
	slices.Sort(paths)

	// Map from path to its depth
	depthMap := make(map[string]int, n)

	// Stack stores paths of ancestors
	stack := make([]string, 0, 65)

	for _, path := range paths {
		// Pop until stack top is a strict prefix of path
		for len(stack) > 0 {
			top := stack[len(stack)-1]
			if len(top) < len(path) && path[:len(top)] == top {
				break
			}
			stack = stack[:len(stack)-1]
		}

		depth := len(stack)
		depthMap[path] = depth

		stack = append(stack, path)
	}

	// Now classify each node based on Prev/New status
	for path, change := range pathMap {
		depth := depthMap[path]

		var prevLen, newLen int
		if change.Prev != nil {
			prevLen = len(change.Prev.Blob)
		}
		if change.New != nil {
			newLen = len(change.New.Blob)
		}

		// Created/Modified: New has data (node exists after update)
		if newLen > 0 {
			created[depth].Count++
			created[depth].Bytes += int64(newLen)
		}
		// Deleted: Prev has data but New is empty (node removed)
		if prevLen > 0 && newLen == 0 {
			deleted[depth].Count++
			deleted[depth].Bytes += int64(prevLen)
		}
	}

	return
}

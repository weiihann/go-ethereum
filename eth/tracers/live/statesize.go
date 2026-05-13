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

// stateSizeChanges represents the gross write/delete activity for a single block.
// Updates are accounted as BOTH a write (of the new value) AND a delete (of the
// previous value), so net delta = writes - deletes for all five categories.
//
// ContractCodeDeletes / ContractCodeDeleteBytes are present for schema parity
// but always 0 — see the comment on the contract-code loop in
// calculateStateSizeChanges.
type stateSizeChanges struct {
	AccountWrites              int64
	AccountWriteBytes          int64
	AccountDeletes             int64
	AccountDeleteBytes         int64
	AccountTrienodeWrites      int64
	AccountTrienodeWriteBytes  int64
	AccountTrienodeDeletes     int64
	AccountTrienodeDeleteBytes int64
	ContractCodeWrites         int64
	ContractCodeWriteBytes     int64
	ContractCodeDeletes        int64
	ContractCodeDeleteBytes    int64
	StorageWrites              int64
	StorageWriteBytes          int64
	StorageDeletes             int64
	StorageDeleteBytes         int64
	StorageTrienodeWrites      int64
	StorageTrienodeWriteBytes  int64
	StorageTrienodeDeletes     int64
	StorageTrienodeDeleteBytes int64
}

// JSON log output types following the "Slow block" pattern in blockchain_stats.go.
// These structs control the exact JSON format captured by the sentry-logs Vector pipeline.

type stateMetricsLog struct {
	Level           string            `json:"level"`
	Msg             string            `json:"msg"`
	BlockNumber     uint64            `json:"block_number"`
	StateRoot       string            `json:"state_root"`
	ParentStateRoot string            `json:"parent_state_root"`
	Writes          stateMetricsSizes `json:"writes"`
	Deletes         stateMetricsSizes `json:"deletes"`
	Depth           stateMetricsDepth `json:"depth"`
}

// stateMetricsSizes is the per-category count + bytes payload, used twice in
// stateMetricsLog: once for writes and once for deletes.
type stateMetricsSizes struct {
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

	changes, accountDepthCreated, storageDepthCreated, accountDepthDeleted, storageDepthDeleted := calculateStateSizeChanges(update)

	depth := buildDepthMetrics(accountDepthCreated, storageDepthCreated, accountDepthDeleted, storageDepthDeleted)

	entry := stateMetricsLog{
		Level:           "info",
		Msg:             "State metrics",
		BlockNumber:     update.BlockNumber,
		StateRoot:       update.Root.Hex(),
		ParentStateRoot: update.OriginRoot.Hex(),
		Writes: stateMetricsSizes{
			Account:              changes.AccountWrites,
			AccountBytes:         changes.AccountWriteBytes,
			AccountTrienode:      changes.AccountTrienodeWrites,
			AccountTrienodeBytes: changes.AccountTrienodeWriteBytes,
			ContractCode:         changes.ContractCodeWrites,
			ContractCodeBytes:    changes.ContractCodeWriteBytes,
			Storage:              changes.StorageWrites,
			StorageBytes:         changes.StorageWriteBytes,
			StorageTrienode:      changes.StorageTrienodeWrites,
			StorageTrienodeBytes: changes.StorageTrienodeWriteBytes,
		},
		Deletes: stateMetricsSizes{
			Account:              changes.AccountDeletes,
			AccountBytes:         changes.AccountDeleteBytes,
			AccountTrienode:      changes.AccountTrienodeDeletes,
			AccountTrienodeBytes: changes.AccountTrienodeDeleteBytes,
			ContractCode:         changes.ContractCodeDeletes,
			ContractCodeBytes:    changes.ContractCodeDeleteBytes,
			Storage:              changes.StorageDeletes,
			StorageBytes:         changes.StorageDeleteBytes,
			StorageTrienode:      changes.StorageTrienodeDeletes,
			StorageTrienodeBytes: changes.StorageTrienodeDeleteBytes,
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

// calculateStateSizeChanges computes write/delete counts and bytes per category
// from a state update. An "update" (both prev and new present) is accounted as
// BOTH a write of the new value and a delete of the previous one — so a query
// of `writes - deletes` recovers the net delta exactly.
//
// Returns the changes plus per-depth stats for account/storage trie nodes.
func calculateStateSizeChanges(update *tracing.StateUpdate) (
	changes stateSizeChanges,
	accountDepthCreated, storageDepthCreated, accountDepthDeleted, storageDepthDeleted [65]depthStats,
) {
	// Account size changes.
	for _, change := range update.AccountChanges {
		prevLen := slimAccountSize(change.Prev)
		newLen := slimAccountSize(change.New)

		switch {
		case prevLen > 0 && newLen == 0:
			changes.AccountDeletes++
			changes.AccountDeleteBytes += accountKeySize + int64(prevLen)
		case prevLen == 0 && newLen > 0:
			changes.AccountWrites++
			changes.AccountWriteBytes += accountKeySize + int64(newLen)
		default:
			// Update: overwrite semantics — count as both a write and a delete.
			changes.AccountWrites++
			changes.AccountWriteBytes += accountKeySize + int64(newLen)
			changes.AccountDeletes++
			changes.AccountDeleteBytes += accountKeySize + int64(prevLen)
		}
	}

	// Storage slot changes.
	for _, slots := range update.StorageChanges {
		for _, change := range slots {
			prevLen := len(encodeStorageValue(change.Prev))
			newLen := len(encodeStorageValue(change.New))

			switch {
			case prevLen > 0 && newLen == 0:
				changes.StorageDeletes++
				changes.StorageDeleteBytes += storageKeySize + int64(prevLen)
			case prevLen == 0 && newLen > 0:
				changes.StorageWrites++
				changes.StorageWriteBytes += storageKeySize + int64(newLen)
			default:
				changes.StorageWrites++
				changes.StorageWriteBytes += storageKeySize + int64(newLen)
				changes.StorageDeletes++
				changes.StorageDeleteBytes += storageKeySize + int64(prevLen)
			}
		}
	}

	// Trie node changes (both account and storage tries) — and depth stats.
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
					changes.AccountTrienodeDeletes++
					changes.AccountTrienodeDeleteBytes += keySize + int64(prevLen)
				} else {
					changes.StorageTrienodeDeletes++
					changes.StorageTrienodeDeleteBytes += keySize + int64(prevLen)
				}
			case prevLen == 0 && newLen > 0:
				if isAccount {
					changes.AccountTrienodeWrites++
					changes.AccountTrienodeWriteBytes += keySize + int64(newLen)
				} else {
					changes.StorageTrienodeWrites++
					changes.StorageTrienodeWriteBytes += keySize + int64(newLen)
				}
			default:
				if isAccount {
					changes.AccountTrienodeWrites++
					changes.AccountTrienodeWriteBytes += keySize + int64(newLen)
					changes.AccountTrienodeDeletes++
					changes.AccountTrienodeDeleteBytes += keySize + int64(prevLen)
				} else {
					changes.StorageTrienodeWrites++
					changes.StorageTrienodeWriteBytes += keySize + int64(newLen)
					changes.StorageTrienodeDeletes++
					changes.StorageTrienodeDeleteBytes += keySize + int64(prevLen)
				}
			}
		}

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

	// Contract code: write-only by design. Counts unique new code blobs by hash
	// (deduped — adding the same bytecode to two accounts only adds it to the DB
	// once). Deletes are not tracked here because reliably attributing a "last
	// reference gone" event would require ref-counting that state_sizer.go
	// deliberately omits. ContractCodeDeletes / ContractCodeDeleteBytes stay 0.
	codeExists := make(map[common.Hash]struct{})
	for _, change := range update.CodeChanges {
		if change.New == nil {
			continue
		}
		if _, ok := codeExists[change.New.Hash]; ok || change.New.Exists {
			continue
		}
		changes.ContractCodeWrites++
		changes.ContractCodeWriteBytes += codeKeySize + int64(len(change.New.Code))
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

	paths := make([]string, 0, n)
	for path := range pathMap {
		paths = append(paths, path)
	}
	slices.Sort(paths)

	depthMap := make(map[string]int, n)

	stack := make([]string, 0, 65)

	for _, path := range paths {
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

	for path, change := range pathMap {
		depth := depthMap[path]

		var prevLen, newLen int
		if change.Prev != nil {
			prevLen = len(change.Prev.Blob)
		}
		if change.New != nil {
			newLen = len(change.New.Blob)
		}

		if newLen > 0 {
			created[depth].Count++
			created[depth].Bytes += int64(newLen)
		}
		if prevLen > 0 && newLen == 0 {
			deleted[depth].Count++
			deleted[depth].Bytes += int64(prevLen)
		}
	}

	return
}

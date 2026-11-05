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
	"strconv"

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

type stateSizeTracer struct{}

func newStateSizeTracer(cfg json.RawMessage) (*tracing.Hooks, error) {
	t := &stateSizeTracer{}

	return &tracing.Hooks{
		OnStateUpdate: t.onStateUpdate,
	}, nil
}

func (s *stateSizeTracer) onStateUpdate(update *tracing.StateUpdate) {
	if update == nil {
		return
	}

	delta, acCreated, stCreated, acDeleted, stDeleted := calculateStateSizeDelta(update)

	// Build depth map data for JSON output
	depthData := buildDepthData(acCreated, stCreated, acDeleted, stDeleted)

	data := map[string]any{
		"msg":               "State metrics",
		"block_number":      update.BlockNumber,
		"state_root":        update.Root.Hex(),
		"parent_state_root": update.OriginRoot.Hex(),
		"delta": map[string]int64{
			"account":                delta.AccountDelta,
			"account_bytes":          delta.AccountBytesDelta,
			"account_trienode":       delta.AccountTrienodeDelta,
			"account_trienode_bytes": delta.AccountTrienodeBytesDelta,
			"contract_code":          delta.ContractCodeDelta,
			"contract_code_bytes":    delta.ContractCodeBytesDelta,
			"storage":                delta.StorageDelta,
			"storage_bytes":          delta.StorageBytesDelta,
			"storage_trienode":       delta.StorageTrienodeDelta,
			"storage_trienode_bytes": delta.StorageTrienodeBytesDelta,
		},
		"depth": depthData,
	}

	jsonBytes, err := json.Marshal(data)
	if err != nil {
		log.Error("Failed to marshal state metrics", "err", err)
		return
	}

	log.Info(string(jsonBytes))
}

// buildDepthData converts depth stat arrays into maps suitable for JSON output.
// Produces per-depth maps (depth -> value) and totals for written/deleted nodes.
func buildDepthData(
	acCreated, stCreated, acDeleted, stDeleted [65]depthStats,
) map[string]any {
	acWrittenNodes := make(map[string]int64)
	acWrittenBytes := make(map[string]int64)
	acDeletedNodes := make(map[string]int64)
	acDeletedBytes := make(map[string]int64)
	stWrittenNodes := make(map[string]int64)
	stWrittenBytes := make(map[string]int64)
	stDeletedNodesMap := make(map[string]int64)
	stDeletedBytesMap := make(map[string]int64)

	var totalAcWrittenNodes, totalAcWrittenBytes int64
	var totalAcDeletedNodes, totalAcDeletedBytes int64
	var totalStWrittenNodes, totalStWrittenBytes int64
	var totalStDeletedNodes, totalStDeletedBytes int64

	for i := range 65 {
		d := strconv.Itoa(i)
		if acCreated[i].Count > 0 || acCreated[i].Bytes > 0 {
			acWrittenNodes[d] = acCreated[i].Count
			acWrittenBytes[d] = acCreated[i].Bytes
			totalAcWrittenNodes += acCreated[i].Count
			totalAcWrittenBytes += acCreated[i].Bytes
		}
		if acDeleted[i].Count > 0 || acDeleted[i].Bytes > 0 {
			acDeletedNodes[d] = acDeleted[i].Count
			acDeletedBytes[d] = acDeleted[i].Bytes
			totalAcDeletedNodes += acDeleted[i].Count
			totalAcDeletedBytes += acDeleted[i].Bytes
		}
		if stCreated[i].Count > 0 || stCreated[i].Bytes > 0 {
			stWrittenNodes[d] = stCreated[i].Count
			stWrittenBytes[d] = stCreated[i].Bytes
			totalStWrittenNodes += stCreated[i].Count
			totalStWrittenBytes += stCreated[i].Bytes
		}
		if stDeleted[i].Count > 0 || stDeleted[i].Bytes > 0 {
			stDeletedNodesMap[d] = stDeleted[i].Count
			stDeletedBytesMap[d] = stDeleted[i].Bytes
			totalStDeletedNodes += stDeleted[i].Count
			totalStDeletedBytes += stDeleted[i].Bytes
		}
	}

	return map[string]any{
		"total_account_written_nodes": totalAcWrittenNodes,
		"total_account_written_bytes": totalAcWrittenBytes,
		"total_account_deleted_nodes": totalAcDeletedNodes,
		"total_account_deleted_bytes": totalAcDeletedBytes,
		"total_storage_written_nodes": totalStWrittenNodes,
		"total_storage_written_bytes": totalStWrittenBytes,
		"total_storage_deleted_nodes": totalStDeletedNodes,
		"total_storage_deleted_bytes": totalStDeletedBytes,
		"account_written_nodes":       acWrittenNodes,
		"account_written_bytes":       acWrittenBytes,
		"account_deleted_nodes":       acDeletedNodes,
		"account_deleted_bytes":       acDeletedBytes,
		"storage_written_nodes":       stWrittenNodes,
		"storage_written_bytes":       stWrittenBytes,
		"storage_deleted_nodes":       stDeletedNodesMap,
		"storage_deleted_bytes":       stDeletedBytesMap,
	}
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

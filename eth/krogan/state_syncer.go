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

package krogan

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
)

const (
	checkpointInterval = 1000 // Checkpoint every N accounts
	codeHashBatchSize  = 100  // Batch size for fetching bytecodes
)

// StateSnapshotSyncer downloads the full state snapshot from the master node
// for the configured account range. It runs in parallel with the gap filler.
type StateSnapshotSyncer struct {
	httpClients       []*rpc.Client
	db                *KroganDB
	gapFiller         *GapFiller
	accountRangeStart common.Hash
	accountRangeEnd   common.Hash

	syncBaseBlock   atomic.Uint64
	accountSyncDone chan struct{}
}

// NewStateSnapshotSyncer creates a new state snapshot syncer.
func NewStateSnapshotSyncer(
	httpClients []*rpc.Client,
	db *KroganDB,
	gapFiller *GapFiller,
	rangeStart, rangeEnd common.Hash,
) *StateSnapshotSyncer {
	return &StateSnapshotSyncer{
		httpClients:       httpClients,
		db:                db,
		gapFiller:         gapFiller,
		accountRangeStart: rangeStart,
		accountRangeEnd:   rangeEnd,
		accountSyncDone:   make(chan struct{}),
	}
}

// SyncState performs a full state snapshot sync from scratch.
func (s *StateSnapshotSyncer) SyncState(ctx context.Context) error {
	log.Info("Starting state snapshot sync", "rangeStart", s.accountRangeStart.Hex()[:10], "rangeEnd", s.accountRangeEnd.Hex()[:10])

	// Phase 1: Download first batch to get initial base block
	firstResult, err := s.callAccountRange(ctx, s.accountRangeStart)
	if err != nil {
		return fmt.Errorf("failed to fetch initial account range: %w", err)
	}

	// CRITICAL: Set initial base block and launch gap filler
	s.syncBaseBlock.Store(uint64(firstResult.BlockNumber))
	log.Info("State sync starting", "baseBlock", firstResult.BlockNumber)

	// Launch gap filler in parallel
	go s.gapFiller.ContinuousFill(ctx, &s.syncBaseBlock, s.accountSyncDone)

	// Phase 2: Download all accounts in range
	if err := s.downloadAccounts(ctx); err != nil {
		return fmt.Errorf("failed to download accounts: %w", err)
	}

	// Phase 3: Download storage for each account
	if err := s.downloadStorage(ctx); err != nil {
		return fmt.Errorf("failed to download storage: %w", err)
	}

	// Phase 4: Download bytecodes
	if err := s.downloadBytecodes(ctx); err != nil {
		return fmt.Errorf("failed to download bytecodes: %w", err)
	}

	// Signal gap filler that account sync is complete
	log.Info("Account sync complete, waiting for final gap fill")
	close(s.accountSyncDone)

	// Wait for gap filler to finish (with timeout)
	select {
	case <-s.gapFiller.Done():
		log.Info("Gap fill complete, ready for live sync")
	case <-time.After(5 * time.Minute):
		return errors.New("gap filler did not finish in time")
	case <-ctx.Done():
		return ctx.Err()
	}

	// Mark complete
	if err := s.db.SetSyncComplete(s.syncBaseBlock.Load()); err != nil {
		return fmt.Errorf("failed to mark sync complete: %w", err)
	}

	return nil
}

// ResumeSync resumes state sync from a checkpoint after a crash.
func (s *StateSnapshotSyncer) ResumeSync(ctx context.Context) error {
	// Read checkpoint from disk
	lastAccountHash, blockNum, err := s.db.LoadCheckpoint()
	if err != nil {
		return s.SyncState(ctx) // Fresh start if checkpoint doesn't exist
	}

	if lastAccountHash == (common.Hash{}) {
		// No checkpoint, start fresh
		return s.SyncState(ctx)
	}

	// Check if gap is too large to resume
	currentHead, err := s.getCurrentHead(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current head: %w", err)
	}

	gap := currentHead - blockNum
	if gap > 128 {
		log.Warn("Checkpoint too old, starting fresh sync", "gap", gap, "checkpoint", blockNum, "currentHead", currentHead)
		return s.SyncState(ctx)
	}

	// Resume from checkpoint
	log.Info("Resuming state sync from checkpoint",
		"lastAccount", lastAccountHash.Hex()[:10],
		"baseBlock", blockNum)

	s.syncBaseBlock.Store(blockNum)

	// Launch gap filler
	go s.gapFiller.ContinuousFill(ctx, &s.syncBaseBlock, s.accountSyncDone)

	// Continue downloading from checkpoint
	return s.downloadAccountsFrom(ctx, lastAccountHash)
}

// downloadAccounts downloads all accounts starting from accountRangeStart.
func (s *StateSnapshotSyncer) downloadAccounts(ctx context.Context) error {
	return s.downloadAccountsFrom(ctx, s.accountRangeStart)
}

// downloadAccountsFrom downloads accounts starting from a specific hash (for resume).
func (s *StateSnapshotSyncer) downloadAccountsFrom(ctx context.Context, startHash common.Hash) error {
	currentHash := startHash
	accountCount := 0

	log.Info("Downloading accounts", "startHash", startHash.Hex()[:10])

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		result, err := s.callAccountRange(ctx, currentHash)
		if err != nil {
			return fmt.Errorf("failed to call accountRange: %w", err)
		}

		if len(result.Accounts) == 0 {
			log.Info("No more accounts to fetch")
			break
		}

		// Note: result.BlockNumber may have advanced since last call!
		// This is normal and expected. Gap filler will handle it.

		for _, acc := range result.Accounts {
			// Filter: only store accounts in our range
			if !s.isInRange(acc.Hash) {
				continue
			}

			if err := s.db.WriteAccount(acc.Hash, acc.Val); err != nil {
				return fmt.Errorf("failed to write account: %w", err)
			}
			accountCount++

			// Checkpoint every 1000 accounts
			if accountCount%checkpointInterval == 0 {
				log.Info("State sync progress",
					"accounts", accountCount,
					"lastHash", acc.Hash.Hex()[:10],
					"baseBlock", result.BlockNumber)

				if err := s.db.UpdateCheckpoint(acc.Hash, uint64(result.BlockNumber)); err != nil {
					return fmt.Errorf("failed to update checkpoint: %w", err)
				}
			}
		}

		lastHash := result.Accounts[len(result.Accounts)-1].Hash

		// Check if we've exceeded our range
		if lastHash.Big().Cmp(s.accountRangeEnd.Big()) > 0 {
			log.Info("Reached end of account range", "lastHash", lastHash.Hex()[:10])
			break
		}

		// Move to next batch (lastHash + 1)
		currentHash = common.BigToHash(new(big.Int).Add(lastHash.Big(), common.Big1))
	}

	log.Info("Account download complete", "total", accountCount)
	return nil
}

// downloadStorage downloads storage slots for all accounts in our range.
func (s *StateSnapshotSyncer) downloadStorage(ctx context.Context) error {
	log.Info("Downloading storage for accounts in range")

	// We need to iterate through all accounts we've downloaded
	// For now, we'll download storage on-demand during live sync
	// This is a simplification - a full implementation would iterate through disk

	log.Info("Storage download deferred to on-demand fetching")
	return nil
}

// downloadBytecodes downloads contract bytecodes for all accounts.
func (s *StateSnapshotSyncer) downloadBytecodes(ctx context.Context) error {
	log.Info("Downloading bytecodes")

	// Similar to storage, bytecodes can be fetched on-demand
	// or we can iterate through accounts and batch-fetch code hashes

	log.Info("Bytecode download deferred to on-demand fetching")
	return nil
}

// callAccountRange calls krogan_accountRange on the master node.
func (s *StateSnapshotSyncer) callAccountRange(ctx context.Context, startHash common.Hash) (*ethapi.AccountRangeResult, error) {
	client := s.httpClients[0] // TODO: Round-robin or load balance

	var result ethapi.AccountRangeResult
	err := client.CallContext(ctx, &result, "krogan_accountRange", startHash)
	if err != nil {
		return nil, fmt.Errorf("krogan_accountRange failed: %w", err)
	}

	return &result, nil
}

// getCurrentHead gets the current head block number from master.
func (s *StateSnapshotSyncer) getCurrentHead(ctx context.Context) (uint64, error) {
	client := s.httpClients[0]

	var head hexutil.Big
	err := client.CallContext(ctx, &head, "eth_blockNumber")
	if err != nil {
		return 0, fmt.Errorf("eth_blockNumber failed: %w", err)
	}

	return head.ToInt().Uint64(), nil
}

// isInRange checks if an account hash is within our configured range.
func (s *StateSnapshotSyncer) isInRange(addrHash common.Hash) bool {
	return addrHash.Big().Cmp(s.accountRangeStart.Big()) >= 0 &&
		addrHash.Big().Cmp(s.accountRangeEnd.Big()) <= 0
}

// Copyright 2026 The go-ethereum Authors
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

package eip8188

import (
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
)

// progressInterval is how often the per-phase loops emit a heartbeat log.
// The injector is otherwise silent for the duration of a multi-hour mainnet
// run; without this, an operator can't tell stalled from working.
const progressInterval = 30 * time.Second

// DefaultBatchSize is the number of snapshot records rewritten per pebble batch.
// Bounded to keep peak memory small while still amortizing pebble's per-commit
// overhead; tune via Config.BatchSize if profiling shows otherwise.
const DefaultBatchSize = 10_000

// Config parameterizes a single Inject run.
type Config struct {
	// Source supplies the stream of most-recent writes. Required.
	Source Source

	// ForkBlock is the anchor block for period derivation. Blocks below this
	// value produce period=0.
	ForkBlock uint64

	// BlocksPerPeriod is the number of blocks in each period window.
	// Zero is rejected.
	BlocksPerPeriod uint64

	// EndBlock caps the block range the injector consumes. When zero, the
	// caller is expected to have no cap (Source decides). In practice the
	// CLI passes the chaindb head number here.
	EndBlock uint64

	// BatchSize overrides DefaultBatchSize when non-zero.
	BatchSize int

	// DryRun, when set, reads and counts diffs but does not mutate the DB.
	DryRun bool
}

// Stats summarises the outcome of an Inject run.
type Stats struct {
	AccountDiffsSeen        uint64
	StorageDiffsSeen        uint64
	AccountSnapshotsUpdated uint64
	StorageSnapshotsUpdated uint64
	AccountSnapshotsMissing uint64
	StorageSnapshotsMissing uint64
}

// Inject consumes diffs from cfg.Source and rewrites matching account and
// storage snapshot records in db with the computed EIP-8188 period.
func Inject(ctx context.Context, db ethdb.KeyValueStore, cfg Config) (Stats, error) {
	if cfg.Source == nil {
		return Stats{}, fmt.Errorf("eip8188: source is required")
	}
	if cfg.BlocksPerPeriod == 0 {
		return Stats{}, fmt.Errorf("eip8188: blocks-per-period must be > 0")
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	stats := Stats{}

	if err := injectAccounts(ctx, db, cfg, batchSize, &stats); err != nil {
		return stats, fmt.Errorf("inject accounts: %w", err)
	}
	if err := injectStorage(ctx, db, cfg, batchSize, &stats); err != nil {
		return stats, fmt.Errorf("inject storage: %w", err)
	}
	return stats, nil
}

func injectAccounts(ctx context.Context, db ethdb.KeyValueStore, cfg Config, batchSize int, stats *Stats) error {
	log.Info("inject-periods: account phase starting")
	stream, err := cfg.Source.AccountDiffs(ctx, cfg.ForkBlock, cfg.EndBlock)
	if err != nil {
		return err
	}
	batch := db.NewBatch()
	defer batch.Reset()

	phaseStart := time.Now()
	nextLog := phaseStart.Add(progressInterval)
	for diff := range stream {
		stats.AccountDiffsSeen++

		if now := time.Now(); !now.Before(nextLog) {
			elapsed := now.Sub(phaseStart).Seconds()
			rate := float64(stats.AccountDiffsSeen) / elapsed
			log.Info("inject-periods: account progress",
				"diffs-seen", stats.AccountDiffsSeen,
				"updated", stats.AccountSnapshotsUpdated,
				"missing", stats.AccountSnapshotsMissing,
				"latest-block", diff.Block,
				"diffs-per-sec", uint64(rate),
				"elapsed", common.PrettyDuration(now.Sub(phaseStart)),
			)
			nextLog = now.Add(progressInterval)
		}

		period := ComputePeriod(diff.Block, cfg.ForkBlock, cfg.BlocksPerPeriod)
		addrHash := crypto.Keccak256Hash(diff.Address[:])

		blob := rawdb.ReadAccountSnapshot(db, addrHash)
		if len(blob) == 0 {
			stats.AccountSnapshotsMissing++
			log.Debug("account snapshot not present; skipping",
				"address", diff.Address, "block", diff.Block)
			continue
		}

		var account types.SlimAccount
		if err := rlp.DecodeBytes(blob, &account); err != nil {
			return fmt.Errorf("decode snapshot for %x: %w", addrHash, err)
		}
		// Monotonic: never lower an existing period. Lets the source emit
		// diffs in any order (per-batch ARGMAX, multiple batches, retries)
		// without producing an incorrect final value.
		if account.LastWrittenPeriod >= period {
			continue
		}
		account.LastWrittenPeriod = period

		if cfg.DryRun {
			stats.AccountSnapshotsUpdated++
			continue
		}
		newBlob, err := rlp.EncodeToBytes(&account)
		if err != nil {
			return fmt.Errorf("encode snapshot for %x: %w", addrHash, err)
		}
		rawdb.WriteAccountSnapshot(batch, addrHash, newBlob)
		stats.AccountSnapshotsUpdated++

		if batch.ValueSize() >= batchSize*96 {
			if err := batch.Write(); err != nil {
				return err
			}
			batch.Reset()
		}
	}
	if !cfg.DryRun {
		if err := batch.Write(); err != nil {
			return err
		}
	}
	if err := cfg.Source.Err(); err != nil {
		return fmt.Errorf("account stream: %w", err)
	}
	return nil
}

func injectStorage(ctx context.Context, db ethdb.KeyValueStore, cfg Config, batchSize int, stats *Stats) error {
	log.Info("inject-periods: storage phase starting")
	stream, err := cfg.Source.StorageDiffs(ctx, cfg.ForkBlock, cfg.EndBlock)
	if err != nil {
		return err
	}
	batch := db.NewBatch()
	defer batch.Reset()

	var zeroHash common.Hash
	phaseStart := time.Now()
	nextLog := phaseStart.Add(progressInterval)
	for diff := range stream {
		stats.StorageDiffsSeen++

		if now := time.Now(); !now.Before(nextLog) {
			elapsed := now.Sub(phaseStart).Seconds()
			rate := float64(stats.StorageDiffsSeen) / elapsed
			log.Info("inject-periods: storage progress",
				"diffs-seen", stats.StorageDiffsSeen,
				"updated", stats.StorageSnapshotsUpdated,
				"missing", stats.StorageSnapshotsMissing,
				"latest-block", diff.Block,
				"diffs-per-sec", uint64(rate),
				"elapsed", common.PrettyDuration(now.Sub(phaseStart)),
			)
			nextLog = now.Add(progressInterval)
		}

		period := ComputePeriod(diff.Block, cfg.ForkBlock, cfg.BlocksPerPeriod)
		addrHash := crypto.Keccak256Hash(diff.Address[:])
		storageHash := crypto.Keccak256Hash(diff.Slot[:])

		blob := rawdb.ReadStorageSnapshot(db, addrHash, storageHash)
		if len(blob) == 0 {
			stats.StorageSnapshotsMissing++
			continue
		}

		value, existingPeriod, err := types.DecodeStorageSnapshotValue(blob)
		if err != nil {
			return fmt.Errorf("decode storage snapshot for %x/%x: %w", addrHash, storageHash, err)
		}
		if existingPeriod == period {
			continue // idempotent
		}
		if diff.Slot == zeroHash {
			// Defensive: Source is required to filter zero-clears, but if
			// somehow one slipped through we still tag whatever value is
			// present (not our concern here).
			_ = zeroHash
		}

		if cfg.DryRun {
			stats.StorageSnapshotsUpdated++
			continue
		}
		newBlob := types.EncodeStorageSnapshotValue(value, period)
		rawdb.WriteStorageSnapshot(batch, addrHash, storageHash, newBlob)
		stats.StorageSnapshotsUpdated++

		if batch.ValueSize() >= batchSize*96 {
			if err := batch.Write(); err != nil {
				return err
			}
			batch.Reset()
		}
	}
	if !cfg.DryRun {
		if err := batch.Write(); err != nil {
			return err
		}
	}
	if err := cfg.Source.Err(); err != nil {
		return fmt.Errorf("storage stream: %w", err)
	}
	return nil
}

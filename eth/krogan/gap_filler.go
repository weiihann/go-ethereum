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
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"
)

const (
	maxDiffBlocks     = 128 // Master node limit
	chunkSize         = 16  // Safe chunk size for gap filling
	fillInterval      = 10 * time.Second
	maxGapBeforeAbort = 100 // Abort state sync if gap reaches this
)

var ErrGapTooLarge = errors.New("gap too large, state diffs no longer available")

// GapFiller continuously fills the gap between the state snapshot base block
// and the current head by fetching block and state diffs from the master node.
type GapFiller struct {
	httpClients       []*rpc.Client
	db                *KroganDB
	chain             *ChainWindow
	accountRangeStart common.Hash
	accountRangeEnd   common.Hash

	done chan struct{} // Signals when gap filler is done
}

// NewGapFiller creates a new gap filler.
func NewGapFiller(
	httpClients []*rpc.Client,
	db *KroganDB,
	chain *ChainWindow,
	rangeStart, rangeEnd common.Hash,
) *GapFiller {
	return &GapFiller{
		httpClients:       httpClients,
		db:                db,
		chain:             chain,
		accountRangeStart: rangeStart,
		accountRangeEnd:   rangeEnd,
		done:              make(chan struct{}),
	}
}

// Done returns a channel that will be closed when the gap filler is done.
func (g *GapFiller) Done() <-chan struct{} {
	return g.done
}

// ContinuousFill runs in a goroutine, continuously filling gaps until signaled to stop.
func (g *GapFiller) ContinuousFill(ctx context.Context, syncBaseBlock *atomic.Uint64, accountSyncDone <-chan struct{}) {
	defer close(g.done)

	ticker := time.NewTicker(fillInterval)
	defer ticker.Stop()

	log.Info("Gap filler started")

	for {
		select {
		case <-ctx.Done():
			log.Info("Gap filler stopping due to context cancellation")
			return

		case <-ticker.C:
			// Periodic gap check and fill
			baseBlock := syncBaseBlock.Load()
			if baseBlock == 0 {
				// syncBaseBlock not yet initialized
				continue
			}

			if err := g.fillGapOnce(ctx, baseBlock, syncBaseBlock); err != nil {
				if errors.Is(err, ErrGapTooLarge) {
					log.Crit("Gap too large, aborting state sync", "err", err)
					// TODO: trigger full resync via callback
					return
				}
				log.Warn("Gap fill failed, will retry", "err", err)
			}

		case <-accountSyncDone:
			// Account sync is complete, do final fill and exit
			log.Info("Account sync complete signal received, performing final gap fill")

			baseBlock := syncBaseBlock.Load()
			if err := g.fillGapOnce(ctx, baseBlock, syncBaseBlock); err != nil {
				log.Error("Final gap fill failed", "err", err)
				return
			}

			log.Info("Gap filler finished successfully")
			return
		}
	}
}

// fillGapOnce performs a single gap fill iteration.
func (g *GapFiller) fillGapOnce(ctx context.Context, baseBlock uint64, syncBaseBlock *atomic.Uint64) error {
	// Get current head from master
	currentHead, err := g.getCurrentHead(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current head: %w", err)
	}

	gap := currentHead - baseBlock
	if gap == 0 {
		// No gap, we're synced
		return nil
	}

	if gap > maxDiffBlocks {
		return fmt.Errorf("%w: gap=%d blocks", ErrGapTooLarge, gap)
	}

	if gap > maxGapBeforeAbort {
		log.Warn("Gap is getting large, may need to abort soon", "gap", gap, "max", maxDiffBlocks)
	}

	log.Debug("Filling gap", "baseBlock", baseBlock, "currentHead", currentHead, "gap", gap)

	// Fill in chunks
	for current := baseBlock + 1; current <= currentHead; current += chunkSize {
		chunkEnd := current + chunkSize - 1
		if chunkEnd > currentHead {
			chunkEnd = currentHead
		}

		if err := g.FillChunk(ctx, current, chunkEnd); err != nil {
			return fmt.Errorf("failed to fill chunk [%d-%d]: %w", current, chunkEnd, err)
		}

		// Atomically update syncBaseBlock
		syncBaseBlock.Store(chunkEnd)
	}

	log.Info("Gap filled", "baseBlock", baseBlock, "newHead", currentHead, "filled", gap)
	return nil
}

// getCurrentHead gets the current head block number from master.
func (g *GapFiller) getCurrentHead(ctx context.Context) (uint64, error) {
	client := g.httpClients[0] // TODO: round-robin with failover

	var result hexutil.Uint64
	if err := client.CallContext(ctx, &result, "eth_blockNumber"); err != nil {
		return 0, err
	}

	return uint64(result), nil
}

// FillChunk fetches and processes a chunk of blocks with state diffs.
func (g *GapFiller) FillChunk(ctx context.Context, start, end uint64) error {
	client := g.httpClients[0] // TODO: round-robin

	var result *ethapi.BlockAndStateDiffsResult
	err := client.CallContext(ctx, &result, "krogan_blockAndStateDiffs",
		rpc.BlockNumber(start), rpc.BlockNumber(end))
	if err != nil {
		return fmt.Errorf("failed to fetch diffs: %w", err)
	}

	// Process each block
	for i, blockBytes := range result.Blocks {
		var block types.Block
		if err := rlp.DecodeBytes(blockBytes, &block); err != nil {
			return fmt.Errorf("failed to decode block: %w", err)
		}

		blockNum := block.NumberU64()

		// OPTIMIZATION: Check if block already exists in ChainWindow
		existingBlock, err := g.chain.GetBlockByNumber(blockNum)
		if err == nil && existingBlock.Hash() == block.Hash() {
			log.Debug("Block already in ChainWindow, skipping", "number", blockNum)
			continue // Block with same hash already exists, skip
		}

		var receipts types.Receipts
		if i < len(result.Receipts) {
			if err := rlp.DecodeBytes(result.Receipts[i], &receipts); err != nil {
				return fmt.Errorf("failed to decode receipts: %w", err)
			}
		}

		// Build block data with filtered state diffs
		blockData := &BlockData{
			Block:    &block,
			Receipts: receipts,
			Accounts: make(map[common.Hash]*types.SlimAccount),
			Storages: make(map[common.Hash]map[common.Hash]common.Hash),
			Codes:    make(map[common.Hash][]byte),
		}

		// Apply state diffs (filtered by account range)
		// CRITICAL: Check syncBaseBlock before writing to avoid overwriting newer state
		for addrHash, accBytes := range result.Accounts {
			if !g.isInRange(addrHash) {
				continue
			}

			var acc types.SlimAccount
			if err := rlp.DecodeBytes(accBytes, &acc); err != nil {
				return fmt.Errorf("failed to decode account: %w", err)
			}
			blockData.Accounts[addrHash] = &acc

			// Write to disk ONLY if this block's state is newer than what we have
			// syncBaseBlock tracks the latest finalized state we've downloaded
			if blockNum >= g.db.GetSyncBaseBlock() {
				if err := g.db.WriteAccountAtBlock(addrHash, accBytes, blockNum); err != nil {
					return err
				}
			}
		}

		for addrHash, storage := range result.Slots {
			if !g.isInRange(addrHash) {
				continue
			}

			blockData.Storages[addrHash] = make(map[common.Hash]common.Hash)
			for slotHash, slotBytes := range storage {
				var value common.Hash
				if err := rlp.DecodeBytes(slotBytes, &value); err != nil {
					return fmt.Errorf("failed to decode storage: %w", err)
				}
				blockData.Storages[addrHash][slotHash] = value

				// Write to disk ONLY if newer
				if blockNum >= g.db.GetSyncBaseBlock() {
					if err := g.db.WriteStorageAtBlock(addrHash, slotHash, slotBytes, blockNum); err != nil {
						return err
					}
				}
			}
		}

		// Note: Codes are not included in BlockAndStateDiffs response.
		// Codes are fetched separately on-demand when needed via krogan_bytecodes API.

		// Insert into ChainWindow (handles duplicates/reorgs internally)
		if err := g.chain.InsertBlock(blockData); err != nil {
			return fmt.Errorf("failed to insert block: %w", err)
		}
	}

	return nil
}

// isInRange checks if an account hash is within our configured range.
func (g *GapFiller) isInRange(addrHash common.Hash) bool {
	return addrHash.Big().Cmp(g.accountRangeStart.Big()) >= 0 &&
		addrHash.Big().Cmp(g.accountRangeEnd.Big()) <= 0
}

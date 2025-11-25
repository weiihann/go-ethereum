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
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"
)

// Downloader orchestrates the three phases of Krogan synchronization:
// 1. State Snapshot Sync - Download accounts/storage/codes in configured range
// 2. Gap Filling - Continuously fill gap between base block and current head (runs in parallel with phase 1)
// 3. Live Block Sync - Real-time WebSocket subscription for new blocks
type Downloader struct {
	wsURL       string
	wsClient    *rpc.Client
	httpClients []*rpc.Client

	db                  *KroganDB
	stack               StackCloser // Interface to allow node shutdown
	stateSnapshotSyncer *StateSnapshotSyncer
	gapFiller           *GapFiller
	accountRangeStart   common.Hash
	accountRangeEnd     common.Hash

	wg     sync.WaitGroup
	closed chan struct{}
}

// StackCloser defines the interface for closing the node stack
type StackCloser interface {
	Close() error
}

func NewDownloader(db *KroganDB, stack StackCloser, rangeStart, rangeEnd common.Hash) *Downloader {
	return &Downloader{
		db:                db,
		stack:             stack,
		accountRangeStart: rangeStart,
		accountRangeEnd:   rangeEnd,
		closed:            make(chan struct{}),
	}
}

func (d *Downloader) RegisterWSClient(wsURL string) error {
	client, err := rpc.DialWebsocket(context.Background(), wsURL, "")
	if err != nil {
		return err
	}
	d.wsURL = wsURL
	d.wsClient = client
	return nil
}

func (d *Downloader) RegisterHTTPClient(httpURL string) error {
	client, err := rpc.DialHTTP(httpURL)
	if err != nil {
		return err
	}
	d.httpClients = append(d.httpClients, client)
	return nil
}

func (d *Downloader) Start() error {
	log.Info("Starting Krogan Downloader")

	if len(d.httpClients) == 0 {
		return errors.New("no HTTP clients registered")
	}

	// Initialize GapFiller and StateSnapshotSyncer
	d.gapFiller = NewGapFiller(d.httpClients, d.db, d.db.chain, d.accountRangeStart, d.accountRangeEnd)
	d.stateSnapshotSyncer = NewStateSnapshotSyncer(d.httpClients, d.db, d.gapFiller, d.accountRangeStart, d.accountRangeEnd)

	d.wg.Add(1)
	go d.run()
	return nil
}

func (d *Downloader) Stop() error {
	log.Info("Stopping Krogan Downloader")
	close(d.closed)
	d.wg.Wait()
	log.Info("Krogan Downloader stopped")
	return nil
}

func (d *Downloader) run() {
	defer d.wg.Done()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Check if we need to perform initial state sync
	syncComplete, err := d.db.IsSyncComplete()
	if err != nil {
		log.Error("Failed to check sync status, shutting down node", "err", err)
		go d.stack.Close()
		return
	}

	if !syncComplete {
		log.Info("State snapshot not complete, starting sync")

		// Phase 1 & 2: State Snapshot Sync with continuous Gap Filling (in parallel)
		if err := d.stateSnapshotSyncer.SyncState(ctx); err != nil {
			log.Error("State snapshot sync failed, shutting down node", "err", err)
			go d.stack.Close()
			return
		}

		log.Info("State snapshot sync complete, transitioning to live sync")
	} else {
		log.Info("State snapshot already complete, checking for gap")

		// Load sync base block and check if we need to fill a gap
		baseBlock, err := d.db.LoadSyncBaseBlock()
		if err != nil {
			log.Error("Failed to load sync base block", "err", err)
			go d.stack.Close()
			return
		}

		// Get current head to check gap size
		currentHead, err := d.getCurrentHead(ctx)
		if err != nil {
			log.Error("Failed to get current head", "err", err)
			go d.stack.Close()
			return
		}

		gap := currentHead - baseBlock
		if gap > 0 && gap <= 128 {
			log.Info("Gap detected, filling before live sync", "gap", gap, "baseBlock", baseBlock, "currentHead", currentHead)

			// Fill the gap before starting live sync
			if err := d.fillGapBeforeLiveSync(ctx, baseBlock, currentHead); err != nil {
				log.Error("Failed to fill gap before live sync", "err", err)
				go d.stack.Close()
				return
			}
		} else if gap > 128 {
			log.Warn("Gap too large, performing full resync", "gap", gap)
			if err := d.stateSnapshotSyncer.SyncState(ctx); err != nil {
				log.Error("Full resync failed", "err", err)
				go d.stack.Close()
				return
			}
		}
	}

	// Phase 3: Live Block Sync
	log.Info("Starting live block sync via WebSocket")
	d.runLiveSync(ctx)
}

func (d *Downloader) runLiveSync(ctx context.Context) {
	blocks := make(chan *types.EncodedBlockWithStateUpdates)

	sub, err := d.wsClient.Subscribe(ctx, "eth", blocks, "stateUpdates")
	if err != nil {
		log.Error("Failed to subscribe to state updates, shutting down node", "err", err)
		go d.stack.Close()
		return
	}
	defer sub.Unsubscribe()

	log.Info("Live block sync active")

	for {
		select {
		case <-d.closed:
			log.Info("Live sync stopping")
			return
		case err := <-sub.Err():
			log.Error("Subscription error, shutting down node", "err", err)
			go d.stack.Close()
			return
		case b := <-blocks:
			if err := d.processBlock(b); err != nil {
				log.Error("Block processing error, shutting down node", "err", err)
				go d.stack.Close()
				return
			}
		}
	}
}

func (d *Downloader) fillGapBeforeLiveSync(ctx context.Context, baseBlock, currentHead uint64) error {
	log.Info("Filling gap before live sync", "start", baseBlock+1, "end", currentHead)

	for current := baseBlock + 1; current <= currentHead; current += 16 {
		chunkEnd := current + 15
		if chunkEnd > currentHead {
			chunkEnd = currentHead
		}

		if err := d.gapFiller.FillChunk(ctx, current, chunkEnd); err != nil {
			return err
		}

		if err := d.db.SetSyncBaseBlock(chunkEnd); err != nil {
			return err
		}
	}

	log.Info("Gap filled successfully", "newHead", currentHead)
	return nil
}

func (d *Downloader) getCurrentHead(ctx context.Context) (uint64, error) {
	client := d.httpClients[0]

	var result common.Hash
	if err := client.CallContext(ctx, &result, "eth_blockNumber"); err != nil {
		return 0, err
	}

	return result.Big().Uint64(), nil
}

func (s *Downloader) processBlock(enc *types.EncodedBlockWithStateUpdates) error {
	if len(enc.Block) == 0 {
		return errors.New("block is empty")
	}

	// 1. Decode the block from RLP
	var block types.Block
	if err := rlp.DecodeBytes(enc.Block, &block); err != nil {
		return err
	}
	log.Debug("debug(weiihann): decoded block", "block", block.NumberU64())

	var (
		storageReceipts []*types.ReceiptForStorage
		receipts        types.Receipts
		accounts        map[common.Hash]*types.SlimAccount
		storages        map[common.Hash]map[common.Hash]common.Hash
	)

	if len(enc.Receipts) > 0 {
		if err := rlp.DecodeBytes(enc.Receipts, &storageReceipts); err != nil {
			return err
		}
		log.Debug("debug(weiihann): decoded storage receipts", "receipts", len(storageReceipts))

		receipts = make(types.Receipts, len(storageReceipts))
		for i, receipt := range storageReceipts {
			receipts[i] = (*types.Receipt)(receipt)
		}
		log.Debug("debug(weiihann): decoded receipts", "receipts", len(receipts))
	}

	if len(enc.Accounts) > 0 {
		accounts = make(map[common.Hash]*types.SlimAccount, len(enc.Accounts))
		for hash, encAcc := range enc.Accounts {
			account := new(types.SlimAccount)
			if err := rlp.DecodeBytes(encAcc, account); err != nil {
				log.Debug("debug(weiihann): error decoding account", "encAcc", encAcc)
				return err
			}
			accounts[hash] = account
		}
		log.Debug("debug(weiihann): decoded accounts", "accounts", len(accounts))
	}

	if len(enc.Storages) > 0 {
		storages = make(map[common.Hash]map[common.Hash]common.Hash, len(enc.Storages))
		for addrHash, storage := range enc.Storages {
			slots := make(map[common.Hash]common.Hash)
			for hash, encSlot := range storage {
				var val common.Hash
				if len(encSlot) == 0 {
					val = common.Hash{}
				} else {
					_, content, _, err := rlp.Split(encSlot)
					if err != nil {
						return err
					}
					val = common.BytesToHash(content)
				}
				slots[hash] = val
			}
			storages[addrHash] = slots
		}
		log.Debug("debug(weiihann): decoded storages", "storages", len(storages))
	}

	data := &BlockData{
		Block:    &block,
		Receipts: receipts,
		Accounts: accounts,
		Storages: storages,
		Codes:    enc.Codes,
	}

	if err := s.db.InsertBlock(data); err != nil {
		return err
	}

	log.Info("Inserted block", "block", block.NumberU64())

	return nil
}

func (s *Downloader) Progress() {
	panic("TODO(weiihann): implement")
}

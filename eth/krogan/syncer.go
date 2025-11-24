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

type Syncer struct {
	wsURL       string
	httpURLs    []string
	wsClient    *rpc.Client
	httpClients []*rpc.Client

	db    *KroganDB
	stack StackCloser // Interface to allow node shutdown

	wg     sync.WaitGroup
	closed chan struct{}
}

// StackCloser defines the interface for closing the node stack
type StackCloser interface {
	Close() error
}

func NewSyncer(db *KroganDB, stack StackCloser) *Syncer {
	return &Syncer{
		db:     db,
		stack:  stack,
		closed: make(chan struct{}),
	}
}

func (s *Syncer) RegisterWSClient(wsURL string) error {
	client, err := rpc.DialWebsocket(context.Background(), wsURL, "")
	if err != nil {
		return err
	}
	s.wsURL = wsURL
	s.wsClient = client
	return nil
}

func (s *Syncer) RegisterHTTPClient(httpURL string) error {
	client, err := rpc.DialHTTP(httpURL)
	if err != nil {
		return err
	}
	s.httpURLs = append(s.httpURLs, httpURL)
	s.httpClients = append(s.httpClients, client)
	return nil
}

func (s *Syncer) Start() error {
	log.Info("Starting Krogan syncer")
	s.wg.Add(1)
	go s.run()
	return nil
}

func (s *Syncer) Stop() error {
	log.Debug("debug(weiihann): stopping syncer")
	close(s.closed)
	s.wg.Wait()
	log.Info("Krogan syncer stopped")
	return nil
}

func (s *Syncer) run() {
	log.Debug("debug(weiihann): running syncer")
	defer s.wg.Done()

	blocks := make(chan *types.EncodedBlockWithStateUpdates)

	log.Debug("debug(weiihann): subscribing to state updates")
	sub, err := s.wsClient.Subscribe(context.Background(), "eth", blocks, "stateUpdates")
	if err != nil {
		log.Error("Failed to subscribe to state updates, shutting down node", "err", err)
		go s.stack.Close() // async since we need to close ourselves
		return
	}
	defer sub.Unsubscribe()

	log.Debug("running syncer loop")
	for {
		select {
		case <-s.closed:
			log.Debug("debug(weiihann): closing syncer")
			return
		case err := <-sub.Err():
			log.Error("Subscription error, shutting down node", "err", err)
			go s.stack.Close() // async since we need to close ourselves
			return
		case b := <-blocks:
			if err := s.processBlock(b); err != nil {
				log.Error("Block processing error, shutting down node", "err", err)
				go s.stack.Close() // async since we need to close ourselves
				return
			}
		}
	}
}

func (s *Syncer) processBlock(enc *types.EncodedBlockWithStateUpdates) error {
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

func (s *Syncer) Progress() {
	panic("TODO(weiihann): implement")
}

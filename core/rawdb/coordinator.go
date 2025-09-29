// Copyright 2018 The go-ethereum Authors
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

package rawdb

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

type iteratorType int

const (
	// iteratorAccounts iteratorType = iota
	iteratorStorage iteratorType = iota
	// iteratorAccountNodes iteratorType = iota
	// iteratorStorageNodes
	numIterators = 1
)

type batchResult struct {
	iterType iteratorType
	batch    ethdb.Batch
	hasMore  bool
	err      error
}

type iteratorState struct {
	start   []byte
	hasMore bool
}

type batchCoordinator struct {
	sourceDB  ethdb.KeyValueStore
	targetDB  ethdb.KeyValueStore
	batchSize int
	count     *atomic.Uint64

	// Iterator states
	iterStates [numIterators]*iteratorState

	// Synchronization
	barrier      sync.WaitGroup
	batchResults chan batchResult
	ctx          context.Context
	cancel       context.CancelFunc
}

func newBatchCoordinator(sourceDB, targetDB ethdb.KeyValueStore, batchSize int, count *atomic.Uint64) *batchCoordinator {
	ctx, cancel := context.WithCancel(context.Background())

	coord := &batchCoordinator{
		sourceDB:     sourceDB,
		targetDB:     targetDB,
		batchSize:    batchSize,
		count:        count,
		batchResults: make(chan batchResult, numIterators),
		ctx:          ctx,
		cancel:       cancel,
	}

	// Initialize iterator states
	for i := 0; i < numIterators; i++ {
		coord.iterStates[i] = &iteratorState{
			start:   []byte{},
			hasMore: true,
		}
	}

	return coord
}

func (c *batchCoordinator) run() error {
	defer c.cancel()

	for {
		// Check if all iterators are done
		allDone := true
		for i := 0; i < numIterators; i++ {
			if c.iterStates[i].hasMore {
				allDone = false
				break
			}
		}
		if allDone {
			break
		}

		// Phase 1: Launch 4 concurrent iterators to collect batches
		// Each iterator processes batchSize items
		c.barrier.Add(numIterators)

		for i := 0; i < numIterators; i++ {
			iterType := iteratorType(i)
			state := c.iterStates[i]

			if !state.hasMore {
				// This iterator is done, just signal completion
				c.barrier.Done()
				continue
			}

			go c.runIterator(iterType, state)
		}

		// Wait for all iterators to finish collecting their batch
		c.barrier.Wait()

		// Phase 2: Collect results from all iterators
		batches := make([]ethdb.Batch, 0, numIterators)
		var activeIterators int

		for i := 0; i < numIterators; i++ {
			select {
			case result := <-c.batchResults:
				if result.err != nil {
					return result.err
				}
				if result.batch != nil && result.batch.ValueSize() > 0 {
					batches = append(batches, result.batch)
				}
				// Update iterator state with next position
				c.iterStates[result.iterType].hasMore = result.hasMore
				if result.hasMore {
					activeIterators++
				}
			case <-c.ctx.Done():
				return c.ctx.Err()
			}
		}

		// Phase 3: Commit all batches together
		// All iterators are blocked here waiting for commits to complete
		if err := c.commitBatches(batches); err != nil {
			return err
		}

		log.Info("Batch iteration complete", "active_iterators", activeIterators, "batches_committed", len(batches))

		// Now the cycle repeats - iterators can proceed with the next batch
		// but they won't start until all commits from this round are done
	}

	return nil
}

func (c *batchCoordinator) runIterator(iterType iteratorType, state *iteratorState) {
	defer c.barrier.Done()

	var (
		batch   ethdb.Batch
		hasMore bool
		err     error
	)

	switch iterType {
	// case iteratorAccounts:
	// 	batch, hasMore, err = c.collectAccountsBatch(state.start)
	case iteratorStorage:
		batch, hasMore, err = c.collectStorageBatch(state.start)
		// case iteratorAccountNodes:
		// 	batch, hasMore, err = c.collectAccountNodeBatch(state.start)
		// case iteratorStorageNodes:
		// 	batch, hasMore, err = c.collectStorageNodeBatch(state.start)
	}

	select {
	case c.batchResults <- batchResult{
		iterType: iterType,
		batch:    batch,
		hasMore:  hasMore,
		err:      err,
	}:
	case <-c.ctx.Done():
		return
	}
}

func (c *batchCoordinator) commitBatches(batches []ethdb.Batch) error {
	for _, batch := range batches {
		if batch == nil {
			continue
		}
		if err := batch.Write(); err != nil {
			return err
		}
		log.Info("Committed batch", "size", batch.ValueSize())
	}
	return nil
}

// func (c *batchCoordinator) collectAccountsBatch(start []byte) (ethdb.Batch, bool, error) {
// 	it := c.targetDB.NewIterator(SnapshotAccountPrefix, start)
// 	defer it.Release()

// 	batch := c.targetDB.NewBatch()
// 	count := 0
// 	var lastKey []byte

// 	for it.Next() {
// 		key := it.Key()
// 		addrHash := key[len(SnapshotAccountPrefix):]

// 		if !HasAccessAccount(c.sourceDB, common.BytesToHash(addrHash)) {
// 			if err := batch.Delete(key); err != nil {
// 				return nil, false, err
// 			}
// 		}

// 		lastKey = key[len(SnapshotAccountPrefix):]
// 		count++
// 		c.count.Add(1)

// 		if count >= c.batchSize {
// 			// Update the start position for next batch
// 			c.iterStates[iteratorAccounts].start = lastKey
// 			return batch, true, nil
// 		}

// 		select {
// 		case <-c.ctx.Done():
// 			return nil, false, c.ctx.Err()
// 		default:
// 		}
// 	}

// 	log.Info("End of accounts batch", "count", count)
// 	return batch, false, it.Error()
// }

func (c *batchCoordinator) collectStorageBatch(start []byte) (ethdb.Batch, bool, error) {
	it := c.targetDB.NewIterator(SnapshotStoragePrefix, start)
	defer it.Release()

	batch := c.targetDB.NewBatch()
	count := 0
	var lastKey []byte

	for it.Next() {
		key := it.Key()
		k := key[len(SnapshotStoragePrefix):]

		if !HasAccessSlot(c.sourceDB, common.BytesToHash(k[:common.HashLength]), common.BytesToHash(k[common.HashLength:])) {
			if err := batch.Delete(key); err != nil {
				return nil, false, err
			}
		}

		lastKey = k
		count++
		c.count.Add(1)

		if count >= c.batchSize {
			// Update the start position for next batch
			c.iterStates[iteratorStorage].start = lastKey
			return batch, true, nil
		}

		select {
		case <-c.ctx.Done():
			return nil, false, c.ctx.Err()
		default:
		}
	}

	log.Info("End of storage batch", "count", count)
	return batch, false, it.Error()
}

// func (c *batchCoordinator) collectAccountNodeBatch(start []byte) (ethdb.Batch, bool, error) {
// 	it := c.targetDB.NewIterator(TrieNodeAccountPrefix, start)
// 	defer it.Release()

// 	batch := c.targetDB.NewBatch()
// 	count := 0
// 	var lastKey []byte

// 	for it.Next() {
// 		key := it.Key()
// 		accPath := key[len(TrieNodeAccountPrefix):]

// 		if !HasAccessNodeAccount(c.sourceDB, accPath) {
// 			if err := batch.Delete(key); err != nil {
// 				return nil, false, err
// 			}
// 		}

// 		lastKey = accPath
// 		count++
// 		c.count.Add(1)

// 		if count >= c.batchSize {
// 			// Update the start position for next batch
// 			c.iterStates[iteratorAccountNodes].start = lastKey
// 			return batch, true, nil
// 		}

// 		select {
// 		case <-c.ctx.Done():
// 			return nil, false, c.ctx.Err()
// 		default:
// 		}
// 	}

// 	log.Info("End of account nodes batch", "count", count)
// 	return batch, false, it.Error()
// }

// func (c *batchCoordinator) collectStorageNodeBatch(start []byte) (ethdb.Batch, bool, error) {
// 	it := c.targetDB.NewIterator(TrieNodeStoragePrefix, start)
// 	defer it.Release()

// 	batch := c.targetDB.NewBatch()
// 	count := 0
// 	var lastKey []byte

// 	for it.Next() {
// 		key := it.Key()
// 		k := key[len(TrieNodeStoragePrefix):]
// 		addrHash := k[:common.HashLength]
// 		path := k[common.HashLength:]

// 		if !HasAccessNodeSlot(c.sourceDB, common.BytesToHash(addrHash), path) {
// 			if err := batch.Delete(key); err != nil {
// 				return nil, false, err
// 			}
// 		}

// 		lastKey = k
// 		count++
// 		c.count.Add(1)

// 		if count >= c.batchSize {
// 			// Update the start position for next batch
// 			c.iterStates[iteratorStorageNodes].start = lastKey
// 			return batch, true, nil
// 		}

// 		select {
// 		case <-c.ctx.Done():
// 			return nil, false, c.ctx.Err()
// 		default:
// 		}
// 	}

// 	log.Info("End of storage nodes batch", "count", count)
// 	return batch, false, it.Error()
// }

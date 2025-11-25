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

package ethapi

import (
	"context"
	"errors"
	"fmt"
	"maps"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"
)

const (
	accRangeLimitBytes     = 10 * 1024 * 1024 // 10MB
	storageRangeLimitBytes = 10 * 1024 * 1024 // 10MB
	codeLimitBytes         = 10 * 1024 * 1024 // 10MB
	maxStateDiffBlocks     = 128
)

type KroganAPI struct {
	b Backend
}

func NewKroganAPI(b Backend) *KroganAPI {
	return &KroganAPI{b}
}

type AccountData struct {
	Hash common.Hash  `json:"addrHash"`
	Val  rlp.RawValue `json:"val"`
}

type AccountRangeResult struct {
	BlockNumber hexutil.Uint64 `json:"blockNumber"`
	Accounts    []*AccountData `json:"accounts"`
}

func (api *KroganAPI) AccountRange(ctx context.Context, startHash common.Hash) (*AccountRangeResult, error) {
	head, err := api.b.HeaderByNumber(ctx, rpc.FinalizedBlockNumber)
	if err != nil {
		return nil, err
	}

	state, _, err := api.b.StateAndHeaderByNumber(ctx, rpc.FinalizedBlockNumber)
	if err != nil {
		return nil, err
	}

	accIt, err := state.Database().Snapshot().AccountIterator(head.Root, startHash)
	if err != nil {
		return nil, err
	}
	defer accIt.Release()

	var (
		accounts []*AccountData
		size     uint64
	)

	for accIt.Next() {
		hash, account := accIt.Hash(), common.CopyBytes(accIt.Account())
		accounts = append(accounts, &AccountData{
			Hash: hash,
			Val:  account,
		})
		size += uint64(common.HashLength + len(account))
		if size > accRangeLimitBytes {
			break
		}
	}

	return &AccountRangeResult{
		BlockNumber: hexutil.Uint64(head.Number.Uint64()),
		Accounts:    accounts,
	}, nil
}

type StorageData struct {
	Hash common.Hash  `json:"slotHash"`
	Val  rlp.RawValue `json:"val"`
}

type StorageRangeResult struct {
	BlockNumber hexutil.Uint64 `json:"blockNumber"`
	Slots       []*StorageData `json:"storage"`
}

func (api *KroganAPI) StorageRange(ctx context.Context, addrHash common.Hash, startHash common.Hash) (*StorageRangeResult, error) {
	head, err := api.b.HeaderByNumber(ctx, rpc.FinalizedBlockNumber)
	if err != nil {
		return nil, err
	}

	state, _, err := api.b.StateAndHeaderByNumber(ctx, rpc.FinalizedBlockNumber)
	if err != nil {
		return nil, err
	}

	storageIt, err := state.Database().Snapshot().StorageIterator(head.Root, addrHash, startHash)
	if err != nil {
		return nil, err
	}
	defer storageIt.Release()

	var (
		slots []*StorageData
		size  uint64
	)

	for storageIt.Next() {
		hash, slot := storageIt.Hash(), common.CopyBytes(storageIt.Slot())
		slots = append(slots, &StorageData{
			Hash: hash,
			Val:  slot,
		})
		size += uint64(common.HashLength + len(slot))
		if size > storageRangeLimitBytes {
			break
		}
	}

	return &StorageRangeResult{
		BlockNumber: hexutil.Uint64(head.Number.Uint64()),
		Slots:       slots,
	}, nil
}

type BytecodeResult struct {
	Codes map[common.Hash][]byte `json:"codes"`
}

func (api *KroganAPI) Bytecodes(ctx context.Context, codeHashes []common.Hash) (*BytecodeResult, error) {
	head, err := api.b.HeaderByNumber(ctx, rpc.FinalizedBlockNumber)
	if err != nil {
		return nil, err
	}

	state, _, err := api.b.StateAndHeaderByNumber(ctx, rpc.FinalizedBlockNumber)
	if err != nil {
		return nil, err
	}

	codeReader, err := state.Database().Reader(head.Root)
	if err != nil {
		return nil, err
	}

	codes := make(map[common.Hash][]byte, len(codeHashes))
	for _, codeHash := range codeHashes {
		code, err := codeReader.Code(common.Address{}, codeHash)
		if err != nil {
			continue
		}
		codes[codeHash] = code
		if len(code) > codeLimitBytes {
			break
		}
	}

	return &BytecodeResult{Codes: codes}, nil
}

type BlockAndStateDiffsResult struct {
	Blocks   [][]byte                               `json:"blocks"`
	Receipts [][]byte                               `json:"receipts"`
	Accounts map[common.Hash][]byte                 `json:"accounts"`
	Slots    map[common.Hash]map[common.Hash][]byte `json:"storage"`
}

// TODO(weiihann): deal with some limitations here:
// - reached disk layer?
func (api *KroganAPI) BlockAndStateDiffs(ctx context.Context, startBlock, endBlock rpc.BlockNumber) (*BlockAndStateDiffsResult, error) {
	// Ensure startBlock is less than endBlock
	if startBlock > endBlock {
		return nil, errors.New("start block must be less than or equal to end block")
	}

	var (
		resAccounts = make(map[common.Hash][]byte)
		resSlots    = make(map[common.Hash]map[common.Hash][]byte)
		resBlocks   = make([][]byte, 0, endBlock-startBlock+1)
		resReceipts = make([][]byte, 0, endBlock-startBlock+1)
	)

	for b := startBlock; b <= endBlock; b++ {
		block, err := api.b.BlockByNumber(ctx, b)
		if err != nil {
			return nil, err
		}

		receipts, err := api.b.GetReceipts(ctx, block.Hash())
		if err != nil {
			return nil, err
		}

		encBlock, err := rlp.EncodeToBytes(block)
		if err != nil {
			return nil, err
		}

		storageReceipts := make([]*types.ReceiptForStorage, len(receipts))
		for i, receipt := range receipts {
			storageReceipts[i] = (*types.ReceiptForStorage)(receipt)
		}
		encReceipts, err := rlp.EncodeToBytes(storageReceipts)
		if err != nil {
			return nil, err
		}

		resBlocks = append(resBlocks, encBlock)
		resReceipts = append(resReceipts, encReceipts)

		state, header, err := api.b.StateAndHeaderByNumber(ctx, b)
		if err != nil {
			return nil, err
		}

		diff := state.Database().Snapshot().Diff(header.Root)
		if diff == nil {
			return nil, fmt.Errorf("no state diff for block %d", b)
		}

		for addrHash, account := range diff.AccountData() {
			if account == nil {
				continue
			}
			resAccounts[addrHash] = account
		}

		for addrHash, storage := range diff.StorageData() {
			if _, ok := resSlots[addrHash]; !ok {
				resSlots[addrHash] = make(map[common.Hash][]byte)
			}
			maps.Copy(resSlots[addrHash], storage)
		}
	}

	return &BlockAndStateDiffsResult{
		Blocks:   resBlocks,
		Receipts: resReceipts,
		Accounts: resAccounts,
		Slots:    resSlots,
	}, nil
}

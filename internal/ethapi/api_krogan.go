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
	"maps"
	"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
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
	head, err := api.b.HeaderByNumber(ctx, rpc.LatestBlockNumber)
	if err != nil {
		return nil, err
	}

	state, _, err := api.b.StateAndHeaderByNumber(ctx, rpc.LatestBlockNumber)
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
	head, err := api.b.HeaderByNumber(ctx, rpc.LatestBlockNumber)
	if err != nil {
		return nil, err
	}

	state, _, err := api.b.StateAndHeaderByNumber(ctx, rpc.LatestBlockNumber)
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
	head, err := api.b.HeaderByNumber(ctx, rpc.LatestBlockNumber)
	if err != nil {
		return nil, err
	}

	state, _, err := api.b.StateAndHeaderByNumber(ctx, rpc.LatestBlockNumber)
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

type StateDiffsResult struct {
	Accounts map[common.Hash][]byte                 `json:"accounts"`
	Slots    map[common.Hash]map[common.Hash][]byte `json:"storage"`
}

// TODO(weiihann): deal with some limitations here:
// - reached disk layer?
func (api *KroganAPI) StateDiffs(ctx context.Context, blocks []rpc.BlockNumberOrHash) (*StateDiffsResult, error) {
	accounts := make(map[common.Hash][]byte)
	slots := make(map[common.Hash]map[common.Hash][]byte)

	if len(blocks) > maxStateDiffBlocks {
		return nil, errors.New("max number of blocks exceeded")
	}

	// Sort blocks in ascending order
	sort.Slice(blocks, func(i, j int) bool {
		blockI, ok := blocks[i].Number()
		if !ok {
			return false
		}
		blockJ, ok := blocks[j].Number()
		if !ok {
			return true
		}
		return blockI < blockJ
	})

	for _, block := range blocks {
		state, head, err := api.b.StateAndHeaderByNumberOrHash(ctx, block)
		if err != nil {
			return nil, err
		}

		diff := state.Database().Snapshot().Diff(head.Root)
		if diff == nil {
			continue
		}

		maps.Copy(accounts, diff.AccountData())

		for addrHash, storage := range diff.StorageData() {
			if _, ok := slots[addrHash]; !ok {
				slots[addrHash] = make(map[common.Hash][]byte)
			}
			maps.Copy(slots[addrHash], storage)
		}
	}

	return &StateDiffsResult{
		Accounts: accounts,
		Slots:    slots,
	}, nil
}

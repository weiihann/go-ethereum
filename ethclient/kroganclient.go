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

package ethclient

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/rpc"
)

func (ec *Client) AccountRange(ctx context.Context, startHash common.Hash) (*ethapi.AccountRangeResult, error) {
	var result *ethapi.AccountRangeResult
	err := ec.c.CallContext(ctx, &result, "krogan_accountRange", startHash)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (ec *Client) StorageRange(ctx context.Context, addrHash common.Hash, startHash common.Hash) (*ethapi.StorageRangeResult, error) {
	var result *ethapi.StorageRangeResult
	err := ec.c.CallContext(ctx, &result, "krogan_storageRange", addrHash, startHash)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (ec *Client) Bytecodes(ctx context.Context, codeHashes []common.Hash) (*ethapi.BytecodeResult, error) {
	var result *ethapi.BytecodeResult
	err := ec.c.CallContext(ctx, &result, "krogan_bytecodes", codeHashes)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (ec *Client) StateDiffs(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*ethapi.StateDiffResult, error) {
	var result *ethapi.StateDiffResult
	err := ec.c.CallContext(ctx, &result, "krogan_stateDiffs", blockNrOrHash)
	if err != nil {
		return nil, err
	}
	return result, nil
}

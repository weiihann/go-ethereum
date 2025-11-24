package kroganclient

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"
)

type Client struct {
	url string
	c   *rpc.Client
}

func New(url string, c *rpc.Client) *Client {
	return &Client{url: url, c: c}
}

func (c *Client) AccountRange(ctx context.Context, startHash common.Hash) (hexutil.Uint64, map[common.Hash]*types.SlimAccount, error) {
	var callRes *ethapi.AccountRangeResult
	err := c.c.CallContext(ctx, &callRes, "krogan_accountRange", startHash)
	if err != nil {
		return 0, nil, err
	}

	accounts := make(map[common.Hash]*types.SlimAccount, len(callRes.Accounts))
	for _, data := range callRes.Accounts {
		acc := new(types.SlimAccount)
		if err := rlp.DecodeBytes(data.Val, acc); err != nil {
			return 0, nil, err
		}
		accounts[data.Hash] = acc
	}

	return callRes.BlockNumber, accounts, nil
}

func (c *Client) StorageRange(ctx context.Context, addrHash common.Hash, startHash common.Hash) (hexutil.Uint64, map[common.Hash]common.Hash, error) {
	var callRes *ethapi.StorageRangeResult
	err := c.c.CallContext(ctx, &callRes, "krogan_storageRange", addrHash, startHash)
	if err != nil {
		return 0, nil, err
	}

	storage := make(map[common.Hash]common.Hash, len(callRes.Slots))
	for _, data := range callRes.Slots {
		var slot common.Hash
		if err := rlp.DecodeBytes(data.Val, &slot); err != nil {
			return 0, nil, err
		}
		storage[data.Hash] = slot
	}

	return callRes.BlockNumber, storage, nil
}

func (c *Client) Bytecodes(ctx context.Context, codeHashes []common.Hash) (map[common.Hash][]byte, error) {
	var result *ethapi.BytecodeResult
	err := c.c.CallContext(ctx, &result, "krogan_bytecodes", codeHashes)
	if err != nil {
		return nil, err
	}

	return result.Codes, nil
}

// TODO(weiihann): handle the decoding part. THe receipts seem iffy
func (c *Client) BlockAndStateDiffs(ctx context.Context, startBlock, endBlock rpc.BlockNumber) (*ethapi.BlockAndStateDiffsResult, error) {
	var result *ethapi.BlockAndStateDiffsResult
	err := c.c.CallContext(ctx, &result, "krogan_blockAndStateDiffs", startBlock, endBlock)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) Reconnect() error {
	client, err := rpc.DialHTTP(c.url)
	if err != nil {
		return err
	}
	c.c = client
	return nil
}

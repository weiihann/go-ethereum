package kroganclient

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/rpc"
)

type Client struct {
	c *rpc.Client
}

func New(c *rpc.Client) *Client {
	return &Client{c}
}

func (c *Client) AccountRange(ctx context.Context, startHash common.Hash) (*ethapi.AccountRangeResult, error) {
	var result *ethapi.AccountRangeResult
	err := c.c.CallContext(ctx, &result, "krogan_accountRange", startHash)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) StorageRange(ctx context.Context, addrHash common.Hash, startHash common.Hash) (*ethapi.StorageRangeResult, error) {
	var result *ethapi.StorageRangeResult
	err := c.c.CallContext(ctx, &result, "krogan_storageRange", addrHash, startHash)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) Bytecodes(ctx context.Context, codeHashes []common.Hash) (*ethapi.BytecodeResult, error) {
	var result *ethapi.BytecodeResult
	err := c.c.CallContext(ctx, &result, "krogan_bytecodes", codeHashes)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) BlockAndStateDiffs(ctx context.Context, startBlock, endBlock rpc.BlockNumber) (*ethapi.BlockAndStateDiffsResult, error) {
	var result *ethapi.BlockAndStateDiffsResult
	err := c.c.CallContext(ctx, &result, "krogan_blockAndStateDiffs", startBlock, endBlock)
	if err != nil {
		return nil, err
	}
	return result, nil
}

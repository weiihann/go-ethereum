package krogan

import (
	"context"
	"errors"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/filtermaps"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rpc"
)

var (
	ErrAPIBackendFunctionNotSupported = errors.New("api backend function not supported by krogan")
	ErrHeaderNotFound                 = errors.New("header not found")
	ErrInvalidArguments               = errors.New("invalid arguments; neither block nor hash specified")
)

type APIBackend struct {
	chain *ChainWindow
	db    *KroganDB
}

func NewAPIBackend(chain *ChainWindow, db *KroganDB) *APIBackend {
	return &APIBackend{
		chain: chain,
		db:    db,
	}
}

func (b *APIBackend) SyncProgress(ctx context.Context) ethereum.SyncProgress {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) SuggestGasTipCap(ctx context.Context) (*big.Int, error) {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) FeeHistory(ctx context.Context, blockCount uint64, lastBlock rpc.BlockNumber, rewardPercentiles []float64) (*big.Int, [][]*big.Int, []*big.Int, []float64, []*big.Int, []float64, error) {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) BlobBaseFee(ctx context.Context) *big.Int {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) ChainDb() ethdb.Database {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) AccountManager() *accounts.Manager {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) ExtRPCEnabled() bool {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) RPCGasCap() uint64 {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) RPCEVMTimeout() time.Duration {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) RPCTxFeeCap() float64 {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) UnprotectedAllowed() bool {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) RPCTxSyncDefaultTimeout() time.Duration {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) RPCTxSyncMaxTimeout() time.Duration {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) SetHead(number uint64) {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) HeaderByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Header, error) {
	switch number {
	case rpc.LatestBlockNumber:
		return b.chain.CurrentHeader()
	case rpc.FinalizedBlockNumber:
		panic("TODO(weiihann):not implemented")
	case rpc.EarliestBlockNumber:
		panic("TODO(weiihann):not implemented")
	case rpc.SafeBlockNumber:
		panic("TODO(weiihann):not implemented")
	case rpc.PendingBlockNumber:
		panic("TODO(weiihann):not implemented")
	}

	bn := uint64(number)
	return b.chain.GetHeaderByNumber(bn)
}

func (b *APIBackend) HeaderByHash(ctx context.Context, hash common.Hash) (*types.Header, error) {
	return b.chain.GetHeaderByHash(hash)
}

// TODO(weiihann): check if need to deal with canonical
func (b *APIBackend) HeaderByNumberOrHash(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*types.Header, error) {
	if blockNr, ok := blockNrOrHash.Number(); ok {
		return b.HeaderByNumber(ctx, blockNr)
	}
	if hash, ok := blockNrOrHash.Hash(); ok {
		return b.HeaderByHash(ctx, hash)
	}
	return nil, errors.New("invalid arguments; neither block nor hash specified")
}

func (b *APIBackend) CurrentHeader() *types.Header {
	header, _ := b.chain.CurrentHeader()
	return header
}

func (b *APIBackend) CurrentBlock() *types.Header {
	block, _ := b.chain.CurrentBlock()
	return block.Header()
}

func (b *APIBackend) BlockByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Block, error) {
	switch number {
	case rpc.LatestBlockNumber:
		return b.chain.CurrentBlock()
	case rpc.FinalizedBlockNumber:
		panic("TODO(weiihann):not implemented")
	case rpc.EarliestBlockNumber:
		panic("TODO(weiihann):not implemented")
	case rpc.SafeBlockNumber:
		panic("TODO(weiihann):not implemented")
	case rpc.PendingBlockNumber:
		panic("TODO(weiihann):not implemented")
	}

	bn := uint64(number)
	return b.chain.GetBlockByNumber(bn)
}

func (b *APIBackend) BlockByHash(ctx context.Context, hash common.Hash) (*types.Block, error) {
	return b.chain.GetBlockByHash(hash)
}

func (b *APIBackend) BlockByNumberOrHash(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*types.Block, error) {
	if blockNr, ok := blockNrOrHash.Number(); ok {
		return b.BlockByNumber(ctx, blockNr)
	}
	if hash, ok := blockNrOrHash.Hash(); ok {
		return b.BlockByHash(ctx, hash)
	}
	return nil, ErrInvalidArguments
}

func (b *APIBackend) StateAndHeaderByNumber(ctx context.Context, number rpc.BlockNumber) (*state.StateDB, *types.Header, error) {
	header, err := b.HeaderByNumber(ctx, number)
	if err != nil {
		return nil, nil, err
	}
	if header == nil {
		return nil, nil, errors.New("header not found")
	}
	stateDb, err := state.New(header.Root, b.db)
	if err != nil {
		return nil, nil, err
	}
	return stateDb, header, nil
}

// TODO(weiihann): check if need to deal with canonical
func (b *APIBackend) stateAndHeaderByHash(ctx context.Context, hash common.Hash) (*state.StateDB, *types.Header, error) {
	header, err := b.HeaderByHash(ctx, hash)
	if err != nil {
		return nil, nil, err
	}
	if header == nil {
		return nil, nil, ErrHeaderNotFound
	}
	stateDb, err := state.New(header.Root, b.db)
	if err != nil {
		return nil, nil, err
	}
	return stateDb, header, nil
}

func (b *APIBackend) StateAndHeaderByNumberOrHash(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*state.StateDB, *types.Header, error) {
	if blockNr, ok := blockNrOrHash.Number(); ok {
		return b.StateAndHeaderByNumber(ctx, blockNr)
	}
	if hash, ok := blockNrOrHash.Hash(); ok {
		return b.stateAndHeaderByHash(ctx, hash)
	}
	return nil, nil, ErrInvalidArguments
}

func (b *APIBackend) Pending() (*types.Block, types.Receipts, *state.StateDB) {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) GetReceipts(ctx context.Context, hash common.Hash) (types.Receipts, error) {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) GetCanonicalReceipt(tx *types.Transaction, blockHash common.Hash, blockNumber, blockIndex uint64) (*types.Receipt, error) {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) GetEVM(ctx context.Context, state *state.StateDB, header *types.Header, vmConfig *vm.Config, blockCtx *vm.BlockContext) *vm.EVM {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) SubscribeChainEvent(ch chan<- core.ChainEvent) event.Subscription {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) SubscribeChainHeadEvent(ch chan<- core.ChainHeadEvent) event.Subscription {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) SubscribeStateUpdateEvent(ch chan<- core.StateUpdateEvent) event.Subscription {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) SendTx(ctx context.Context, signedTx *types.Transaction) error {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) GetCanonicalTransaction(txHash common.Hash) (bool, *types.Transaction, common.Hash, uint64, uint64) {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) TxIndexDone() bool {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) GetPoolTransactions() (types.Transactions, error) {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) GetPoolTransaction(txHash common.Hash) *types.Transaction {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) GetPoolNonce(ctx context.Context, addr common.Address) (uint64, error) {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) Stats() (runnable int, blocked int) {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) TxPoolContent() (map[common.Address][]*types.Transaction, map[common.Address][]*types.Transaction) {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) TxPoolContentFrom(addr common.Address) ([]*types.Transaction, []*types.Transaction) {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) SubscribeNewTxsEvent(ch chan<- core.NewTxsEvent) event.Subscription {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) ChainConfig() *params.ChainConfig {
	// TODO(Weiihann): just return the mainnet chain config for now
	return params.MainnetChainConfig
}

func (b *APIBackend) Engine() consensus.Engine {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) HistoryPruningCutoff() uint64 {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) GetBody(ctx context.Context, hash common.Hash, number rpc.BlockNumber) (*types.Body, error) {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) GetLogs(ctx context.Context, blockHash common.Hash, number uint64) ([][]*types.Log, error) {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) SubscribeRemovedLogsEvent(ch chan<- core.RemovedLogsEvent) event.Subscription {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) SubscribeLogsEvent(ch chan<- []*types.Log) event.Subscription {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) CurrentView() *filtermaps.ChainView {
	panic("TODO(weiihann):not implemented")
}

func (b *APIBackend) NewMatcherBackend() filtermaps.MatcherBackend {
	panic("TODO(weiihann):not implemented")
}

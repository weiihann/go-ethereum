// Copyright 2026 go-ethereum Authors
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

package core

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/beacon"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/triedb"
)

// TestUBTBlockchainE2E runs real EVM transactions through the full block
// pipeline (GenerateChainWithGenesis -> InsertChain) on a UBT-backed chain,
// exercising the parallel trie hashing/commit drivers under real block
// processing, then reopens the chain from disk and confirms the persisted head
// root matches. It logs each block's state root so the run can be diffed against
// a sequential build for byte-identical equality. Uses the same config/genesis
// scaffolding as TestProcessUBT (no Cancun), which is a working UBT setup.
func TestUBTBlockchainE2E(t *testing.T) {
	const blocks, deploysPerBlock = 8, 12
	signer := types.LatestSigner(testUBTChainConfig)
	testKey, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	coinbase := common.HexToAddress("0x71562b71999873DB5b286dF957af199Ec94617F7")
	// Init bytecode that SSTOREs slot 0x64 (=100, >=64 -> storage zone) then
	// returns empty runtime: PUSH1 1, PUSH1 0x64, SSTORE, PUSH1 0, PUSH1 0, RETURN.
	// Each deployment creates a distinct contract address, so its single storage
	// stem lands in a storage-zone subtree keyed by the address hash — spreading
	// dirty stems across the depth-5 storage subtrees so Hash/Commit fan out.
	code := common.FromHex("600160645560006000f3")

	gspec := &Genesis{
		Config: testUBTChainConfig,
		Alloc: GenesisAlloc{
			coinbase:                         {Balance: new(big.Int).Mul(big.NewInt(10), big.NewInt(params.Ether))},
			params.BeaconRootsAddress:        {Nonce: 1, Code: params.BeaconRootsCode, Balance: common.Big0},
			params.HistoryStorageAddress:     {Nonce: 1, Code: params.HistoryStorageCode, Balance: common.Big0},
			params.WithdrawalQueueAddress:    {Nonce: 1, Code: params.WithdrawalQueueCode, Balance: common.Big0},
			params.ConsolidationQueueAddress: {Nonce: 1, Code: params.ConsolidationQueueCode, Balance: common.Big0},
		},
	}

	var nonce uint64
	_, chain, _ := GenerateChainWithGenesis(gspec, beacon.New(ethash.NewFaker()), blocks, func(i int, gen *BlockGen) {
		gen.SetPoS()
		// A couple of plain transfers (account zone).
		for j := range 2 {
			to := common.Address{byte(i), byte(j), 0xcd}
			tx, _ := types.SignTx(types.NewTransaction(nonce, to, big.NewInt(1000+int64(j)), params.TxGas, big.NewInt(875000000), nil), signer, testKey)
			gen.AddTx(tx)
			nonce++
		}
		// Many storage-writing contract deployments (storage zone), spreading
		// stems across depth-5 subtrees so the parallel path actually fans out.
		for range deploysPerBlock {
			tx, _ := types.SignNewTx(testKey, signer, &types.LegacyTx{
				Nonce:    nonce,
				Value:    big.NewInt(0),
				Gas:      1000000,
				GasPrice: big.NewInt(875000000),
				Data:     code,
			})
			gen.AddTx(tx)
			nonce++
		}
	})

	bcdb := rawdb.NewMemoryDatabase()
	options := DefaultConfig().WithStateScheme(rawdb.PathScheme)
	options.SnapshotLimit = 0
	options.BinTrieGroupDepth = triedb.DefaultBinTrieGroupDepth
	bc, err := NewBlockChain(bcdb, gspec, beacon.New(ethash.NewFaker()), options)
	if err != nil {
		t.Fatalf("new blockchain: %v", err)
	}

	if n, err := bc.InsertChain(chain); err != nil {
		t.Fatalf("InsertChain failed at block %d: %v", n, err)
	}
	for i, b := range chain {
		t.Logf("UBT-CHAIN block %d stateRoot %x", i+1, b.Root())
	}
	head := bc.CurrentBlock()
	if head.Number.Uint64() != uint64(blocks) {
		t.Fatalf("head number = %d, want %d", head.Number.Uint64(), blocks)
	}
	wantRoot := chain[len(chain)-1].Root()
	if head.Root != wantRoot {
		t.Fatalf("head root = %x, want %x", head.Root, wantRoot)
	}
	t.Logf("UBT-CHAIN HEAD stateRoot %x", head.Root)
	bc.Stop()

	// Reopen from disk: the persisted parallel-committed state must reload to the
	// same head root.
	reopened, err := NewBlockChain(bcdb, gspec, beacon.New(ethash.NewFaker()), options)
	if err != nil {
		t.Fatalf("reopen blockchain: %v", err)
	}
	defer reopened.Stop()
	rh := reopened.CurrentBlock()
	if rh.Root != wantRoot {
		t.Fatalf("reopened head root = %x, want %x", rh.Root, wantRoot)
	}
	if _, err := reopened.StateAt(rh); err != nil {
		t.Fatalf("state unavailable at persisted head root %x: %v", wantRoot, err)
	}
}

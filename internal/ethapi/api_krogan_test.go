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
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/beacon"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestAPI creates a test backend with deployed contracts for Krogan testing
func newTestAPI(t *testing.T) (*testBackend, *KroganAPI, map[common.Hash]types.Account) {
	// Setup genesis with funded accounts
	accounts := newAccounts(3)
	alloc := map[common.Address]types.Account{
		accounts[0].addr: {Balance: big.NewInt(params.Ether)},
		accounts[1].addr: {
			Balance: big.NewInt(params.Ether),
			Storage: map[common.Hash]common.Hash{
				{}: common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001"),
			},
		},
		accounts[2].addr: {
			Balance: big.NewInt(params.Ether),
			Storage: map[common.Hash]common.Hash{
				{}: common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000002"),
			},
			Code: common.Hex2Bytes("608060405234801561000f575f80fd5b50602a5f81905550603e8061"),
		},
	}
	genesis := &core.Genesis{
		Config: params.MergedTestChainConfig,
		Alloc:  alloc,
	}

	backend := newTestBackend(t, 5, genesis, beacon.New(ethash.NewFaker()), func(i int, gen *core.BlockGen) {})

	api := NewKroganAPI(backend)

	accs := make(map[common.Hash]types.Account)
	for addr, acc := range alloc {
		hashSlots := make(map[common.Hash]common.Hash)
		for slot, value := range acc.Storage {
			hashSlots[crypto.Keccak256Hash(slot.Bytes())] = value
		}
		accs[crypto.Keccak256Hash(addr.Bytes())] = types.Account{
			Balance: acc.Balance,
			Storage: hashSlots,
			Code:    acc.Code,
		}
	}

	return backend, api, accs
}

func TestAccountRange(t *testing.T) {
	_, api, accounts := newTestAPI(t)

	t.Run("fetch from zero hash", func(t *testing.T) {
		result, err := api.AccountRange(t.Context(), common.Hash{})
		require.NoError(t, err)
		require.NotNil(t, result)

		assert.Equal(t, len(result.Accounts), 4)

		for _, acc := range result.Accounts {
			assert.Contains(t, accounts, acc.Hash)
		}
	})

	t.Run("no remaining accounts", func(t *testing.T) {
		result, err := api.AccountRange(t.Context(), common.MaxHash)
		require.NoError(t, err)
		require.NotNil(t, result)

		assert.Equal(t, len(result.Accounts), 0)
	})
}

func TestStorageRange(t *testing.T) {
	t.Parallel()

	_, api, accounts := newTestAPI(t)

	t.Run("fetch all storage", func(t *testing.T) {
		for addrHash, acc := range accounts {
			result, err := api.StorageRange(t.Context(), addrHash, common.Hash{})
			require.NoError(t, err)
			require.NotNil(t, result)

			assert.Equal(t, len(acc.Storage), len(result.Slots))

			for _, slot := range result.Slots {
				assert.Contains(t, acc.Storage, slot.Hash)
			}
		}
	})

	t.Run("fetch from non-existent account", func(t *testing.T) {
		nonExistentHash := common.HexToHash("0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd")
		result, err := api.StorageRange(t.Context(), nonExistentHash, common.Hash{})
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, len(result.Slots), 0)
	})
}

func TestBytecodes(t *testing.T) {
	t.Parallel()

	_, api, accounts := newTestAPI(t)

	t.Run("fetch all bytecodes", func(t *testing.T) {
		var codeHashes []common.Hash
		for _, acc := range accounts {
			if acc.Code == nil {
				continue
			}
			codeHashes = append(codeHashes, crypto.Keccak256Hash(acc.Code))
		}

		result, err := api.Bytecodes(t.Context(), codeHashes)
		require.NoError(t, err)
		require.NotNil(t, result)

		assert.Equal(t, len(codeHashes), len(result.Codes))
	})
}

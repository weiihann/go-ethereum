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

package state

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/holiman/uint256"
)

// ubtE2EAddr derives a deterministic address for account index i.
func ubtE2EAddr(i int) common.Address {
	var a common.Address
	a[0] = byte(i)
	a[1] = byte(i >> 8)
	a[2] = 0x5a
	return a
}

// applyUBTBlock applies a deterministic mutation set for block b across the
// account, code, and storage zones: balances/nonces on every account, code on a
// quarter of them, and both header (<64) and main (>=64) storage slots on a
// third — spreading dirty stems across many storage subtrees so the parallel
// hash/commit fan-out is genuinely exercised.
func applyUBTBlock(s *StateDB, b, accounts int) {
	base := []byte{0x60, byte(b), 0x60, 0x00, 0x55} // PUSH b, PUSH 0, SSTORE
	for i := range accounts {
		a := ubtE2EAddr(i)
		s.SetBalance(a, uint256.NewInt(uint64((b+1)*1000+i)), tracing.BalanceChangeUnspecified)
		s.SetNonce(a, uint64(b*7+i), tracing.NonceChangeUnspecified)
		if i%4 == 0 {
			s.SetCode(a, append(append([]byte{}, base...), byte(i)), tracing.CodeChangeUnspecified)
		}
		if i%3 == 0 {
			var hk common.Hash
			hk[31] = byte(i % 60) // header storage slot (<64)
			s.SetState(a, hk, common.BigToHash(big.NewInt(int64(b*100+i+1))))
			var mk common.Hash
			mk[30] = byte(i)
			mk[31] = byte(100 + i%50) // main storage slot (>=64)
			s.SetState(a, mk, common.BigToHash(big.NewInt(int64(b*1000+i+1))))
		}
	}
}

// TestUBTParallelStateE2E drives a multi-block UBT workload through the real
// StateDB commit pipeline (parallel trie hashing/commit active by default),
// reopening from the committed root between blocks, then verifies the final
// persisted state reads back correctly. It logs every block's state root so the
// run can be compared against a sequential build for byte-identical equality.
func TestUBTParallelStateE2E(t *testing.T) {
	const blocks, accounts = 8, 200
	_, sdb := newBinaryStateDBWithDB(t)

	root := types.EmptyBinaryHash
	for b := range blocks {
		st, err := New(root, sdb)
		if err != nil {
			t.Fatalf("open block %d: %v", b, err)
		}
		applyUBTBlock(st, b, accounts)
		root, err = st.Commit(uint64(b), true, true)
		if err != nil {
			t.Fatalf("commit block %d: %v", b, err)
		}
		t.Logf("UBT-STATE block %d root %x", b, root)
	}
	t.Logf("UBT-STATE HEAD root %x", root)

	// Reopen the final root and confirm a sample of state reads back — i.e. the
	// parallel-committed on-disk blobs reconstruct the same values.
	final, err := New(root, sdb)
	if err != nil {
		t.Fatalf("reopen head root %x: %v", root, err)
	}
	for _, i := range []int{0, 1, 3, 99, 150, 199} {
		a := ubtE2EAddr(i)
		wantBal := uint256.NewInt(uint64(blocks*1000 + i)) // last block b=blocks-1 -> (b+1)=blocks
		if got := final.GetBalance(a); got.Cmp(wantBal) != 0 {
			t.Fatalf("account %d balance after reopen: got %v want %v", i, got, wantBal)
		}
		if got, want := final.GetNonce(a), uint64((blocks-1)*7+i); got != want {
			t.Fatalf("account %d nonce after reopen: got %d want %d", i, got, want)
		}
		if i%3 == 0 {
			var mk common.Hash
			mk[30] = byte(i)
			mk[31] = byte(100 + i%50)
			want := common.BigToHash(big.NewInt(int64((blocks-1)*1000 + i + 1)))
			if got := final.GetState(a, mk); got != want {
				t.Fatalf("account %d main slot after reopen: got %x want %x", i, got, want)
			}
		}
	}
}

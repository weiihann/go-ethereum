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
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/holiman/uint256"
)

// applyParallelWorkload mutates a UBT state with a deterministic multi-account,
// multi-slot workload that spreads keys across storage subtrees.
func applyParallelWorkload(s *StateDB) {
	for i := range 1500 {
		var addr common.Address
		addr[19] = byte(i)
		addr[18] = byte(i >> 8)
		s.SetBalance(addr, uint256.NewInt(uint64(i+1)), tracing.BalanceChangeUnspecified)
		s.SetNonce(addr, uint64(i), tracing.NonceChangeUnspecified)
		for j := range 5 {
			var key, val common.Hash
			key[31] = byte(j + 100) // slot >= 64 -> storage zone
			key[30] = byte(i)
			val[31] = byte(i + j + 1)
			s.SetState(addr, key, val)
		}
	}
}

// TestUBTParallelCommitDeterministic asserts the parallel commit drivers produce
// the same root across runs (scheduling nondeterminism must not perturb output).
func TestUBTParallelCommitDeterministic(t *testing.T) {
	roots := make([]common.Hash, 2)
	for run := range 2 {
		state, _ := newBinaryStateDBWithDB(t)
		applyParallelWorkload(state)
		root, err := state.Commit(uint64(run), true, true)
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
		roots[run] = root
	}
	if roots[0] != roots[1] {
		t.Fatalf("nondeterministic parallel root: %x != %x", roots[0], roots[1])
	}
}

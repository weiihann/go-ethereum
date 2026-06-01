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

package bintrie

import (
	"encoding/binary"
	"math/rand/v2"

	"github.com/ethereum/go-ethereum/common"
)

// syntheticAddress derives a deterministic 20-byte address from an index. Used
// so the same `accIdx` produces the same address across runs and configs.
func syntheticAddress(idx uint64) common.Address {
	var a common.Address
	binary.BigEndian.PutUint64(a[12:], idx)
	return a
}

// buildSyntheticPBTTrie populates a fresh BinaryTrie with `numAccounts` accounts
// and `slotsPerAccount` storage slots per account. Slot keys are spread across
// header (slot < HeaderStorageSlots) and main (>= HeaderStorageSlots) storage
// so both zones are populated.
//
// The trie is returned in its post-Hash state so subsequent mutations exercise
// the realistic hashing+commit path. The arena is in-memory only — no reader
// is attached and no disk activity occurs.
func buildSyntheticPBTTrie(numAccounts, slotsPerAccount int) *BinaryTrie {
	store := newNodeStore()
	store.groupDepth = 5
	t := &BinaryTrie{
		store:      store,
		groupDepth: 5,
	}
	for a := uint64(0); a < uint64(numAccounts); a++ {
		addr := syntheticAddress(a)
		// One zone-000 anchor leaf per account (basic data).
		basic := GetBinaryTreeKeyBasicData(addr)
		var anchor [HashSize]byte
		binary.BigEndian.PutUint64(anchor[24:], a+1)
		_ = t.store.Insert(basic, anchor[:], nil)
		// `slotsPerAccount` storage slots, spread so we get both zones.
		for s := 0; s < slotsPerAccount; s++ {
			var slot [HashSize]byte
			// Spread: every 8th index puts us above HeaderStorageSlots so a
			// mix of header / main storage is exercised.
			binary.BigEndian.PutUint64(slot[24:], uint64(s)*8)
			key := GetBinaryTreeKeyStorageSlot(addr, slot[:])
			var val [HashSize]byte
			binary.BigEndian.PutUint64(val[24:], a*1_000_000+uint64(s))
			_ = t.store.Insert(key, val[:], nil)
		}
	}
	// The store-level Insert above bypasses BinaryTrie's applyStemValues,
	// so t.root is still its zero value. Sync it with store.root so cow-mode
	// callers (effectiveRoot under cowOnWrite=true) see the populated tree.
	t.root = t.store.root
	// Hash so the in-memory trie has computed hashes everywhere; mimics
	// the post-commit steady state.
	_ = t.Hash()
	return t
}

// dirtyOp represents one storage update issued during a synthetic block.
type dirtyOp struct {
	Addr  common.Address
	Slot  [HashSize]byte
	Value []byte
}

// drawDirtyOps returns `kAccounts` × `mSlots` dirty operations targeting a
// deterministic subset of accounts in a trie built by buildSyntheticPBTTrie.
//
// The values written are non-zero and per-(account,slot) unique so callers
// can verify the post-state via re-reads if desired.
func drawDirtyOps(seed uint64, kAccounts, mSlots, totalAccounts int) []dirtyOp {
	if kAccounts == 0 || mSlots == 0 {
		return nil
	}
	rng := rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15))
	perm := rng.Perm(totalAccounts)[:kAccounts]
	ops := make([]dirtyOp, 0, kAccounts*mSlots)
	for _, accIdx := range perm {
		addr := syntheticAddress(uint64(accIdx))
		for j := 0; j < mSlots; j++ {
			var slot [HashSize]byte
			binary.BigEndian.PutUint64(slot[24:], uint64(j)*8)
			val := make([]byte, HashSize)
			binary.BigEndian.PutUint64(val[24:], uint64(accIdx)*1_000_001+uint64(j))
			ops = append(ops, dirtyOp{Addr: addr, Slot: slot, Value: val})
		}
	}
	return ops
}

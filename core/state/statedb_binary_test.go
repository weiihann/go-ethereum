// Copyright 2025 go-ethereum Authors
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
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"
)

// newBinaryStateDB creates a fresh StateDB backed by a binary trie.
func newBinaryStateDB(t *testing.T) *StateDB {
	t.Helper()
	disk := rawdb.NewMemoryDatabase()
	db := triedb.NewDatabase(disk, triedb.UBTDefaults)
	sdb := NewDatabase(db, nil)
	state, err := New(types.EmptyBinaryHash, sdb)
	if err != nil {
		t.Fatalf("failed to create binary statedb: %v", err)
	}
	return state
}

// newBinaryStateDBWithDB creates a fresh binary StateDB and returns the
// underlying CachingDB so callers can reopen state from a committed root.
func newBinaryStateDBWithDB(t *testing.T) (*StateDB, Database) {
	t.Helper()
	disk := rawdb.NewMemoryDatabase()
	db := triedb.NewDatabase(disk, triedb.UBTDefaults)
	sdb := NewDatabase(db, nil)
	state, err := New(types.EmptyBinaryHash, sdb)
	if err != nil {
		t.Fatalf("failed to create binary statedb: %v", err)
	}
	return state, sdb
}

// TestBinaryTrieCommitAndReopen tests the full persistence cycle:
// create state → commit → reopen from root → verify → mutate → commit again.
func TestBinaryTrieCommitAndReopen(t *testing.T) {
	state, sdb := newBinaryStateDBWithDB(t)

	addr1 := common.HexToAddress("0x1111111111111111111111111111111111111111")
	addr2 := common.HexToAddress("0x2222222222222222222222222222222222222222")
	code := []byte{0x60, 0x0a, 0x60, 0x00, 0x55} // PUSH 10, PUSH 0, SSTORE

	// Block 1: create accounts.
	state.SetBalance(addr1, uint256.NewInt(1e18), tracing.BalanceChangeUnspecified)
	state.SetNonce(addr1, 1, tracing.NonceChangeUnspecified)
	state.SetBalance(addr2, uint256.NewInt(2e18), tracing.BalanceChangeUnspecified)
	state.SetCode(addr2, code, tracing.CodeChangeUnspecified)
	state.SetState(addr2, common.Hash{31: 5}, common.HexToHash("0xaa"))   // header storage
	state.SetState(addr2, common.Hash{31: 100}, common.HexToHash("0xbb")) // main storage

	root1, err := state.Commit(0, true, true)
	if err != nil {
		t.Fatalf("commit block 0 failed: %v", err)
	}

	// Reopen from committed root.
	state2, err := New(root1, sdb)
	if err != nil {
		t.Fatalf("reopen after block 0 failed: %v", err)
	}

	// Verify all values survived the commit+reopen cycle.
	if bal := state2.GetBalance(addr1); bal.Cmp(uint256.NewInt(1e18)) != 0 {
		t.Errorf("addr1 balance after reopen: got %v, want 1e18", bal)
	}
	if nonce := state2.GetNonce(addr1); nonce != 1 {
		t.Errorf("addr1 nonce after reopen: got %d, want 1", nonce)
	}
	if bal := state2.GetBalance(addr2); bal.Cmp(uint256.NewInt(2e18)) != 0 {
		t.Errorf("addr2 balance after reopen: got %v, want 2e18", bal)
	}
	if ch := state2.GetCodeHash(addr2); ch != crypto.Keccak256Hash(code) {
		t.Errorf("addr2 code hash after reopen: got %x, want %x", ch, crypto.Keccak256Hash(code))
	}
	if v := state2.GetState(addr2, common.Hash{31: 5}); v != common.HexToHash("0xaa") {
		t.Errorf("slot 5 after reopen: got %x, want 0xaa", v)
	}
	if v := state2.GetState(addr2, common.Hash{31: 100}); v != common.HexToHash("0xbb") {
		t.Errorf("slot 100 after reopen: got %x, want 0xbb", v)
	}

	// Block 1: mutate reopened state.
	state2.SetNonce(addr1, 2, tracing.NonceChangeUnspecified)
	state2.SetState(addr2, common.Hash{31: 200}, common.HexToHash("0xcc"))

	root2, err := state2.Commit(1, true, true)
	if err != nil {
		t.Fatalf("commit block 1 failed: %v", err)
	}
	if root2 == root1 {
		t.Fatal("root2 should differ from root1 after mutations")
	}

	// Reopen and verify block 1 state.
	state3, err := New(root2, sdb)
	if err != nil {
		t.Fatalf("reopen after block 1 failed: %v", err)
	}
	if nonce := state3.GetNonce(addr1); nonce != 2 {
		t.Errorf("addr1 nonce after block 1: got %d, want 2", nonce)
	}
	if v := state3.GetState(addr2, common.Hash{31: 200}); v != common.HexToHash("0xcc") {
		t.Errorf("slot 200 after block 1: got %x, want 0xcc", v)
	}
}

// TestBinaryTrieStorageDeletion verifies that setting storage to zero
// correctly deletes it from the binary trie.
func TestBinaryTrieStorageDeletion(t *testing.T) {
	state, sdb := newBinaryStateDBWithDB(t)

	addr := common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	// Create account with storage in both zones.
	state.SetBalance(addr, uint256.NewInt(1e18), tracing.BalanceChangeUnspecified)
	state.SetState(addr, common.Hash{31: 10}, common.HexToHash("0xaa"))  // zone 000
	state.SetState(addr, common.Hash{31: 100}, common.HexToHash("0xbb")) // zone 1
	state.SetState(addr, common.Hash{31: 200}, common.HexToHash("0xcc")) // zone 1

	root1, err := state.Commit(0, true, true)
	if err != nil {
		t.Fatalf("commit block 0 failed: %v", err)
	}

	// Reopen, delete some slots, keep others.
	state2, err := New(root1, sdb)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	state2.SetState(addr, common.Hash{31: 10}, common.Hash{})  // delete zone 000 slot
	state2.SetState(addr, common.Hash{31: 100}, common.Hash{}) // delete zone 1 slot

	root2, err := state2.Commit(1, true, true)
	if err != nil {
		t.Fatalf("commit block 1 failed: %v", err)
	}

	// Verify deletions.
	state3, err := New(root2, sdb)
	if err != nil {
		t.Fatalf("reopen after deletion failed: %v", err)
	}
	if v := state3.GetState(addr, common.Hash{31: 10}); v != (common.Hash{}) {
		t.Errorf("slot 10 should be deleted, got %x", v)
	}
	if v := state3.GetState(addr, common.Hash{31: 100}); v != (common.Hash{}) {
		t.Errorf("slot 100 should be deleted, got %x", v)
	}
	// Slot 200 should survive.
	if v := state3.GetState(addr, common.Hash{31: 200}); v != common.HexToHash("0xcc") {
		t.Errorf("slot 200 should survive, got %x, want 0xcc", v)
	}
}

// TestBinaryTrieSelfDestruct verifies account self-destruction works
// correctly in the binary trie.
func TestBinaryTrieSelfDestruct(t *testing.T) {
	state := newBinaryStateDB(t)

	addr1 := common.HexToAddress("0x1111111111111111111111111111111111111111")
	addr2 := common.HexToAddress("0x2222222222222222222222222222222222222222")

	// Create two accounts.
	state.SetBalance(addr1, uint256.NewInt(1e18), tracing.BalanceChangeUnspecified)
	state.SetCode(addr1, []byte{0x60, 0x00}, tracing.CodeChangeUnspecified)
	state.SetState(addr1, common.Hash{31: 1}, common.HexToHash("0xaa"))

	state.SetBalance(addr2, uint256.NewInt(2e18), tracing.BalanceChangeUnspecified)

	state.IntermediateRoot(false)

	// Self-destruct addr1.
	state.SelfDestruct(addr1)
	state.Finalise(true)
	state.IntermediateRoot(false)

	// addr1 should be gone.
	if state.GetBalance(addr1).Sign() != 0 {
		t.Error("addr1 balance should be zero after self-destruct")
	}
	if state.GetNonce(addr1) != 0 {
		t.Error("addr1 nonce should be zero after self-destruct")
	}

	// addr2 should be unaffected.
	if bal := state.GetBalance(addr2); bal.Cmp(uint256.NewInt(2e18)) != 0 {
		t.Errorf("addr2 balance should be unchanged, got %v", bal)
	}
}

// TestBinaryTrieSnapshotRevert verifies that snapshot+revert restores
// state correctly in the binary trie.
func TestBinaryTrieSnapshotRevert(t *testing.T) {
	state := newBinaryStateDB(t)

	addr := common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	// Set initial state.
	state.SetBalance(addr, uint256.NewInt(1e18), tracing.BalanceChangeUnspecified)
	state.SetState(addr, common.Hash{31: 1}, common.HexToHash("0xaa"))

	// Take snapshot.
	snap := state.Snapshot()

	// Modify state after snapshot.
	state.SetBalance(addr, uint256.NewInt(999), tracing.BalanceChangeUnspecified)
	state.SetState(addr, common.Hash{31: 1}, common.HexToHash("0xff"))
	state.SetState(addr, common.Hash{31: 200}, common.HexToHash("0xbb")) // new slot

	// Verify modifications are visible.
	if bal := state.GetBalance(addr); bal.Cmp(uint256.NewInt(999)) != 0 {
		t.Fatalf("balance before revert: got %v, want 999", bal)
	}

	// Revert to snapshot.
	state.RevertToSnapshot(snap)

	// Verify state is restored.
	if bal := state.GetBalance(addr); bal.Cmp(uint256.NewInt(1e18)) != 0 {
		t.Errorf("balance after revert: got %v, want 1e18", bal)
	}
	if v := state.GetState(addr, common.Hash{31: 1}); v != common.HexToHash("0xaa") {
		t.Errorf("slot 1 after revert: got %x, want 0xaa", v)
	}
	if v := state.GetState(addr, common.Hash{31: 200}); v != (common.Hash{}) {
		t.Errorf("slot 200 should not exist after revert, got %x", v)
	}
}

// TestBinaryTrieMultiBlockTransitions simulates 3 blocks of state changes
// to exercise the parallel zone path (blocks 2+ have InternalNode root).
func TestBinaryTrieMultiBlockTransitions(t *testing.T) {
	state, sdb := newBinaryStateDBWithDB(t)

	addr1 := common.HexToAddress("0x1111111111111111111111111111111111111111")
	addr2 := common.HexToAddress("0x2222222222222222222222222222222222222222")
	addr3 := common.HexToAddress("0x3333333333333333333333333333333333333333")

	// Block 1: create accounts with storage spanning both zones.
	state.SetBalance(addr1, uint256.NewInt(1e18), tracing.BalanceChangeUnspecified)
	state.SetState(addr1, common.Hash{31: 10}, common.HexToHash("0x01"))  // zone 000
	state.SetState(addr1, common.Hash{31: 100}, common.HexToHash("0x02")) // zone 1

	state.SetBalance(addr2, uint256.NewInt(2e18), tracing.BalanceChangeUnspecified)
	state.SetState(addr2, common.Hash{31: 50}, common.HexToHash("0x03"))

	root1, err := state.Commit(0, true, true)
	if err != nil {
		t.Fatalf("block 0 commit failed: %v", err)
	}

	// Block 1: modify existing accounts (exercises parallel path).
	state, err = New(root1, sdb)
	if err != nil {
		t.Fatalf("reopen for block 1 failed: %v", err)
	}
	state.SetBalance(addr1, uint256.NewInt(5e17), tracing.BalanceChangeUnspecified) // spend
	state.SetState(addr1, common.Hash{31: 10}, common.HexToHash("0x0a"))            // update zone 000
	state.SetState(addr1, common.Hash{31: 100}, common.HexToHash("0x0b"))           // update zone 1
	state.SetBalance(addr2, uint256.NewInt(3e18), tracing.BalanceChangeUnspecified) // receive

	root2, err := state.Commit(1, true, true)
	if err != nil {
		t.Fatalf("block 1 commit failed: %v", err)
	}

	// Block 2: delete some storage, add a new account.
	state, err = New(root2, sdb)
	if err != nil {
		t.Fatalf("reopen for block 2 failed: %v", err)
	}
	state.SetState(addr1, common.Hash{31: 10}, common.Hash{})                       // delete zone 000
	state.SetBalance(addr3, uint256.NewInt(1e18), tracing.BalanceChangeUnspecified) // new account
	state.SetState(addr3, common.Hash{31: 70}, common.HexToHash("0xdd"))            // zone 1

	root3, err := state.Commit(2, true, true)
	if err != nil {
		t.Fatalf("block 2 commit failed: %v", err)
	}

	// Verify final state after 3 blocks.
	final, err := New(root3, sdb)
	if err != nil {
		t.Fatalf("reopen final state failed: %v", err)
	}

	// addr1: balance updated, slot 10 deleted, slot 100 updated.
	if bal := final.GetBalance(addr1); bal.Cmp(uint256.NewInt(5e17)) != 0 {
		t.Errorf("addr1 final balance: got %v, want 5e17", bal)
	}
	if v := final.GetState(addr1, common.Hash{31: 10}); v != (common.Hash{}) {
		t.Errorf("addr1 slot 10 should be deleted, got %x", v)
	}
	if v := final.GetState(addr1, common.Hash{31: 100}); v != common.HexToHash("0x0b") {
		t.Errorf("addr1 slot 100: got %x, want 0x0b", v)
	}

	// addr2: balance updated.
	if bal := final.GetBalance(addr2); bal.Cmp(uint256.NewInt(3e18)) != 0 {
		t.Errorf("addr2 final balance: got %v, want 3e18", bal)
	}

	// addr3: new account with storage.
	if bal := final.GetBalance(addr3); bal.Cmp(uint256.NewInt(1e18)) != 0 {
		t.Errorf("addr3 final balance: got %v, want 1e18", bal)
	}
	if v := final.GetState(addr3, common.Hash{31: 70}); v != common.HexToHash("0xdd") {
		t.Errorf("addr3 slot 70: got %x, want 0xdd", v)
	}

	// All three roots should be different.
	if root1 == root2 || root2 == root3 || root1 == root3 {
		t.Errorf("roots should all differ: %x, %x, %x", root1, root2, root3)
	}
}

// TestBinaryTrieLargeContract creates a contract with storage spanning many
// stems across both zones to verify stem boundary handling.
func TestBinaryTrieLargeContract(t *testing.T) {
	state := newBinaryStateDB(t)

	addr := common.HexToAddress("0xcccccccccccccccccccccccccccccccccccccccc")
	state.SetBalance(addr, uint256.NewInt(1e18), tracing.BalanceChangeUnspecified)

	state.IntermediateRoot(false)

	// Set 600 storage slots: 0-63 in zone 000, 64-599 in zone 1 (multiple stems).
	// stem boundaries: tree_index 0 (slots 0-255), 1 (256-511), 2 (512-599)
	expected := make(map[uint64]common.Hash)
	for i := uint64(0); i < 600; i++ {
		var slotKey common.Hash
		slotKey[31] = byte(i % 256)
		slotKey[30] = byte(i / 256)

		var val common.Hash
		val[31] = byte((i + 1) % 256)
		val[30] = byte((i + 1) / 256)

		state.SetState(addr, slotKey, val)
		expected[i] = val
	}

	// Also test a very large slot number.
	largeSlot := common.Hash{24: 0x27, 25: 0x10} // slot 10000 (big-endian)
	largeVal := common.HexToHash("0xdeadbeef")
	state.SetState(addr, largeSlot, largeVal)

	state.IntermediateRoot(false)

	// Verify all 600 slots.
	for i := uint64(0); i < 600; i++ {
		var slotKey common.Hash
		slotKey[31] = byte(i % 256)
		slotKey[30] = byte(i / 256)

		got := state.GetState(addr, slotKey)
		if got != expected[i] {
			t.Errorf("slot %d: got %x, want %x", i, got, expected[i])
		}
	}

	// Verify large slot.
	if v := state.GetState(addr, largeSlot); v != largeVal {
		t.Errorf("slot 10000: got %x, want %x", v, largeVal)
	}
}

// Copyright 2026 The go-ethereum Authors
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

package eip8188

import (
	"bytes"
	"context"
	"math/big"
	"os"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/holiman/uint256"
)

// fakeSource is a Source backed by pre-arranged in-memory slices. Used to
// drive Inject deterministically from tests.
type fakeSource struct {
	accounts []AccountDiff
	storage  []StorageDiff
}

func (f *fakeSource) AccountDiffs(ctx context.Context, startBlock, endBlock uint64) (<-chan AccountDiff, error) {
	out := make(chan AccountDiff)
	go func() {
		defer close(out)
		for _, d := range f.accounts {
			if d.Block < startBlock || (endBlock > 0 && d.Block > endBlock) {
				continue
			}
			select {
			case out <- d:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

func (f *fakeSource) StorageDiffs(ctx context.Context, startBlock, endBlock uint64) (<-chan StorageDiff, error) {
	out := make(chan StorageDiff)
	go func() {
		defer close(out)
		for _, d := range f.storage {
			if d.Block < startBlock || (endBlock > 0 && d.Block > endBlock) {
				continue
			}
			select {
			case out <- d:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

func (f *fakeSource) Close() error { return nil }

// seedSnapshot writes a minimal snapshot layout to db: head header at
// headBlock, account records for alice/bob, storage record for (alice, slot1).
// Returns the addresses and slot used so the test can assert against them.
func seedSnapshot(t *testing.T, db ethdb.KeyValueStore, headBlock uint64) (alice, bob common.Address, slot1 common.Hash) {
	t.Helper()

	alice = common.HexToAddress("0xaaaa000000000000000000000000000000000001")
	bob = common.HexToAddress("0xbbbb000000000000000000000000000000000002")
	slot1 = common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001")

	aliceAcc := types.StateAccount{
		Nonce:    1,
		Balance:  uint256.NewInt(100),
		Root:     types.EmptyRootHash,
		CodeHash: types.EmptyCodeHash[:],
	}
	bobAcc := types.StateAccount{
		Nonce:    0,
		Balance:  uint256.NewInt(1),
		Root:     types.EmptyRootHash,
		CodeHash: types.EmptyCodeHash[:],
	}
	rawdb.WriteAccountSnapshot(db, crypto.Keccak256Hash(alice[:]), types.SlimAccountRLP(aliceAcc))
	rawdb.WriteAccountSnapshot(db, crypto.Keccak256Hash(bob[:]), types.SlimAccountRLP(bobAcc))

	slotValue := common.HexToHash("0x00000000000000000000000000000000000000000000000000000000deadbeef")
	slotBlob := types.EncodeStorageSnapshotValue(common.TrimLeftZeroes(slotValue[:]), 0)
	rawdb.WriteStorageSnapshot(db,
		crypto.Keccak256Hash(alice[:]), crypto.Keccak256Hash(slot1[:]),
		slotBlob)

	// Seed a head header so Inject's block-range validation has something real.
	header := &types.Header{Number: big.NewInt(int64(headBlock))}
	rawdb.WriteHeader(db, header)
	rawdb.WriteHeadHeaderHash(db, header.Hash())

	return alice, bob, slot1
}

// readAccountPeriod returns the LastWrittenPeriod field of the snapshot record
// for addr, or an error if the record is missing/corrupt.
func readAccountPeriod(t *testing.T, db ethdb.KeyValueStore, addr common.Address) uint32 {
	t.Helper()
	blob := rawdb.ReadAccountSnapshot(db, crypto.Keccak256Hash(addr[:]))
	if len(blob) == 0 {
		t.Fatalf("no account snapshot for %s", addr.Hex())
	}
	var acc types.SlimAccount
	if err := rlp.DecodeBytes(blob, &acc); err != nil {
		t.Fatalf("decode account snapshot: %v", err)
	}
	return acc.LastWrittenPeriod
}

// readStoragePeriod returns the period (and decoded value) of a storage slot.
func readStoragePeriod(t *testing.T, db ethdb.KeyValueStore, addr common.Address, slot common.Hash) (uint32, []byte) {
	t.Helper()
	blob := rawdb.ReadStorageSnapshot(db,
		crypto.Keccak256Hash(addr[:]), crypto.Keccak256Hash(slot[:]))
	if len(blob) == 0 {
		t.Fatalf("no storage snapshot for %s/%s", addr.Hex(), slot.Hex())
	}
	value, period, err := types.DecodeStorageSnapshotValue(blob)
	if err != nil {
		t.Fatalf("decode storage snapshot: %v", err)
	}
	return period, value
}

// TestInjectEndToEnd is the headline integration test: seed a snapshot, run
// the injector, verify accounts/slots touched by the fake Source carry the
// correct period, and untouched records remain in legacy shape with period=0.
func TestInjectEndToEnd(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()

	alice, bob, slot1 := seedSnapshot(t, db, 50)

	// Diffs: Alice wrote at block 10 → period 1. Bob at 42 → period 4.
	// Alice's slot1 wrote at block 30 → period 3.
	src := &fakeSource{
		accounts: []AccountDiff{
			{Address: alice, Block: 10},
			{Address: bob, Block: 42},
		},
		storage: []StorageDiff{
			{Address: alice, Slot: slot1, Block: 30},
		},
	}

	stats, err := Inject(context.Background(), db, Config{
		Source:          src,
		ForkBlock:       0,
		BlocksPerPeriod: 10,
		EndBlock:        50,
	})
	if err != nil {
		t.Fatalf("Inject: %v", err)
	}
	if stats.AccountSnapshotsUpdated != 2 {
		t.Errorf("accounts updated: got %d, want 2", stats.AccountSnapshotsUpdated)
	}
	if stats.StorageSnapshotsUpdated != 1 {
		t.Errorf("storage updated: got %d, want 1", stats.StorageSnapshotsUpdated)
	}
	if got := readAccountPeriod(t, db, alice); got != 1 {
		t.Errorf("alice period: got %d, want 1", got)
	}
	if got := readAccountPeriod(t, db, bob); got != 4 {
		t.Errorf("bob period: got %d, want 4", got)
	}
	period, value := readStoragePeriod(t, db, alice, slot1)
	if period != 3 {
		t.Errorf("slot period: got %d, want 3", period)
	}
	wantValue := []byte{0xde, 0xad, 0xbe, 0xef}
	if !bytes.Equal(value, wantValue) {
		t.Errorf("slot value after inject: got %x, want %x", value, wantValue)
	}
}

// TestInjectLegacyUntouched asserts that a record not mentioned in the diff
// stream remains in legacy 4-tuple / byte-string form and decodes as
// period=0. This is the guarantee for partial injection.
func TestInjectLegacyUntouched(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()

	_, bob, _ := seedSnapshot(t, db, 20)
	// Snapshot the exact bytes of bob's record before injection.
	bobKey := crypto.Keccak256Hash(bob[:])
	before := append([]byte(nil), rawdb.ReadAccountSnapshot(db, bobKey)...)

	src := &fakeSource{accounts: []AccountDiff{{
		Address: common.HexToAddress("0xccccccccccccccccccccccccccccccccccccccc0"),
		Block:   5,
	}}}

	if _, err := Inject(context.Background(), db, Config{
		Source: src, ForkBlock: 0, BlocksPerPeriod: 10, EndBlock: 20,
	}); err != nil {
		t.Fatalf("Inject: %v", err)
	}
	after := rawdb.ReadAccountSnapshot(db, bobKey)
	if !bytes.Equal(before, after) {
		t.Errorf("untouched account bytes changed:\n  before %x\n  after  %x", before, after)
	}
	if got := readAccountPeriod(t, db, bob); got != 0 {
		t.Errorf("untouched account period: got %d, want 0", got)
	}
}

// TestInjectIdempotent asserts running Inject twice is a no-op on the second
// pass (every record already has the computed period).
func TestInjectIdempotent(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()

	alice, _, slot1 := seedSnapshot(t, db, 30)

	src := &fakeSource{
		accounts: []AccountDiff{{Address: alice, Block: 20}},
		storage:  []StorageDiff{{Address: alice, Slot: slot1, Block: 25}},
	}
	cfg := Config{Source: src, ForkBlock: 0, BlocksPerPeriod: 10, EndBlock: 30}

	if _, err := Inject(context.Background(), db, cfg); err != nil {
		t.Fatalf("first Inject: %v", err)
	}
	// Re-run with a fresh fake (channels can't be re-ranged).
	cfg.Source = &fakeSource{
		accounts: src.accounts,
		storage:  src.storage,
	}
	stats, err := Inject(context.Background(), db, cfg)
	if err != nil {
		t.Fatalf("second Inject: %v", err)
	}
	if stats.AccountSnapshotsUpdated != 0 {
		t.Errorf("second pass rewrote %d account snapshots; expected 0", stats.AccountSnapshotsUpdated)
	}
	if stats.StorageSnapshotsUpdated != 0 {
		t.Errorf("second pass rewrote %d storage snapshots; expected 0", stats.StorageSnapshotsUpdated)
	}
}

// TestInjectDryRun asserts that --dry-run tallies but does not mutate.
func TestInjectDryRun(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()

	alice, _, _ := seedSnapshot(t, db, 20)
	before := append([]byte(nil), rawdb.ReadAccountSnapshot(db, crypto.Keccak256Hash(alice[:]))...)

	src := &fakeSource{accounts: []AccountDiff{{Address: alice, Block: 15}}}
	stats, err := Inject(context.Background(), db, Config{
		Source: src, ForkBlock: 0, BlocksPerPeriod: 10, EndBlock: 20, DryRun: true,
	})
	if err != nil {
		t.Fatalf("Inject dry: %v", err)
	}
	if stats.AccountSnapshotsUpdated != 1 {
		t.Errorf("dry-run should have counted 1 account, got %d", stats.AccountSnapshotsUpdated)
	}
	after := rawdb.ReadAccountSnapshot(db, crypto.Keccak256Hash(alice[:]))
	if !bytes.Equal(before, after) {
		t.Errorf("dry-run mutated disk:\n  before %x\n  after  %x", before, after)
	}
}

// TestInjectFileSource exercises the JSONL-backed Source end-to-end, the same
// path taken by the bintrie-benchmarks shell harness.
func TestInjectFileSource(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()

	alice, _, slot1 := seedSnapshot(t, db, 50)
	tmp := t.TempDir()
	path := filepath.Join(tmp, "diffs.jsonl")
	lines := "" +
		`{"kind":"account","block":12,"address":"` + alice.Hex() + `"}` + "\n" +
		`{"kind":"storage","block":33,"address":"` + alice.Hex() + `","slot":"` + slot1.Hex() + `"}` + "\n"
	if err := os.WriteFile(path, []byte(lines), 0o644); err != nil {
		t.Fatal(err)
	}

	src := NewFileSource(path)
	if _, err := Inject(context.Background(), db, Config{
		Source: src, ForkBlock: 0, BlocksPerPeriod: 10, EndBlock: 50,
	}); err != nil {
		t.Fatalf("Inject: %v", err)
	}
	if got := readAccountPeriod(t, db, alice); got != 1 {
		t.Errorf("alice period: got %d, want 1", got)
	}
	if period, _ := readStoragePeriod(t, db, alice, slot1); period != 3 {
		t.Errorf("slot period: got %d, want 3", period)
	}
}

// TestInjectBlockRangeFilter verifies that diffs outside [ForkBlock, EndBlock]
// are ignored even if the Source emits them.
func TestInjectBlockRangeFilter(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	alice, _, _ := seedSnapshot(t, db, 20)

	src := &fakeSource{accounts: []AccountDiff{
		{Address: alice, Block: 5},  // before fork → filtered
		{Address: alice, Block: 99}, // after end  → filtered
	}}
	stats, err := Inject(context.Background(), db, Config{
		Source: src, ForkBlock: 10, BlocksPerPeriod: 10, EndBlock: 20,
	})
	if err != nil {
		t.Fatalf("Inject: %v", err)
	}
	if stats.AccountSnapshotsUpdated != 0 {
		t.Errorf("updated %d but no diffs were in range", stats.AccountSnapshotsUpdated)
	}
}

// TestInjectMissingSnapshot asserts a diff whose account has no snapshot
// record (e.g. account was never committed) is counted as missing and skipped.
func TestInjectMissingSnapshot(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	seedSnapshot(t, db, 20)

	ghost := common.HexToAddress("0xddddddddddddddddddddddddddddddddddddd0dd")
	src := &fakeSource{accounts: []AccountDiff{{Address: ghost, Block: 15}}}
	stats, err := Inject(context.Background(), db, Config{
		Source: src, ForkBlock: 0, BlocksPerPeriod: 10, EndBlock: 20,
	})
	if err != nil {
		t.Fatalf("Inject: %v", err)
	}
	if stats.AccountSnapshotsMissing != 1 {
		t.Errorf("missing count: got %d, want 1", stats.AccountSnapshotsMissing)
	}
	if stats.AccountSnapshotsUpdated != 0 {
		t.Errorf("updated count: got %d, want 0", stats.AccountSnapshotsUpdated)
	}
}

// TestInspect exercises the JSON report produced by the inspect-periods CLI.
func TestInspect(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	alice, bob, slot1 := seedSnapshot(t, db, 50)

	src := &fakeSource{
		accounts: []AccountDiff{
			{Address: alice, Block: 10},
			{Address: bob, Block: 42},
		},
		storage: []StorageDiff{{Address: alice, Slot: slot1, Block: 30}},
	}
	if _, err := Inject(context.Background(), db, Config{
		Source: src, ForkBlock: 0, BlocksPerPeriod: 10, EndBlock: 50,
	}); err != nil {
		t.Fatalf("Inject: %v", err)
	}

	report, err := Inspect(context.Background(), db)
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if report.TotalAccounts != 2 {
		t.Errorf("total accounts: got %d, want 2", report.TotalAccounts)
	}
	if report.AccountsWithPeriod != 2 {
		t.Errorf("accounts with period: got %d, want 2", report.AccountsWithPeriod)
	}
	if report.MaxAccountPeriod != 4 {
		t.Errorf("max account period: got %d, want 4", report.MaxAccountPeriod)
	}
	if report.TotalStorageSlots != 1 {
		t.Errorf("total storage slots: got %d, want 1", report.TotalStorageSlots)
	}
	if report.StorageWithPeriod != 1 {
		t.Errorf("storage with period: got %d, want 1", report.StorageWithPeriod)
	}
	if report.MaxStoragePeriod != 3 {
		t.Errorf("max storage period: got %d, want 3", report.MaxStoragePeriod)
	}
}

func TestComputePeriod(t *testing.T) {
	cases := []struct {
		name                         string
		block, fork, blocksPerPeriod uint64
		want                         uint32
	}{
		{"before fork", 5, 10, 1000, 0},
		{"at fork", 10, 10, 1000, 0},
		{"one period in", 1010, 10, 1000, 1},
		{"many periods", 50_000, 0, 1000, 50},
		{"zero length saturates", 99, 0, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ComputePeriod(tc.block, tc.fork, tc.blocksPerPeriod)
			if got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}

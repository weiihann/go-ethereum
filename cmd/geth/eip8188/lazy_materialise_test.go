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
	"context"
	"math/big"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/inactive"
	"github.com/ethereum/go-ethereum/triedb/pathdb"
	"github.com/holiman/uint256"
)

// TestLazyMaterialiseAfterConvert exercises the EIP-8188 v2 hybrid-commit
// path end-to-end:
//  1. Build a state, convert subtrees into the inactive file (stubs in chaindb).
//  2. Re-open with the inactive file attached.
//  3. Modify one converted account's balance — this triggers a trie write
//     that descends into the stub. The lazy materialiser walks just the
//     modified path; off-path siblings stay as *expiredNode and fold into
//     the parent's hybrid (0x01) chaindb entry at commit time.
//  4. Verify both the modified account AND a still-stubbed sibling read
//     back correctly.
//  5. Verify the chaindb now contains at least one hybrid entry along the
//     modification path.
func TestLazyMaterialiseAfterConvert(t *testing.T) {
	tmpDir := t.TempDir()
	memDb := rawdb.NewMemoryDatabase()
	defer memDb.Close()

	tdb := triedb.NewDatabase(memDb, &triedb.Config{
		PathDB: &pathdb.Config{
			TrieCleanSize:   0,
			StateCleanSize:  0,
			WriteBufferSize: 0,
			NoAsyncFlush:    true,
		},
	})
	sdb := state.NewDatabase(tdb, nil)

	// Seed many accounts so keccak(addr) hashes spread across nibble buckets
	// and multiple inactive accounts cluster under a common branch (the
	// converter then emits that branch as a single subtree spanning several
	// leaves). Modifying one leaf in such a subtree forces the lazy
	// materialiser to leave its siblings as *expiredNode, which become
	// hybrid metadata at commit time.
	const numAddrs = 64
	addrs := make([]common.Address, numAddrs)
	wantBal := make(map[common.Address]uint64, len(addrs))
	for i := range addrs {
		addrs[i] = common.BytesToAddress([]byte{0xb0, 0xb0, 0xb0, byte(i)})
		wantBal[addrs[i]] = uint64(1000 + i)
	}
	st, err := state.New(types.EmptyRootHash, sdb)
	if err != nil {
		t.Fatalf("state.New: %v", err)
	}
	for _, addr := range addrs {
		st.SetBalance(addr, uint256.NewInt(wantBal[addr]), tracing.BalanceChangeUnspecified)
	}
	root, err := st.Commit(1, false, false)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := tdb.Commit(root, false); err != nil {
		t.Fatalf("tdb.Commit: %v", err)
	}

	// Inject mixed periods so identifier emits sub-trees rather than the
	// trie root: mark first half inactive (block 5 → period 1), second half
	// active (block 50 → period 10). The trie root stays live; some
	// interior subtree(s) get stubbed.
	rawdb.WriteHeader(memDb, &types.Header{Number: big.NewInt(100)})
	rawdb.WriteHeadHeaderHash(memDb, common.HexToHash("0x01"))
	// Mark the first 7/8 of accounts as inactive (block 5 → period 1).
	// Keep the last 1/8 active so the trie root stays live. With many
	// inactive accounts the identifier emits subtrees that span multiple
	// leaves, so a write to one leaf's path leaves siblings as *expiredNode.
	diffs := make([]AccountDiff, 0, len(addrs))
	cutoff := len(addrs) - len(addrs)/8
	for i, addr := range addrs {
		blk := uint64(5)
		if i >= cutoff {
			blk = 50
		}
		diffs = append(diffs, AccountDiff{Address: addr, Block: blk})
	}
	if _, err := Inject(context.Background(), memDb, Config{
		Source:          &fakeSource{accounts: diffs},
		ForkBlock:       0,
		BlocksPerPeriod: 5,
		EndBlock:        100,
	}); err != nil {
		t.Fatalf("Inject: %v", err)
	}

	// Convert.
	inactivePath := filepath.Join(tmpDir, "inactive.bin")
	file, err := inactive.Open(inactivePath, true)
	if err != nil {
		t.Fatalf("inactive.Open: %v", err)
	}
	stats, err := Convert(context.Background(), memDb, tdb, ConvertConfig{
		IdentifyConfig: IdentifyConfig{
			CurrentPeriod:  10,
			InactiveMinAge: 3,
			Scope:          ScopeAccount,
		},
		StateRoot:    root,
		InactiveFile: file,
	})
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("file.Close: %v", err)
	}
	if stats.SubtreesConverted == 0 {
		t.Fatalf("no subtrees converted; cannot exercise lazy materialiser")
	}
	t.Logf("Convert stats: %+v", stats)

	// Snapshot pre-modification chaindb stub count (for the post-write check).
	preStubBytes := countStubAndHybridEntries(t, memDb)

	// Re-open with the inactive file attached.
	tdb.Close()
	tdb2 := triedb.NewDatabase(memDb, &triedb.Config{
		PathDB: &pathdb.Config{
			TrieCleanSize:    0,
			StateCleanSize:   0,
			WriteBufferSize:  0,
			NoAsyncFlush:     true,
			InactiveFilePath: inactivePath,
		},
	})
	defer tdb2.Close()
	sdb2 := state.NewDatabase(tdb2, nil)

	// Modify one of the originally-inactive (stubbed) accounts. Pick from
	// the inactive group so the modification path actually descends into a
	// stub.
	target := addrs[0]
	newBal := uint64(99999)
	st2, err := state.New(root, sdb2)
	if err != nil {
		t.Fatalf("state.New (post-convert): %v", err)
	}
	st2.SetBalance(target, uint256.NewInt(newBal), tracing.BalanceChangeUnspecified)
	newRoot, err := st2.Commit(2, false, false)
	if err != nil {
		t.Fatalf("Commit (post-modify): %v", err)
	}
	if err := tdb2.Commit(newRoot, false); err != nil {
		t.Fatalf("tdb.Commit (post-modify): %v", err)
	}

	// Verify the modified balance is observable.
	st3, err := state.New(newRoot, sdb2)
	if err != nil {
		t.Fatalf("state.New (post-modify): %v", err)
	}
	if got := st3.GetBalance(target).Uint64(); got != newBal {
		t.Errorf("after modify: balance(%x) = %d, want %d", target, got, newBal)
	}

	// And every other (still-stubbed) account reads its original value.
	for _, addr := range addrs {
		if addr == target {
			continue
		}
		got := st3.GetBalance(addr).Uint64()
		if got != wantBal[addr] {
			t.Errorf("after modify: balance(%x) = %d, want %d (still-stubbed sibling)",
				addr, got, wantBal[addr])
		}
	}

	// Confirm that the chaindb now contains hybrid (0x01) entries along the
	// modified path. Pre-modify the chaindb only had primary stubs (0x00);
	// post-modify, at least one new hybrid entry must have been written.
	postCounts := countStubAndHybridEntries(t, memDb)
	if postCounts.hybrids == 0 {
		t.Errorf("expected at least one hybrid (0x01) entry after lazy materialise; got %+v", postCounts)
	}
	if postCounts.hybrids+postCounts.stubs == preStubBytes.hybrids+preStubBytes.stubs && postCounts.hybrids == 0 {
		t.Errorf("chaindb stub/hybrid composition unchanged after modification (pre=%+v post=%+v)",
			preStubBytes, postCounts)
	}
	t.Logf("chaindb pre-modify: %+v, post-modify: %+v", preStubBytes, postCounts)
}

// stubCounts tallies trie-node entries by classification.
type stubCounts struct {
	stubs   int // primary-stub markers (0x00)
	hybrids int // hybrid markers (0x01)
	rlp     int // standard RLP nodes (0xc0+)
	other   int
}

// countStubAndHybridEntries iterates the chaindb's trie-node keyspace and
// counts entries by their first byte.
func countStubAndHybridEntries(t *testing.T, db ethdb.Iteratee) stubCounts {
	t.Helper()
	var c stubCounts
	for _, prefix := range [][]byte{rawdb.TrieNodeAccountPrefix, rawdb.TrieNodeStoragePrefix} {
		it := db.NewIterator(prefix, nil)
		for it.Next() {
			val := it.Value()
			if len(val) == 0 {
				c.other++
				continue
			}
			switch {
			case val[0] == 0x00:
				c.stubs++
			case val[0] == 0x01:
				c.hybrids++
			case val[0] >= 0xc0:
				c.rlp++
			default:
				c.other++
			}
		}
		it.Release()
		if err := it.Error(); err != nil {
			t.Fatalf("iterator (%q): %v", prefix, err)
		}
	}
	return c
}

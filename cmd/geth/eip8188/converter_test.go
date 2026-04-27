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
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/inactive"
	"github.com/ethereum/go-ethereum/triedb/pathdb"
	"github.com/holiman/uint256"
)

// TestConvertEndToEnd builds a real triedb-backed state, runs Convert to move
// inactive subtrees out into a separate file, then re-opens the database
// with the file attached and verifies that reads of converted accounts still
// return the original values.
func TestConvertEndToEnd(t *testing.T) {
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

	// Seed several accounts. They live in the chaindb after Commit + flush.
	addrs := make([]common.Address, 6)
	wantBal := make(map[common.Address]uint64, len(addrs))
	for i := range addrs {
		addrs[i] = common.BytesToAddress([]byte{0xa0, byte(i)})
		wantBal[addrs[i]] = uint64(100 + i)
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

	// Inject periods so the identifier finds inactive subtrees.
	rawdb.WriteHeader(memDb, &types.Header{Number: big.NewInt(100)})
	rawdb.WriteHeadHeaderHash(memDb, common.HexToHash("0x01"))

	// First half of addrs are old (period 1, age 9 → inactive); second half
	// are recent (period 10, age 0 → active). With both classes present, the
	// trie root stays mixed and the converter only stubs interior subtrees,
	// leaving the disk's trie-root key intact so pathdb's loadLayers can
	// re-derive the same root hash on re-open.
	diffs := make([]AccountDiff, 0, len(addrs))
	for i, addr := range addrs {
		blk := uint64(5) // period 1 = inactive
		if i >= len(addrs)/2 {
			blk = 50 // period 10 = active at CurrentPeriod=10
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

	// Open the inactive file (created fresh in the temp dir).
	inactiveFilePath := filepath.Join(tmpDir, "inactive.bin")
	file, err := inactive.Open(inactiveFilePath, true)
	if err != nil {
		t.Fatalf("inactive.Open: %v", err)
	}

	// Convert. CurrentPeriod=10, threshold=3: every account has period 1
	// (block 5/5 = 1), age 9 >= 3 → inactive. Identifier emits the trie
	// root as the maximal inactive subtree.
	cfg := ConvertConfig{
		IdentifyConfig: IdentifyConfig{
			CurrentPeriod:  10,
			InactiveMinAge: 3,
			Scope:          "account",
		},
		StateRoot:    root,
		InactiveFile: file,
	}
	stats, err := Convert(context.Background(), memDb, tdb, cfg)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("file.Close: %v", err)
	}
	t.Logf("Convert stats: %+v", stats)
	if stats.SubtreesConverted == 0 {
		t.Fatalf("no subtrees converted; stats=%+v", stats)
	}
	if stats.BytesAppended == 0 {
		t.Errorf("no bytes appended; stats=%+v", stats)
	}
	if stats.ConversionErrors > 0 {
		t.Errorf("conversion errors: %d", stats.ConversionErrors)
	}

	// Re-open the database with the inactive file attached. Reads should
	// transparently follow the stub into the file and return the original
	// values.
	tdb.Close()
	tdb2 := triedb.NewDatabase(memDb, &triedb.Config{
		PathDB: &pathdb.Config{
			TrieCleanSize:    0,
			StateCleanSize:   0,
			WriteBufferSize:  0,
			NoAsyncFlush:     true,
			InactiveFilePath: inactiveFilePath,
		},
	})
	defer tdb2.Close()
	sdb2 := state.NewDatabase(tdb2, nil)
	st2, err := state.New(root, sdb2)
	if err != nil {
		t.Fatalf("state.New (post-convert): %v", err)
	}
	for _, addr := range addrs {
		bal := st2.GetBalance(addr)
		if bal.Uint64() != wantBal[addr] {
			t.Errorf("after convert: balance(%x) = %d, want %d", addr, bal.Uint64(), wantBal[addr])
		}
	}
}

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
	"math/rand"
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

// TestLazyMaterialiseHashInvariance is the hash-divergence canary that the
// existing TestLazyMaterialiseAfterConvert is missing.
//
// It constructs TWO identical states in two separate chaindbs:
//   - "stubbed":  state-A; gets converted, then modifications go through stubs
//     (lazy mat + hybrid commit + later: hybrid decode + re-modify)
//   - "control":  state-B; never converted; modifications go through normal
//     trie reads/writes
//
// For multiple rounds of modifications, both states receive the SAME
// account-balance updates. After each round we commit both and compare the
// resulting state roots. They MUST match — if the lazy-mat / hybrid-commit
// pipeline is hash-invariant, the on-disk shape difference must be invisible
// to consumers.
//
// Multiple rounds matter: the first round emits hybrids; subsequent rounds
// re-decode those hybrids and re-modify them. The handoff doc explicitly
// flagged "re-modification across nested hybrids" as exercised only at one
// level by the existing test — this test exercises multiple.
func TestLazyMaterialiseHashInvariance(t *testing.T) {
	const (
		numAddrs   = 4096 // wide enough to fan out across multiple top-level nibbles
		numRounds  = 30
		modsPerRnd = 50
		seed       = int64(1)
	)
	rng := rand.New(rand.NewSource(seed))

	// Build identical states in two separate chaindbs.
	stubbed := newRig(t, "stubbed")
	control := newRig(t, "control")
	defer stubbed.close()
	defer control.close()

	addrs := make([]common.Address, numAddrs)
	wantBal := make(map[common.Address]uint64, numAddrs)
	for i := range addrs {
		addrs[i] = common.BytesToAddress([]byte{0xb0, 0xb0, byte(i / 256), byte(i % 256)})
		wantBal[addrs[i]] = uint64(1000 + i)
	}

	// Round 0: seed initial state. Both rigs apply identical writes.
	rootStubbed := stubbed.commit(t, types.EmptyRootHash, 1, func(s *state.StateDB) {
		for _, a := range addrs {
			s.SetBalance(a, uint256.NewInt(wantBal[a]), tracing.BalanceChangeUnspecified)
		}
	})
	rootControl := control.commit(t, types.EmptyRootHash, 1, func(s *state.StateDB) {
		for _, a := range addrs {
			s.SetBalance(a, uint256.NewInt(wantBal[a]), tracing.BalanceChangeUnspecified)
		}
	})
	if rootStubbed != rootControl {
		t.Fatalf("seed roots already differ: stubbed=%x control=%x", rootStubbed, rootControl)
	}

	// Convert the "stubbed" rig only. Mark 7/8 of accounts as inactive (period 1)
	// and the last 1/8 as active (period 10), so the trie root stays live but
	// internal subtrees get stubbed. With current=10 / min-age=2:
	//   - inactive group: 10-1 = 9 >= 2 → INACTIVE
	//   - active group:   10-10 = 0 < 2 → ACTIVE → root stays mixed
	cutoff := numAddrs - numAddrs/8
	convertRig(t, stubbed, addrs, rootStubbed, cutoff)
	stubbed.reopenWithInactive(t)

	// N rounds: each round mixes balance changes, nonce bumps, code installs,
	// storage writes/deletes, account creation, and selfdestructs — the same
	// shape of mutations real mainnet blocks produce. Compare roots after each.
	for round := 0; round < numRounds; round++ {
		ops := genOps(rng, addrs, modsPerRnd)
		mut := func(s *state.StateDB) { applyOps(s, ops) }
		blockNum := uint64(2 + round)
		newRootStubbed := stubbed.commit(t, rootStubbed, blockNum, mut)
		newRootControl := control.commit(t, rootControl, blockNum, mut)
		if newRootStubbed != newRootControl {
			t.Fatalf("round %d: state root divergence after %d mutations:\n  stubbed=%x\n  control=%x\n  ops=%+v",
				round, len(ops), newRootStubbed, newRootControl, ops)
		}
		t.Logf("round %d: %d mutations, root=%x (matches control)", round, len(ops), newRootStubbed)
		rootStubbed = newRootStubbed
		rootControl = newRootControl
	}
}

// op describes one mutation applied to both rigs identically.
type op struct {
	kind    string         // "balance" | "nonce" | "code" | "sstore" | "create" | "destruct"
	addr    common.Address // target account
	balance uint64         // for kind=="balance" or kind=="create"
	nonce   uint64         // for kind=="nonce"
	code    []byte         // for kind=="code" or kind=="create"
	slot    common.Hash    // for kind=="sstore"
	value   common.Hash    // for kind=="sstore" (zero → clear)
}

func genOps(rng *rand.Rand, addrs []common.Address, n int) []op {
	out := make([]op, 0, n)
	for i := 0; i < n; i++ {
		switch rng.Intn(4) {
		case 0:
			out = append(out, op{kind: "balance", addr: addrs[rng.Intn(len(addrs))], balance: uint64(rng.Intn(1_000_000_000))})
		case 1:
			out = append(out, op{kind: "nonce", addr: addrs[rng.Intn(len(addrs))], nonce: uint64(rng.Intn(1000))})
		case 2:
			out = append(out, op{
				kind: "code",
				addr: addrs[rng.Intn(len(addrs))],
				code: []byte{0x60, byte(rng.Intn(256)), 0x60, 0x00, 0xf3}, // PUSH1 N PUSH1 0 RETURN
			})
		case 3:
			out = append(out, op{
				kind:  "sstore",
				addr:  addrs[rng.Intn(len(addrs))],
				slot:  common.BigToHash(big.NewInt(int64(rng.Intn(8)))),
				value: common.BigToHash(big.NewInt(int64(rng.Intn(1000)))),
			})
		}
	}
	return out
}

func applyOps(s *state.StateDB, ops []op) {
	for _, o := range ops {
		switch o.kind {
		case "balance":
			s.SetBalance(o.addr, uint256.NewInt(o.balance), tracing.BalanceChangeUnspecified)
		case "nonce":
			s.SetNonce(o.addr, o.nonce, tracing.NonceChangeUnspecified)
		case "code":
			s.SetCode(o.addr, o.code, tracing.CodeChangeUnspecified)
		case "sstore":
			s.SetState(o.addr, o.slot, o.value)
		case "create":
			s.CreateAccount(o.addr)
			s.SetBalance(o.addr, uint256.NewInt(o.balance), tracing.BalanceChangeUnspecified)
		case "destruct":
			s.SelfDestruct(o.addr)
		}
	}
}

// rig holds one independent state-database under test.
type rig struct {
	name         string
	tmpDir       string
	memDb        ethdb.Database
	tdb          *triedb.Database
	sdb          state.Database
	inactivePath string
}

func newRig(t *testing.T, name string) *rig {
	t.Helper()
	tmpDir := t.TempDir()
	memDb := rawdb.NewMemoryDatabase()
	tdb := triedb.NewDatabase(memDb, &triedb.Config{
		PathDB: &pathdb.Config{
			TrieCleanSize:   0,
			StateCleanSize:  0,
			WriteBufferSize: 0,
			NoAsyncFlush:    true,
		},
	})
	return &rig{
		name:         name,
		tmpDir:       tmpDir,
		memDb:        memDb,
		tdb:          tdb,
		sdb:          state.NewDatabase(tdb, nil),
		inactivePath: filepath.Join(tmpDir, "inactive.bin"),
	}
}

func (r *rig) close() {
	if r.tdb != nil {
		r.tdb.Close()
	}
	if r.memDb != nil {
		r.memDb.Close()
	}
}

// commit opens the state at parent, applies mut, commits the state and the
// triedb, and returns the new root. Mirrors how a block import would commit.
func (r *rig) commit(t *testing.T, parent common.Hash, blockNum uint64, mut func(*state.StateDB)) common.Hash {
	t.Helper()
	st, err := state.New(parent, r.sdb)
	if err != nil {
		t.Fatalf("[%s] state.New(%x): %v", r.name, parent, err)
	}
	mut(st)
	root, err := st.Commit(blockNum, false, false)
	if err != nil {
		t.Fatalf("[%s] state.Commit: %v", r.name, err)
	}
	if err := r.tdb.Commit(root, false); err != nil {
		t.Fatalf("[%s] tdb.Commit: %v", r.name, err)
	}
	return root
}

// reopenWithInactive closes the triedb and reopens it pointed at the inactive
// file, simulating a node restart after convert-inactive.
func (r *rig) reopenWithInactive(t *testing.T) {
	t.Helper()
	r.tdb.Close()
	r.tdb = triedb.NewDatabase(r.memDb, &triedb.Config{
		PathDB: &pathdb.Config{
			TrieCleanSize:    0,
			StateCleanSize:   0,
			WriteBufferSize:  0,
			NoAsyncFlush:     true,
			InactiveFilePath: r.inactivePath,
		},
	})
	r.sdb = state.NewDatabase(r.tdb, nil)
}

// convertRig runs the EIP-8188 convert pipeline against r's chaindb so the
// (account) trie is replaced by stubs pointing into r.inactivePath. Accounts
// in addrs[0:activeCutoff] are marked inactive (period 1); the rest are
// marked active (period 10) so the root stays mixed and the state machinery
// can still load it via standard trie reads.
func convertRig(t *testing.T, r *rig, addrs []common.Address, stateRoot common.Hash, activeCutoff int) {
	t.Helper()
	// Inject: write per-account periods.
	rawdb.WriteHeader(r.memDb, &types.Header{Number: big.NewInt(100)})
	rawdb.WriteHeadHeaderHash(r.memDb, common.HexToHash("0x01"))
	diffs := make([]AccountDiff, 0, len(addrs))
	for i, a := range addrs {
		blk := uint64(5)
		if i >= activeCutoff {
			blk = 50
		}
		diffs = append(diffs, AccountDiff{Address: a, Block: blk})
	}
	if _, err := Inject(context.Background(), r.memDb, Config{
		Source:          &fakeSource{accounts: diffs},
		ForkBlock:       0,
		BlocksPerPeriod: 5,
		EndBlock:        100,
	}); err != nil {
		t.Fatalf("[%s] Inject: %v", r.name, err)
	}

	// Convert with min-age=2 so leaves with period <= 1 (i.e. all of ours)
	// are inactive. current=10 satisfies 10-1 >= 2.
	file, err := inactive.Open(r.inactivePath, true)
	if err != nil {
		t.Fatalf("[%s] inactive.Open: %v", r.name, err)
	}
	stats, err := Convert(context.Background(), r.memDb, r.tdb, ConvertConfig{
		IdentifyConfig: IdentifyConfig{
			CurrentPeriod:  10,
			InactiveMinAge: 2,
			Scope:          ScopeAccount,
		},
		StateRoot:    stateRoot,
		InactiveFile: file,
	})
	if err != nil {
		t.Fatalf("[%s] Convert: %v", r.name, err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("[%s] file.Close: %v", r.name, err)
	}
	if stats.SubtreesConverted == 0 {
		t.Fatalf("[%s] no subtrees converted", r.name)
	}
	if stats.ConversionErrors != 0 {
		t.Fatalf("[%s] %d conversion errors", r.name, stats.ConversionErrors)
	}
	t.Logf("[%s] Convert stats: %+v", r.name, stats)
}

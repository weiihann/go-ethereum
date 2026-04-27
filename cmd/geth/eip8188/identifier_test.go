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
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/pathdb"
	"github.com/holiman/uint256"
)

func hashAddr(a common.Address) common.Hash { return crypto.Keccak256Hash(a[:]) }
func rlpDecode(b []byte, v any) error       { return rlp.DecodeBytes(b, v) }

// fakeNode describes a single visit a NodeIterator will make. Tests construct
// pre-order sequences of these to drive the core loop deterministically.
type fakeNode struct {
	path    []byte // hex-nibble path; for leaves include the terminator 0x10
	hash    common.Hash
	isLeaf  bool
	leafKey []byte // raw leaf key (for the lookup function)
}

// fakeIterator is a minimal NodeIterator that replays a fixed sequence of
// fakeNode visits. It implements only the methods runCoreLoop touches.
type fakeIterator struct {
	nodes []fakeNode
	idx   int // -1 before first Next call
}

func newFakeIterator(nodes []fakeNode) *fakeIterator {
	return &fakeIterator{nodes: nodes, idx: -1}
}

func (f *fakeIterator) Next(_ bool) bool {
	f.idx++
	return f.idx < len(f.nodes)
}

func (f *fakeIterator) cur() fakeNode { return f.nodes[f.idx] }

func (f *fakeIterator) Error() error                  { return nil }
func (f *fakeIterator) Hash() common.Hash             { return f.cur().hash }
func (f *fakeIterator) Path() []byte                  { return f.cur().path }
func (f *fakeIterator) Leaf() bool                    { return f.cur().isLeaf }
func (f *fakeIterator) LeafKey() []byte               { return f.cur().leafKey }
func (f *fakeIterator) LeafBlob() []byte              { return nil }
func (f *fakeIterator) LeafProof() [][]byte           { return nil }
func (f *fakeIterator) Parent() common.Hash           { return common.Hash{} }
func (f *fakeIterator) NodeBlob() []byte              { return nil }
func (f *fakeIterator) AddResolver(trie.NodeResolver) {}

// makeIdentifier returns an *identifier with collector emit and the supplied
// inactivity threshold. emitted is filled in-order as subtrees are emitted.
func makeIdentifier(currentPeriod, threshold uint32, emitted *[]InactiveSubtree) *identifier {
	return &identifier{
		cfg: IdentifyConfig{
			CurrentPeriod:  currentPeriod,
			InactiveMinAge: threshold,
		},
		emit: func(s InactiveSubtree) {
			*emitted = append(*emitted, s)
		},
	}
}

// term appends the trie leaf terminator 0x10 to a hex-nibble path.
func term(p []byte) []byte { return append(append([]byte{}, p...), 0x10) }

func hashFromByte(b byte) common.Hash {
	var h common.Hash
	h[0] = b
	return h
}

// TestRunCoreLoop_AllInactive: every leaf is inactive → emit root.
func TestRunCoreLoop_AllInactive(t *testing.T) {
	// Tree:
	//   root (path="", hash=R)
	//   ├── leaf at "10[10]", key=0x10  (inactive)
	//   └── leaf at "20[10]", key=0x20  (inactive)
	rootHash := hashFromByte(0xaa)
	nodes := []fakeNode{
		{path: []byte{}, hash: rootHash},
		{path: term([]byte{0x1, 0x0}), isLeaf: true, leafKey: []byte{0x10}},
		{path: term([]byte{0x2, 0x0}), isLeaf: true, leafKey: []byte{0x20}},
	}
	periods := map[byte]uint32{0x10: 0, 0x20: 0}
	lookup := func(k []byte) (uint32, bool) {
		p, ok := periods[k[0]]
		return p, ok
	}

	var emitted []InactiveSubtree
	id := makeIdentifier(5, 1, &emitted)
	if err := id.runCoreLoop(newFakeIterator(nodes), lookup, "account", common.Hash{}, true); err != nil {
		t.Fatalf("runCoreLoop: %v", err)
	}

	if len(emitted) != 1 {
		t.Fatalf("emitted %d subtrees, want 1: %+v", len(emitted), emitted)
	}
	if emitted[0].Hash != rootHash {
		t.Errorf("emitted hash = %x, want %x", emitted[0].Hash, rootHash)
	}
	if emitted[0].Path != "" {
		t.Errorf("emitted path = %q, want empty (root)", emitted[0].Path)
	}
	if emitted[0].LeafCount != 2 {
		t.Errorf("emitted leafCount = %d, want 2", emitted[0].LeafCount)
	}
}

// TestRunCoreLoop_AllActive: no leaf is inactive → emit nothing.
func TestRunCoreLoop_AllActive(t *testing.T) {
	nodes := []fakeNode{
		{path: []byte{}, hash: hashFromByte(0xaa)},
		{path: term([]byte{0x1, 0x0}), isLeaf: true, leafKey: []byte{0x10}},
		{path: term([]byte{0x2, 0x0}), isLeaf: true, leafKey: []byte{0x20}},
	}
	// Both leaves at currentPeriod, age 0 → active.
	lookup := func(k []byte) (uint32, bool) { return 5, true }

	var emitted []InactiveSubtree
	id := makeIdentifier(5, 1, &emitted)
	if err := id.runCoreLoop(newFakeIterator(nodes), lookup, "account", common.Hash{}, true); err != nil {
		t.Fatalf("runCoreLoop: %v", err)
	}

	if len(emitted) != 0 {
		t.Fatalf("emitted %d subtrees, want 0: %+v", len(emitted), emitted)
	}
}

// TestRunCoreLoop_MixedAtTopLevel: one branch fully inactive, the other has
// an active leaf → emit just the inactive branch (path "1"), NOT the root.
func TestRunCoreLoop_MixedAtTopLevel(t *testing.T) {
	// Tree:
	//   root (path="", hash=R)
	//   ├── branch (path="1", hash=A)         all inactive → MAXIMAL
	//   │   ├── leaf at "12[10]", key=0x12
	//   │   └── leaf at "13[10]", key=0x13
	//   └── branch (path="2", hash=B)         mixed
	//       ├── leaf at "24[10]", key=0x24    (active)
	//       └── leaf at "25[10]", key=0x25    (inactive — but parent mixed,
	//                                          and a leaf has no standalone
	//                                          hash so NOT emitted)
	rootHash := hashFromByte(0xee)
	branchA := hashFromByte(0xaa)
	branchB := hashFromByte(0xbb)
	nodes := []fakeNode{
		{path: []byte{}, hash: rootHash},
		{path: []byte{0x1}, hash: branchA},
		{path: term([]byte{0x1, 0x2}), isLeaf: true, leafKey: []byte{0x12}},
		{path: term([]byte{0x1, 0x3}), isLeaf: true, leafKey: []byte{0x13}},
		{path: []byte{0x2}, hash: branchB},
		{path: term([]byte{0x2, 0x4}), isLeaf: true, leafKey: []byte{0x24}},
		{path: term([]byte{0x2, 0x5}), isLeaf: true, leafKey: []byte{0x25}},
	}
	periods := map[byte]uint32{
		0x12: 0, // inactive
		0x13: 0, // inactive
		0x24: 5, // active
		0x25: 0, // inactive (but won't be emitted — leaf-only)
	}
	lookup := func(k []byte) (uint32, bool) {
		p, ok := periods[k[0]]
		return p, ok
	}

	var emitted []InactiveSubtree
	id := makeIdentifier(5, 1, &emitted)
	if err := id.runCoreLoop(newFakeIterator(nodes), lookup, "account", common.Hash{}, true); err != nil {
		t.Fatalf("runCoreLoop: %v", err)
	}

	if len(emitted) != 1 {
		t.Fatalf("emitted %d subtrees, want 1: %+v", len(emitted), emitted)
	}
	got := emitted[0]
	if got.Hash != branchA {
		t.Errorf("emitted hash = %x, want branchA=%x", got.Hash, branchA)
	}
	if got.Path != "01" {
		t.Errorf("emitted path = %q, want %q", got.Path, "01")
	}
	if got.LeafCount != 2 {
		t.Errorf("emitted leafCount = %d, want 2", got.LeafCount)
	}
}

// TestRunCoreLoop_NestedInactive: an inactive subtree NESTED inside a fully
// inactive parent → only the OUTER (parent) is emitted, not the inner.
func TestRunCoreLoop_NestedInactive(t *testing.T) {
	// Tree:
	//   root (path="", hash=R) — all leaves inactive → root is emitted
	//   └── branch (path="1", hash=A) — all inactive
	//       ├── branch (path="12", hash=B) — all inactive (would be emitted
	//       │   ├── leaf at "120[10]", key=0x12  if parent A weren't also inactive)
	//       │   └── leaf at "121[10]", key=0x12  -- duplicate key but distinct path
	//       └── leaf at "13[10]", key=0x13
	rootHash := hashFromByte(0xee)
	branchA := hashFromByte(0xaa)
	branchB := hashFromByte(0xbb)
	nodes := []fakeNode{
		{path: []byte{}, hash: rootHash},
		{path: []byte{0x1}, hash: branchA},
		{path: []byte{0x1, 0x2}, hash: branchB},
		{path: term([]byte{0x1, 0x2, 0x0}), isLeaf: true, leafKey: []byte{0x12}},
		{path: term([]byte{0x1, 0x2, 0x1}), isLeaf: true, leafKey: []byte{0x21}}, // distinct logical key
		{path: term([]byte{0x1, 0x3}), isLeaf: true, leafKey: []byte{0x13}},
	}
	// All inactive.
	lookup := func(_ []byte) (uint32, bool) { return 0, true }

	var emitted []InactiveSubtree
	id := makeIdentifier(5, 1, &emitted)
	if err := id.runCoreLoop(newFakeIterator(nodes), lookup, "account", common.Hash{}, true); err != nil {
		t.Fatalf("runCoreLoop: %v", err)
	}

	if len(emitted) != 1 {
		t.Fatalf("emitted %d subtrees, want 1 (root subsumes inner): %+v", len(emitted), emitted)
	}
	if emitted[0].Hash != rootHash {
		t.Errorf("emitted hash = %x, want root=%x", emitted[0].Hash, rootHash)
	}
	if emitted[0].LeafCount != 3 {
		t.Errorf("emitted leafCount = %d, want 3", emitted[0].LeafCount)
	}
}

// TestRunCoreLoop_EmbeddedInactiveNotEmitted: an inactive subtree whose root
// has Hash() == zero (embedded in parent, < 32 bytes) is NOT emitted, even
// when its parent is mixed.
func TestRunCoreLoop_EmbeddedInactiveNotEmitted(t *testing.T) {
	// Tree:
	//   root (path="", hash=R)
	//   ├── branch (path="1", hash=zero — EMBEDDED) all inactive
	//   │   └── leaf at "12[10]", key=0x12
	//   └── leaf at "2[10]", key=0x20  (active → root is mixed)
	rootHash := hashFromByte(0xee)
	nodes := []fakeNode{
		{path: []byte{}, hash: rootHash},
		{path: []byte{0x1}, hash: common.Hash{}}, // embedded
		{path: term([]byte{0x1, 0x2}), isLeaf: true, leafKey: []byte{0x12}},
		{path: term([]byte{0x2}), isLeaf: true, leafKey: []byte{0x20}},
	}
	periods := map[byte]uint32{
		0x12: 0, // inactive
		0x20: 5, // active
	}
	lookup := func(k []byte) (uint32, bool) {
		p, ok := periods[k[0]]
		return p, ok
	}

	var emitted []InactiveSubtree
	id := makeIdentifier(5, 1, &emitted)
	if err := id.runCoreLoop(newFakeIterator(nodes), lookup, "account", common.Hash{}, true); err != nil {
		t.Fatalf("runCoreLoop: %v", err)
	}

	if len(emitted) != 0 {
		t.Fatalf("emitted %d subtrees, want 0 (embedded skipped, root mixed): %+v",
			len(emitted), emitted)
	}
}

// TestRunCoreLoop_SnapshotMismatch: a leaf whose period the lookup can't
// resolve is treated as active (defensive). Ensures no false-positive emission.
func TestRunCoreLoop_SnapshotMismatch(t *testing.T) {
	rootHash := hashFromByte(0xee)
	nodes := []fakeNode{
		{path: []byte{}, hash: rootHash},
		{path: term([]byte{0x1, 0x0}), isLeaf: true, leafKey: []byte{0x10}},
		{path: term([]byte{0x2, 0x0}), isLeaf: true, leafKey: []byte{0x20}},
	}
	// 0x10 is inactive; 0x20 has no snapshot record.
	lookup := func(k []byte) (uint32, bool) {
		if k[0] == 0x10 {
			return 0, true
		}
		return 0, false
	}

	var emitted []InactiveSubtree
	id := makeIdentifier(5, 1, &emitted)
	if err := id.runCoreLoop(newFakeIterator(nodes), lookup, "account", common.Hash{}, true); err != nil {
		t.Fatalf("runCoreLoop: %v", err)
	}

	if len(emitted) != 0 {
		t.Fatalf("emitted %d subtrees, want 0 (mismatch → active → not all inactive)",
			len(emitted))
	}
	if id.stats.SnapshotMismatches != 1 {
		t.Errorf("SnapshotMismatches = %d, want 1", id.stats.SnapshotMismatches)
	}
}

// TestIsInactive covers the threshold edge cases.
func TestIsInactive(t *testing.T) {
	cases := []struct {
		current, threshold, leaf uint32
		want                     bool
	}{
		{current: 5, threshold: 1, leaf: 5, want: false}, // age 0
		{current: 5, threshold: 1, leaf: 4, want: true},  // age 1, == threshold
		{current: 5, threshold: 2, leaf: 4, want: false}, // age 1, < threshold
		{current: 5, threshold: 2, leaf: 3, want: true},  // age 2, == threshold
		{current: 5, threshold: 1, leaf: 7, want: false}, // future leaf, defensive active
		{current: 0, threshold: 1, leaf: 0, want: false}, // age 0, no fork yet
	}
	for _, tc := range cases {
		var emitted []InactiveSubtree
		id := makeIdentifier(tc.current, tc.threshold, &emitted)
		if got := id.isInactive(tc.leaf); got != tc.want {
			t.Errorf("isInactive(current=%d threshold=%d leaf=%d) = %v, want %v",
				tc.current, tc.threshold, tc.leaf, got, tc.want)
		}
	}
}

// TestIdentifyEndToEnd is a triedb-backed integration test. It builds a real
// state with several accounts via state.StateDB.Commit, then rewrites a few
// snapshot records via Inject to give them non-zero periods, then runs
// Identify and asserts the result reflects the configured threshold.
//
// This verifies the trie iterator + snapshot iterator synchronization that
// the unit tests cover only via the fake iterator.
func TestIdentifyEndToEnd(t *testing.T) {
	memDb := rawdb.NewMemoryDatabase()
	defer memDb.Close()

	// pathdb with NoAsyncFlush so Commit is synchronous in tests.
	tdb := triedb.NewDatabase(memDb, &triedb.Config{
		PathDB: &pathdb.Config{
			TrieCleanSize:   0,
			StateCleanSize:  0,
			WriteBufferSize: 0,
			NoAsyncFlush:    true,
		},
	})
	defer tdb.Close()
	sdb := state.NewDatabase(tdb, nil)

	// Seed several accounts. Some will be marked "old" via Inject and the
	// rest left at period 0 = current period — i.e. active.
	addrs := []common.Address{
		common.BytesToAddress([]byte("acct-aa")),
		common.BytesToAddress([]byte("acct-bb")),
		common.BytesToAddress([]byte("acct-cc")),
		common.BytesToAddress([]byte("acct-dd")),
	}
	st, err := state.New(types.EmptyRootHash, sdb)
	if err != nil {
		t.Fatalf("state.New: %v", err)
	}
	for i, addr := range addrs {
		st.SetBalance(addr, uint256.NewInt(uint64(100+i)), tracing.BalanceChangeUnspecified)
		st.SetNonce(addr, uint64(i+1), tracing.NonceChangeUnspecified)
	}
	root, err := st.Commit(1, false, false)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := tdb.Commit(root, false); err != nil {
		t.Fatalf("tdb.Commit: %v", err)
	}

	// Inject periods for two of the four accounts, simulating writes at an
	// older block. With BlocksPerPeriod=10, block 5 → period 0, block 50 →
	// period 5. We mark "acct-aa" and "acct-bb" as written in period 0 (old).
	src := &fakeSource{accounts: []AccountDiff{
		{Address: addrs[0], Block: 5},
		{Address: addrs[1], Block: 5},
	}}

	// Seed a head header so Inject's range check passes.
	endHeader := &types.Header{Number: big.NewInt(100)}
	rawdb.WriteHeader(memDb, endHeader)
	rawdb.WriteHeadHeaderHash(memDb, endHeader.Hash())

	if _, err := Inject(context.Background(), memDb, Config{
		Source:          src,
		ForkBlock:       0,
		BlocksPerPeriod: 10,
		EndBlock:        100,
	}); err != nil {
		t.Fatalf("Inject: %v", err)
	}

	// CurrentPeriod=10, threshold=2 → leaves with period <= 8 are inactive.
	// addrs[0] and addrs[1] have period 0 → inactive.
	// addrs[2] and addrs[3] have period 0 too (default) — but their snapshot
	// records still encode period=0, so they would also be flagged inactive
	// by this test. To make the test interesting, we expect "no maximal
	// inactive subtree" because the trie is small enough that all leaves
	// share branches and at least the root is mixed only when at least one
	// leaf is active.
	//
	// Set up: rewrite addrs[2] and addrs[3] snapshot records with period=10
	// (current = active). The Inject above already wrote the period=0
	// records; we want to flip these two to active.
	for _, addr := range addrs[2:] {
		acct := types.StateAccount{
			Nonce:    uint64(0),
			Balance:  uint256.NewInt(0),
			Root:     types.EmptyRootHash,
			CodeHash: types.EmptyCodeHash[:],
		}
		// Re-derive the actual values committed (we don't have them here, but
		// the period-only rewrite uses SlimAccountRLPWithPeriod with the SAME
		// account fields as Inject would). We read the existing record,
		// decode it, and rewrite with the desired period.
		existing := rawdb.ReadAccountSnapshot(memDb, hashAddr(addr))
		var slim types.SlimAccount
		if err := rlpDecode(existing, &slim); err != nil {
			t.Fatalf("decode existing slim: %v", err)
		}
		acct.Nonce = slim.Nonce
		acct.Balance = slim.Balance
		blob := types.SlimAccountRLPWithPeriod(acct, 10)
		rawdb.WriteAccountSnapshot(memDb, hashAddr(addr), blob)
	}

	// Run identification. CurrentPeriod=10, InactiveMinAge=2 → leaves with
	// period <= 8 are inactive. addrs[0..1] have period 0 → inactive;
	// addrs[2..3] have period 10 → active. The trie root is mixed, so we
	// expect inactive subtree(s) covering the addrs[0..1] branch but NOT
	// the trie root.
	var emitted []InactiveSubtree
	stats, err := Identify(context.Background(), tdb, root, IdentifyConfig{
		CurrentPeriod:  10,
		InactiveMinAge: 2,
		Scope:          "account",
	}, func(s InactiveSubtree) { emitted = append(emitted, s) })
	if err != nil {
		t.Fatalf("Identify: %v", err)
	}
	if stats.AccountsScanned != 4 {
		t.Errorf("AccountsScanned = %d, want 4", stats.AccountsScanned)
	}
	// We should find at least one inactive subtree (covering the inactive
	// accounts) but never the entire root (since some accounts are active).
	if len(emitted) == 0 {
		t.Fatalf("no inactive subtrees emitted; want at least 1")
	}
	for _, s := range emitted {
		if s.Path == "" {
			t.Errorf("emitted root subtree but trie has active leaves: %+v", s)
		}
	}
	t.Logf("emitted %d inactive subtree(s); first: %+v", len(emitted), emitted[0])
}

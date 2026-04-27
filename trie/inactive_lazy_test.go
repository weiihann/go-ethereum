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

package trie

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb/inactive"
)

// TestLazyMaterialiseProducesHybrid: build a multi-leaf trie subtree,
// encode it as a v2 blob, mount it behind an *expiredNode in a trie, modify
// ONE leaf, commit, and verify that:
//  1. The modified value is observable via the partial subtree.
//  2. Other leaves still read correctly via the partial subtree's
//     *expiredNode siblings (folded back into the inactive blob).
//  3. Committing produces at least one hybrid (0x01) node in the nodeset.
func TestLazyMaterialiseProducesHybrid(t *testing.T) {
	// Build a trie with several 32-byte keys that share a common first
	// nibble so they cluster under one branch of the root fullNode. We use
	// a fixed first byte (0xa0) and add an index in the second byte to
	// vary the rest. With 32-byte keys plus a non-trivial value, each
	// leaf's RLP exceeds 32 bytes — so the leaves are HASHED children
	// (not embedded), which is the prerequisite for *expiredNode
	// substitution and hybrid emission.
	tr := NewEmpty(newTestDatabase(rawdb.NewMemoryDatabase(), rawdb.HashScheme))
	kv := make(map[string][]byte, 8)
	for i := 0; i < 8; i++ {
		var key [32]byte
		copy(key[:], crypto.Keccak256([]byte{byte(i)}))
		// Force a shared prefix on byte 0 so the root has a branch where
		// these 8 leaves cluster under one path.
		key[0] = 0xa0
		val := make([]byte, 64)
		binary.BigEndian.PutUint64(val[0:8], uint64(0xdeadbeef00+i))
		kv[string(key[:])] = val
	}
	for k, v := range kv {
		if err := tr.Update([]byte(k), v); err != nil {
			t.Fatalf("Update(%x): %v", k, err)
		}
	}
	_ = tr.Hash()

	// Encode as v2 blob.
	blob, err := EncodeInactiveBlob(tr.root)
	if err != nil {
		t.Fatalf("EncodeInactiveBlob: %v", err)
	}
	hdr, err := inactive.ParseHeader(blob)
	if err != nil {
		t.Fatalf("ParseHeader: %v", err)
	}

	// Build a fresh trie whose root is an *expiredNode pointing at the blob.
	tr2 := NewEmpty(newTestDatabase(rawdb.NewMemoryDatabase(), rawdb.HashScheme))
	tr2.SetArchiveResolver(blobReaderFor(blob))
	tr2.root = &expiredNode{
		blobOffset:     0,
		nodeFileOffset: uint64(hdr.RootOffset),
		size:           hdr.RootSize,
		hash:           common.HexToHash("0xfeed"),
	}

	// Modify ONE key. The lazy materialiser walks just this path.
	var target string
	for k := range kv {
		target = k
		break
	}
	newVal := bytes.Repeat([]byte("M"), 64)
	if err := tr2.Update([]byte(target), newVal); err != nil {
		t.Fatalf("Update: %v", err)
	}

	// Verify the modification is observable on the partial subtree.
	got, err := tr2.Get([]byte(target))
	if err != nil {
		t.Fatalf("Get(target): %v", err)
	}
	if !bytes.Equal(got, newVal) {
		t.Errorf("Get(target) = %x, want %x", got, newVal)
	}

	// Other keys still read their original values via the partial subtree's
	// *expiredNode siblings (which descend back into the inactive blob).
	for k, v := range kv {
		if k == target {
			continue
		}
		got, err := tr2.Get([]byte(k))
		if err != nil {
			t.Errorf("Get(%x): %v", []byte(k), err)
			continue
		}
		if !bytes.Equal(got, v) {
			t.Errorf("Get(%x) = %x, want %x (still-stubbed sibling)", []byte(k), got, v)
		}
	}

	// Commit and inspect the nodeset for hybrid entries.
	_, nodes := tr2.Commit(false)
	if nodes == nil {
		t.Fatalf("Commit produced no nodeset")
	}

	hybridCount, regularCount := classifyNodeset(nodes)
	if hybridCount == 0 {
		t.Errorf("expected at least one hybrid node in the commit set; got %d hybrid + %d regular",
			hybridCount, regularCount)
	}
	t.Logf("commit produced %d hybrid + %d regular nodes", hybridCount, regularCount)
}

// classifyNodeset counts nodes by their on-disk classification.
func classifyNodeset(set *trienode.NodeSet) (hybrid, regular int) {
	set.ForEachWithOrder(func(_ string, n *trienode.Node) {
		if len(n.Blob) == 0 {
			return // deleted node
		}
		switch {
		case n.Blob[0] == inactive.HybridMarker:
			hybrid++
		case n.Blob[0] >= 0xc0:
			regular++
		}
	})
	return hybrid, regular
}

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
	"crypto/sha256"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

// buildLiveTrie inserts the given 32-byte keys (all same length, so no value
// ever lands in a branch's terminator slot) and returns the live (un-hashed)
// root node plus the trie's reference root hash.
func buildLiveTrie(t *testing.T, kv map[common.Hash][]byte) (node, common.Hash) {
	t.Helper()
	ref := NewEmpty(nil)
	live := NewEmpty(nil)
	for k, v := range kv {
		ref.MustUpdate(k.Bytes(), v)
		live.MustUpdate(k.Bytes(), v)
	}
	return live.root, ref.Hash()
}

func TestEncodeArchiveRecordsRoundTrip(t *testing.T) {
	mkKey := func(seed ...byte) common.Hash {
		return sha256.Sum256(seed)
	}
	cases := []struct {
		name string
		kv   map[common.Hash][]byte
	}{
		{
			name: "single leaf",
			kv:   map[common.Hash][]byte{mkKey(1): []byte("alpha")},
		},
		{
			name: "branching set",
			kv: map[common.Hash][]byte{
				mkKey(1):       []byte("one"),
				mkKey(2):       []byte("two"),
				mkKey(3):       bytes.Repeat([]byte{0xab}, 80), // >32B value
				mkKey(4):       []byte("four"),
				mkKey(5, 5, 5): []byte("five"),
				mkKey(9, 9):    []byte("nine"),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root, want := buildLiveTrie(t, tc.kv)

			recs, got, err := EncodeArchiveRecords(root)
			if err != nil {
				t.Fatalf("EncodeArchiveRecords: %v", err)
			}
			if got != want {
				t.Fatalf("encoded root hash mismatch: got %x want %x", got, want)
			}
			if len(recs) != len(tc.kv) {
				t.Fatalf("record count = %d, want %d leaves", len(recs), len(tc.kv))
			}

			// Reconstruction from records alone must reproduce the same hash.
			rebuilt, err := archiveRecordsToNode(recs)
			if err != nil {
				t.Fatalf("archiveRecordsToNode: %v", err)
			}
			h := newHasher(false)
			rebuiltHash := common.BytesToHash(h.hash(rebuilt, true))
			returnHasherToPool(h)
			if rebuiltHash != want {
				t.Fatalf("reconstructed root hash mismatch: got %x want %x", rebuiltHash, want)
			}

			// Every record value must be retrievable at its key from the rebuilt trie.
			tr := NewEmpty(nil)
			tr.root = rebuilt
			for k, v := range tc.kv {
				if got := tr.MustGet(k.Bytes()); !bytes.Equal(got, v) {
					t.Fatalf("key %x: got value %x want %x", k, got, v)
				}
			}
		})
	}
}

func TestEncodeExpiredNodeBlobLayout(t *testing.T) {
	blob := EncodeExpiredNodeBlob(0x0102030405060708, 0x1112131415161718)
	want := []byte{
		0x00,
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
	}
	if !bytes.Equal(blob, want) {
		t.Fatalf("expired node blob = %x, want %x", blob, want)
	}
}

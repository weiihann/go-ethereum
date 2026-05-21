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

package state

import (
	"bytes"
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/trie/bintrie"
	"github.com/holiman/uint256"
)

// TestStemBlobRoundTrip exercises serializeStemBlob / lookupSuffix with a
// few hand-picked suffix distributions to verify the bitmap-and-values
// encoding matches state-actor's serializeStemBlob output.
func TestStemBlobRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name    string
		entries map[byte][]byte
	}{
		{
			name: "single low suffix",
			entries: map[byte][]byte{
				0: bytesPattern(0xAA),
			},
		},
		{
			name: "single high suffix",
			entries: map[byte][]byte{
				255: bytesPattern(0xCD),
			},
		},
		{
			name: "two adjacent suffixes (account fields)",
			entries: map[byte][]byte{
				bintrie.BasicDataLeafKey: bytesPattern(0x01),
				bintrie.CodeHashLeafKey:  bytesPattern(0x02),
			},
		},
		{
			name: "sparse suffixes across bytes",
			entries: map[byte][]byte{
				3:   bytesPattern(0x10),
				64:  bytesPattern(0x20),
				127: bytesPattern(0x30),
				200: bytesPattern(0x40),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blob := serializeStemBlob(tc.entries)
			for suffix, want := range tc.entries {
				got, ok := lookupSuffix(blob, suffix)
				if !ok {
					t.Fatalf("suffix %d: expected present, got absent", suffix)
				}
				if !bytes.Equal(got, want) {
					t.Errorf("suffix %d: got %x, want %x", suffix, got, want)
				}
			}
			// Suffixes not in the entry set must report absent.
			for s := 0; s < 256; s++ {
				if _, present := tc.entries[byte(s)]; present {
					continue
				}
				if _, ok := lookupSuffix(blob, uint8(s)); ok {
					t.Errorf("suffix %d: expected absent, got present", s)
				}
			}
		})
	}
}

// TestMergeStemBlob exercises the read-modify-write merge path: existing
// suffix values must be preserved when other suffixes are overwritten.
func TestMergeStemBlob(t *testing.T) {
	existing := serializeStemBlob(map[byte][]byte{
		0:   bytesPattern(0xAA),
		1:   bytesPattern(0xBB),
		200: bytesPattern(0xCC),
	})
	mods := map[byte][]byte{
		1:   bytesPattern(0x11), // overwrite
		100: bytesPattern(0x99), // add
	}
	merged := mergeStemBlob(existing, mods)

	want := map[byte][]byte{
		0:   bytesPattern(0xAA),
		1:   bytesPattern(0x11),
		100: bytesPattern(0x99),
		200: bytesPattern(0xCC),
	}
	for suffix, exp := range want {
		got, ok := lookupSuffix(merged, suffix)
		if !ok {
			t.Fatalf("suffix %d: expected present, got absent", suffix)
		}
		if !bytes.Equal(got, exp) {
			t.Errorf("suffix %d: got %x, want %x", suffix, got, exp)
		}
	}
}

// TestUBTFlatReaderHit covers the happy path: a stem blob with both account
// suffixes populated decodes into a StateAccount with the correct fields.
func TestUBTFlatReaderHit(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()

	addr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	codeHash := crypto.Keccak256Hash([]byte("hello"))
	acct := &types.StateAccount{
		Nonce:    42,
		Balance:  uint256.NewInt(1_000_000_000),
		CodeHash: codeHash.Bytes(),
		Root:     types.EmptyRootHash,
	}
	basicData := encodeBasicData(acct, 99)
	ch := make([]byte, 32)
	copy(ch, codeHash[:])

	stem := bintrie.GetBinaryTreeStemAccount(addr)
	blob := serializeStemBlob(map[byte][]byte{
		bintrie.BasicDataLeafKey: basicData[:],
		bintrie.CodeHashLeafKey:  ch,
	})
	rawdb.WriteUBTFlatStem(db, stem, blob)

	r := newUBTFlatReader(db)
	got, err := r.Account(addr)
	if err != nil {
		t.Fatalf("Account: %v", err)
	}
	if got == nil {
		t.Fatal("Account: got nil, want decoded account")
	}
	if got.Nonce != acct.Nonce {
		t.Errorf("nonce: got %d, want %d", got.Nonce, acct.Nonce)
	}
	if got.Balance.Cmp(acct.Balance) != 0 {
		t.Errorf("balance: got %s, want %s", got.Balance, acct.Balance)
	}
	if !bytes.Equal(got.CodeHash, codeHash[:]) {
		t.Errorf("codeHash: got %x, want %x", got.CodeHash, codeHash[:])
	}
}

// TestUBTFlatReaderMiss verifies the sentinel-error path: a missing stem and
// a stem with the basic-data suffix bit unset must both return
// errStemNotInFlatState so multiStateReader falls through to the trie.
func TestUBTFlatReaderMiss(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()

	r := newUBTFlatReader(db)

	// 1. Stem blob doesn't exist on disk.
	addr := common.HexToAddress("0x2222222222222222222222222222222222222222")
	if _, err := r.Account(addr); !errors.Is(err, errStemNotInFlatState) {
		t.Errorf("missing stem: got err %v, want errStemNotInFlatState", err)
	}

	// 2. Stem blob exists but the basic-data bitmap bit is unset (only an
	//    unrelated suffix is populated).
	stem := bintrie.GetBinaryTreeStemAccount(addr)
	blob := serializeStemBlob(map[byte][]byte{
		42: bytesPattern(0xDE),
	})
	rawdb.WriteUBTFlatStem(db, stem, blob)
	if _, err := r.Account(addr); !errors.Is(err, errStemNotInFlatState) {
		t.Errorf("partial blob: got err %v, want errStemNotInFlatState", err)
	}
}

// TestWriteUBTFlatStateRoundTrip exercises the writer + reader together: a
// crafted StateUpdate is committed via writeUBTFlatState, then read back
// via ubtFlatReader and checked for correctness.
func TestWriteUBTFlatStateRoundTrip(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	codedb := NewCodeDB(db)

	addr := common.HexToAddress("0x3333333333333333333333333333333333333333")
	addrHash := crypto.Keccak256Hash(addr[:])

	acct := &types.StateAccount{
		Nonce:    7,
		Balance:  uint256.NewInt(5000),
		CodeHash: types.EmptyCodeHash.Bytes(),
		Root:     types.EmptyRootHash,
	}
	var slotKey, slotVal common.Hash
	slotKey[0] = 0x11
	slotVal[0] = 0x22
	slotHash := crypto.Keccak256Hash(slotKey[:])

	update := &StateUpdate{
		StorageKeyType: StorageKeyPlain,
		Accounts: map[common.Hash]*types.StateAccount{
			addrHash: acct,
		},
		AccountsOrigin: map[common.Address]*types.StateAccount{
			addr: nil, // newly created
		},
		Storages: map[common.Hash]map[common.Hash]common.Hash{
			addrHash: {slotHash: slotVal},
		},
		StoragesOrigin: map[common.Address]map[common.Hash]common.Hash{
			addr: {slotKey: {}}, // origin value zero (newly created slot)
		},
	}

	if err := writeUBTFlatState(db, codedb, update); err != nil {
		t.Fatalf("writeUBTFlatState: %v", err)
	}

	r := newUBTFlatReader(db)
	gotAcct, err := r.Account(addr)
	if err != nil {
		t.Fatalf("Account: %v", err)
	}
	if gotAcct == nil {
		t.Fatal("Account: nil; expected freshly written account")
	}
	if gotAcct.Nonce != 7 || gotAcct.Balance.Uint64() != 5000 {
		t.Errorf("account fields: got nonce=%d balance=%s, want 7/5000", gotAcct.Nonce, gotAcct.Balance)
	}

	gotSlot, err := r.Storage(addr, slotKey)
	if err != nil {
		t.Fatalf("Storage: %v", err)
	}
	if gotSlot != slotVal {
		t.Errorf("slot value: got %s, want %s", gotSlot.Hex(), slotVal.Hex())
	}
}

// bytesPattern produces a 32-byte slice filled with the given byte value.
// Used to construct distinguishable test values per suffix.
func bytesPattern(b byte) []byte {
	out := make([]byte, 32)
	for i := range out {
		out[i] = b
	}
	return out
}

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

package bintrie

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
)

var testAddr = common.HexToAddress("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")

// TestZone000Prefix verifies that account header keys (zone 000) have
// the correct 3-bit prefix: the top 3 bits of the first byte are 000.
func TestZone000Prefix(t *testing.T) {
	keys := [][]byte{
		GetBinaryTreeKeyBasicData(testAddr),
		GetBinaryTreeKeyCodeHash(testAddr),
	}
	// Add a header storage slot key (slot 0).
	keys = append(keys, GetBinaryTreeKeyStorageSlot(testAddr, common.Hash{}.Bytes()))
	// Add a header code chunk key (chunk 0).
	keys = append(keys, GetBinaryTreeKeyCodeChunk(testAddr, common.Hash{}, uint256.NewInt(0)))

	for i, key := range keys {
		if len(key) != 32 {
			t.Fatalf("key %d: expected 32 bytes, got %d", i, len(key))
		}
		// Top 3 bits must be 000 → first byte AND 0xE0 == 0x00.
		if key[0]&0xE0 != 0x00 {
			t.Fatalf("key %d: zone 000 prefix violated, first byte = 0x%02x", i, key[0])
		}
	}
}

// TestZone001Prefix verifies that code overflow keys (zone 001) have
// the correct 3-bit prefix: bits [255..253] = 001.
func TestZone001Prefix(t *testing.T) {
	codeHash := common.HexToHash("c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470")

	// Chunk 128 is the first chunk in zone 001.
	key := GetBinaryTreeKeyCodeChunk(testAddr, codeHash, uint256.NewInt(128))
	if len(key) != 32 {
		t.Fatalf("expected 32 bytes, got %d", len(key))
	}
	// Top 3 bits must be 001 → first byte AND 0xE0 == 0x20.
	if key[0]&0xE0 != 0x20 {
		t.Fatalf("zone 001 prefix violated, first byte = 0x%02x", key[0])
	}
}

// TestZone1Prefix verifies that storage keys (zone 1) for slots >= 64
// have the correct 1-bit prefix: bit 255 = 1.
func TestZone1Prefix(t *testing.T) {
	// Slot 64 is the first slot in zone 1.
	slot := common.Hash{}
	slot[31] = 64
	key := GetBinaryTreeKeyStorageSlot(testAddr, slot[:])
	if len(key) != 32 {
		t.Fatalf("expected 32 bytes, got %d", len(key))
	}
	// Top bit must be 1 → first byte AND 0x80 == 0x80.
	if key[0]&0x80 != 0x80 {
		t.Fatalf("zone 1 prefix violated, first byte = 0x%02x", key[0])
	}
}

// TestStorageBoundary verifies that slot 63 goes to zone 000 and slot 64
// goes to zone 1.
func TestStorageBoundary(t *testing.T) {
	// Slot 63: zone 000, sub_idx = 0x40 + 63 = 0x7F.
	slot63 := common.Hash{}
	slot63[31] = 63
	key63 := GetBinaryTreeKeyStorageSlot(testAddr, slot63[:])
	if key63[0]&0xE0 != 0x00 {
		t.Fatalf("slot 63: expected zone 000, first byte = 0x%02x", key63[0])
	}
	if key63[31] != 0x7F {
		t.Fatalf("slot 63: expected sub_idx 0x7F, got 0x%02x", key63[31])
	}

	// Slot 64: zone 1, sub_idx = 64 % 256 = 64.
	slot64 := common.Hash{}
	slot64[31] = 64
	key64 := GetBinaryTreeKeyStorageSlot(testAddr, slot64[:])
	if key64[0]&0x80 != 0x80 {
		t.Fatalf("slot 64: expected zone 1, first byte = 0x%02x", key64[0])
	}
	if key64[31] != 64 {
		t.Fatalf("slot 64: expected sub_idx 64, got %d", key64[31])
	}
}

// TestCodeChunkBoundary verifies that chunk 127 goes to zone 000 and
// chunk 128 goes to zone 001.
func TestCodeChunkBoundary(t *testing.T) {
	codeHash := common.HexToHash("c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470")

	// Chunk 127: zone 000, sub_idx = 0x80 + 127 = 0xFF.
	key127 := GetBinaryTreeKeyCodeChunk(testAddr, codeHash, uint256.NewInt(127))
	if key127[0]&0xE0 != 0x00 {
		t.Fatalf("chunk 127: expected zone 000, first byte = 0x%02x", key127[0])
	}
	if key127[31] != 0xFF {
		t.Fatalf("chunk 127: expected sub_idx 0xFF, got 0x%02x", key127[31])
	}

	// Chunk 128: zone 001, tree_index=0, sub_idx=0.
	key128 := GetBinaryTreeKeyCodeChunk(testAddr, codeHash, uint256.NewInt(128))
	if key128[0]&0xE0 != 0x20 {
		t.Fatalf("chunk 128: expected zone 001, first byte = 0x%02x", key128[0])
	}
	if key128[31] != 0x00 {
		t.Fatalf("chunk 128: expected sub_idx 0x00, got 0x%02x", key128[31])
	}
}

// TestStemSharing verifies that keys differing only in sub_idx share
// the same 31-byte stem.
func TestStemSharing(t *testing.T) {
	// BasicData (sub_idx=0) and CodeHash (sub_idx=1) share the same stem.
	basicKey := GetBinaryTreeKeyBasicData(testAddr)
	codeHashKey := GetBinaryTreeKeyCodeHash(testAddr)
	if !bytes.Equal(basicKey[:StemSize], codeHashKey[:StemSize]) {
		t.Fatal("BasicData and CodeHash do not share the same stem")
	}

	// Header storage slot 0 (sub_idx=0x40) shares the account stem too.
	slot0Key := GetBinaryTreeKeyStorageSlot(testAddr, common.Hash{}.Bytes())
	if !bytes.Equal(basicKey[:StemSize], slot0Key[:StemSize]) {
		t.Fatal("BasicData and storage slot 0 do not share the same stem")
	}

	// Header code chunk 0 (sub_idx=0x80) shares the account stem.
	chunk0Key := GetBinaryTreeKeyCodeChunk(testAddr, common.Hash{}, uint256.NewInt(0))
	if !bytes.Equal(basicKey[:StemSize], chunk0Key[:StemSize]) {
		t.Fatal("BasicData and code chunk 0 do not share the same stem")
	}
}

// TestStorageStemGrouping verifies that adjacent storage slots share stems.
// Slots 256-511 should share the same stem (tree_index = 1).
func TestStorageStemGrouping(t *testing.T) {
	slot256 := common.Hash{}
	slot256[30] = 1 // big-endian 256
	key256 := GetBinaryTreeKeyStorageSlot(testAddr, slot256[:])

	slot511 := common.Hash{}
	slot511[30] = 1
	slot511[31] = 255 // big-endian 511
	key511 := GetBinaryTreeKeyStorageSlot(testAddr, slot511[:])

	if !bytes.Equal(key256[:StemSize], key511[:StemSize]) {
		t.Fatal("slots 256 and 511 should share the same stem")
	}

	// Slot 512 should have a different stem (tree_index = 2).
	slot512 := common.Hash{}
	slot512[30] = 2 // big-endian 512
	key512 := GetBinaryTreeKeyStorageSlot(testAddr, slot512[:])

	if bytes.Equal(key256[:StemSize], key512[:StemSize]) {
		t.Fatal("slots 256 and 512 should have different stems")
	}
}

// TestStorageSubIndex verifies the sub_idx calculation for storage.
func TestStorageSubIndex(t *testing.T) {
	// Slot 300: tree_index = 300/256 = 1, sub_idx = 300%256 = 44.
	slot := common.Hash{}
	slot[30] = 1 // big-endian: 256 + 44 = 300
	slot[31] = 44
	key := GetBinaryTreeKeyStorageSlot(testAddr, slot[:])
	if key[31] != 44 {
		t.Fatalf("slot 300: expected sub_idx 44, got %d", key[31])
	}
}

// TestCodeChunkContentAddressed verifies that zone 001 code chunks are
// keyed by code_hash, not by address. Two different addresses with the
// same code_hash should produce the same key for the same chunk.
func TestCodeChunkContentAddressed(t *testing.T) {
	addr1 := common.HexToAddress("0x1111111111111111111111111111111111111111")
	addr2 := common.HexToAddress("0x2222222222222222222222222222222222222222")
	codeHash := common.HexToHash("abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789")

	// Chunk 200 is in zone 001 (>= 128).
	key1 := GetBinaryTreeKeyCodeChunk(addr1, codeHash, uint256.NewInt(200))
	key2 := GetBinaryTreeKeyCodeChunk(addr2, codeHash, uint256.NewInt(200))

	if !bytes.Equal(key1, key2) {
		t.Fatal("zone 001 chunks with same code_hash should produce identical keys")
	}
}

// TestCodeChunkPerAccount verifies that zone 000 code chunks (0-127) are
// per-account, not content-addressed.
func TestCodeChunkPerAccount(t *testing.T) {
	addr1 := common.HexToAddress("0x1111111111111111111111111111111111111111")
	addr2 := common.HexToAddress("0x2222222222222222222222222222222222222222")
	codeHash := common.HexToHash("abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789")

	// Chunk 50 is in zone 000 (< 128), keyed by address.
	key1 := GetBinaryTreeKeyCodeChunk(addr1, codeHash, uint256.NewInt(50))
	key2 := GetBinaryTreeKeyCodeChunk(addr2, codeHash, uint256.NewInt(50))

	if bytes.Equal(key1, key2) {
		t.Fatal("zone 000 chunks should differ by address")
	}
}

// TestStorageIndexHeaderSlots verifies StorageIndex for header slots (0-63).
func TestStorageIndexHeaderSlots(t *testing.T) {
	for slot := byte(0); slot < 64; slot++ {
		key := common.Hash{}
		key[31] = slot
		treeIdx, subIdx := StorageIndex(key[:])
		if treeIdx.Sign() != 0 {
			t.Fatalf("slot %d: expected treeIndex=0, got %v", slot, treeIdx)
		}
		expected := byte(HeaderStorageStart + slot)
		if subIdx != expected {
			t.Fatalf("slot %d: expected subIdx=0x%02x, got 0x%02x", slot, expected, subIdx)
		}
	}
}

// TestStorageIndexMainSlots verifies StorageIndex for main storage (>= 64).
func TestStorageIndexMainSlots(t *testing.T) {
	tests := []struct {
		slot            uint64
		expectedTreeIdx uint64
		expectedSubIdx  byte
	}{
		{64, 0, 64},
		{255, 0, 255},
		{256, 1, 0},
		{300, 1, 44},
		{512, 2, 0},
	}
	for _, tt := range tests {
		var key [32]byte
		new(uint256.Int).SetUint64(tt.slot).WriteToSlice(key[:])
		treeIdx, subIdx := StorageIndex(key[:])

		if !treeIdx.Eq(uint256.NewInt(tt.expectedTreeIdx)) {
			t.Fatalf("slot %d: expected treeIndex=%d, got %v",
				tt.slot, tt.expectedTreeIdx, treeIdx)
		}
		if subIdx != tt.expectedSubIdx {
			t.Fatalf("slot %d: expected subIdx=%d, got %d",
				tt.slot, tt.expectedSubIdx, subIdx)
		}
	}
}

// TestDifferentAddressesDifferentStorageKeys verifies that two different
// addresses produce different storage keys even with the same slot.
func TestDifferentAddressesDifferentStorageKeys(t *testing.T) {
	addr1 := common.HexToAddress("0x1111111111111111111111111111111111111111")
	addr2 := common.HexToAddress("0x2222222222222222222222222222222222222222")

	slot := common.Hash{}
	slot[31] = 100 // slot 100, in zone 1

	key1 := GetBinaryTreeKeyStorageSlot(addr1, slot[:])
	key2 := GetBinaryTreeKeyStorageSlot(addr2, slot[:])

	if bytes.Equal(key1, key2) {
		t.Fatal("different addresses should produce different storage keys")
	}
	// Both should be in zone 1.
	if key1[0]&0x80 != 0x80 || key2[0]&0x80 != 0x80 {
		t.Fatal("both keys should have zone 1 prefix")
	}
}

// TestGetBinaryTreeStemAccount returns the same stem as BasicData key.
func TestGetBinaryTreeStemAccount(t *testing.T) {
	stem := GetBinaryTreeStemAccount(testAddr)
	basicKey := GetBinaryTreeKeyBasicData(testAddr)

	if len(stem) != StemSize {
		t.Fatalf("expected %d bytes, got %d", StemSize, len(stem))
	}
	if !bytes.Equal(stem, basicKey[:StemSize]) {
		t.Fatal("stem does not match BasicData key prefix")
	}
}

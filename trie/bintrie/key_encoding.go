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
	"encoding/binary"

	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
)

// Leaf key indices for account header data in the account zone.
const (
	BasicDataLeafKey        = 0
	CodeHashLeafKey         = 1
	BasicDataCodeSizeOffset = 5
	BasicDataNonceOffset    = 8
	BasicDataBalanceOffset  = 16
)

// PBT zone identifiers. The high 16 bits of every non-storage stem hold the
// zone. The storage zone is any key with the high bit set and is handled
// separately by buildKeyStorageZone.
const (
	ZoneAccount uint16 = 0x0000 // account headers
	ZoneCode    uint16 = 0x0001 // content-addressed code overflow
)

// Bit widths for the storage-zone key segments. All zones produce 256-bit keys.
const (
	addrPrefixBits = 60  // H(addr) prefix bits in the storage zone
	stemSuffixBits = 187 // H(addr||tree_index) suffix bits in the storage zone
	subIndexBits   = 8   // sub-index width in all zones
)

// Sub-index offsets for account header stem (zone 000, EIP-7864 layout).
const (
	HeaderStorageStart = 0x40 // sub_idx for storage slot 0
	HeaderCodeStart    = 0x80 // sub_idx for code chunk 0
	HeaderCodeChunks   = 128  // chunks 0-127 in account header
	HeaderStorageSlots = 64   // slots 0-63 in account header
)

var (
	zeroTreeIndex        uint256.Int
	headerStorageMaxSlot = uint256.NewInt(HeaderStorageSlots)
	headerCodeChunkCount = uint256.NewInt(HeaderCodeChunks)
)

// hashAddr returns SHA256(addr). Plain 20-byte input per PBT spec.
func hashAddr(addr common.Address) [32]byte {
	hasher := newSha256()
	defer returnSha256(hasher)
	hasher.Write(addr[:])
	var out [32]byte
	copy(out[:], hasher.Sum(nil))
	return out
}

// hashConcat returns SHA256(a || b) where b is a uint256 encoded as
// 32-byte big-endian. Used for H(addr || tree_index) and
// H(code_hash || tree_index).
func hashConcat(a []byte, b *uint256.Int) [32]byte {
	hasher := newSha256()
	defer returnSha256(hasher)
	hasher.Write(a)
	buf := b.Bytes32()
	hasher.Write(buf[:])
	var out [32]byte
	copy(out[:], hasher.Sum(nil))
	return out
}

// buildKeyZone constructs a 256-bit key for a non-storage zone (account or
// code). The 16-bit zone makes the layout byte-aligned:
//
//	[2-byte zone | 29-byte hash (top 232 bits) | 1-byte sub_idx] = 32 bytes
func buildKeyZone(zone uint16, hash [32]byte, subIdx byte) [32]byte {
	var key [32]byte
	binary.BigEndian.PutUint16(key[0:2], zone)
	copy(key[2:StemSize], hash[:StemSize-2]) // top 232 bits = first 29 bytes
	key[StemSize] = subIdx
	return key
}

// buildKeyStorageZone constructs a 256-bit key for zone 1 (storage).
//
// Layout (bit 255 = MSB, bit 0 = LSB):
//
//	[1 zone bit | 60 addr_prefix bits | 187 stem_suffix bits | 8 sub_idx bits] = 256 bits
func buildKeyStorageZone(addrHash, stemHash [32]byte, subIdx byte) [32]byte {
	var result uint256.Int

	// Zone bit "1" at bit 255.
	result.SetUint64(1)
	result.Lsh(&result, 255)

	// Top 60 bits of H(addr) at [254..195].
	var addrBits uint256.Int
	addrBits.SetBytes(addrHash[:])
	addrBits.Rsh(&addrBits, 256-addrPrefixBits)          // 196: keep top 60 bits
	addrBits.Lsh(&addrBits, stemSuffixBits+subIndexBits) // 195: place at [254..195]
	result.Or(&result, &addrBits)

	// Top 187 bits of H(addr || tree_index) at [194..8].
	var stemBits uint256.Int
	stemBits.SetBytes(stemHash[:])
	stemBits.Rsh(&stemBits, 256-stemSuffixBits) // 69: keep top 187 bits
	stemBits.Lsh(&stemBits, subIndexBits)       // 8: place at [194..8]
	result.Or(&result, &stemBits)

	// Sub_idx at [7..0].
	var sub uint256.Int
	sub.SetUint64(uint64(subIdx))
	result.Or(&result, &sub)

	return result.Bytes32()
}

// GetBinaryTreeKeyBasicData returns the 256-bit key for an account's
// basic data leaf (nonce, balance, code_size) in the account zone.
func GetBinaryTreeKeyBasicData(addr common.Address) []byte {
	key := buildKeyZone(ZoneAccount, hashAddr(addr), BasicDataLeafKey)
	return key[:]
}

// GetBinaryTreeKeyCodeHash returns the 256-bit key for an account's
// code hash leaf in the account zone.
func GetBinaryTreeKeyCodeHash(addr common.Address) []byte {
	key := buildKeyZone(ZoneAccount, hashAddr(addr), CodeHashLeafKey)
	return key[:]
}

// GetBinaryTreeStemAccount returns the 31-byte stem for an account's
// header in the account zone. All leaves in the account header (basic data,
// code hash, hot storage, initial code) share this stem prefix.
func GetBinaryTreeStemAccount(addr common.Address) []byte {
	key := GetBinaryTreeKeyBasicData(addr)
	return key[:StemSize]
}

// GetBinaryTreeKeyStorageSlot returns the 256-bit key for a storage slot.
// Slots 0-63 are in the account header (account zone, sub_idx 0x40-0x7F).
// Slots >= 64 are in the storage zone with a 60-bit address prefix and 187-bit
// stem suffix.
func GetBinaryTreeKeyStorageSlot(addr common.Address, slotKey []byte) []byte {
	var slot uint256.Int
	slot.SetBytes(slotKey)

	// Slots 0-63: account zone header.
	if slot.Cmp(headerStorageMaxSlot) < 0 {
		subIdx := byte(HeaderStorageStart + slot[0])
		key := buildKeyZone(ZoneAccount, hashAddr(addr), subIdx)
		return key[:]
	}

	// Slots >= 64: storage zone.
	addrHash := hashAddr(addr)

	// tree_index = slot / 256, sub_idx = slot % 256
	var treeIndex uint256.Int
	treeIndex.Rsh(&slot, subIndexBits)
	subIdx := byte(slot[0] & 0xFF)

	stemHash := hashConcat(addr[:], &treeIndex)
	key := buildKeyStorageZone(addrHash, stemHash, subIdx)
	return key[:]
}

// GetBinaryTreeKeyCodeChunk returns the 256-bit key for a code chunk.
// Chunks 0-127 are in the account header (account zone, sub_idx 0x80-0xFF).
// Chunks >= 128 are content-addressed in the code zone using the code hash.
func GetBinaryTreeKeyCodeChunk(
	addr common.Address,
	codeHash common.Hash,
	chunknr *uint256.Int,
) []byte {
	// Chunks 0-127: account zone header.
	if chunknr.Cmp(headerCodeChunkCount) < 0 {
		subIdx := byte(HeaderCodeStart + chunknr[0])
		key := buildKeyZone(ZoneAccount, hashAddr(addr), subIdx)
		return key[:]
	}

	// Chunks >= 128: code zone, content-addressed by code_hash.
	var adjusted uint256.Int
	adjusted.Sub(chunknr, headerCodeChunkCount)

	// tree_index = (chunk_id - 128) / 256
	var treeIndex uint256.Int
	treeIndex.Rsh(&adjusted, subIndexBits)

	// sub_idx = (chunk_id - 128) % 256
	subIdx := byte(adjusted[0] & 0xFF)

	h := hashConcat(codeHash[:], &treeIndex)
	key := buildKeyZone(ZoneCode, h, subIdx)
	return key[:]
}

// StorageIndex returns the tree index and sub-index for a storage key,
// used by the gas accounting system to track accessed branches and chunks.
// Slots 0-63 return treeIndex=0 with sub_idx in the header range.
// Slots >= 64 return treeIndex = slot/256, sub_idx = slot%256.
func StorageIndex(storageKey []byte) (*uint256.Int, byte) {
	var slot uint256.Int
	slot.SetBytes(storageKey)

	// Slots 0-63: account header, treeIndex = 0.
	if slot.Cmp(headerStorageMaxSlot) < 0 {
		return &zeroTreeIndex, byte(HeaderStorageStart + slot[0])
	}

	// Slots >= 64: zone 1 storage.
	subIdx := byte(slot[0] & 0xFF)
	var treeIndex uint256.Int
	treeIndex.Rsh(&slot, subIndexBits)
	return &treeIndex, subIdx
}

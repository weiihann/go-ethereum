// Package nomttrie implements a state.Trie backed by the NOMT binary merkle
// trie engine, targeting EIP-7864 compatibility.
//
// Key derivation functions delegate to trie/bintrie to guarantee identical
// key generation. This file provides stem-aware wrappers that split keys
// into the 31-byte stem path and 1-byte suffix used by the NOMT page tree.
package nomttrie

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/nomt/core"
	"github.com/ethereum/go-ethereum/trie/bintrie"
)

// stemAndSuffix splits a 32-byte EIP-7864 trie key into its 31-byte stem
// and 1-byte suffix. The stem identifies the stem node, and the suffix
// identifies the value slot (0-255) within that stem.
func stemAndSuffix(key []byte) (core.StemPath, byte) {
	var stem core.StemPath
	copy(stem[:], key[:core.StemSize])
	return stem, key[core.StemSize]
}

// accountStem returns the stem path for an account's EIP-7864 key.
// All account-level values (basic data at suffix 0, code hash at suffix 1)
// share the same stem.
func accountStem(addr common.Address) core.StemPath {
	stem, _ := stemAndSuffix(bintrie.GetBinaryTreeKeyBasicData(addr))
	return stem
}

// storageStemAndSuffix returns the stem path and suffix for a storage slot.
func storageStemAndSuffix(addr common.Address, storageKey []byte) (core.StemPath, byte) {
	return stemAndSuffix(bintrie.GetBinaryTreeKeyStorageSlot(addr, storageKey))
}

// codeChunkStemAndSuffix returns the stem path and suffix for a code chunk.
//
// This matches bintrie.UpdateContractCode's key construction (not the buggy
// GetBinaryTreeKeyCodeChunk which passes a variable-length uint256.Bytes()
// to GetBinaryTreeKey).
func codeChunkStemAndSuffix(addr common.Address, chunkNr uint64) (core.StemPath, byte) {
	var offset [bintrie.HashSize]byte
	binary.LittleEndian.PutUint64(offset[24:], chunkNr+128)
	return stemAndSuffix(bintrie.GetBinaryTreeKey(addr, offset[:]))
}

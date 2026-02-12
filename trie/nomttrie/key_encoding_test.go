package nomttrie

import (
	"encoding/binary"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/nomt/core"
	"github.com/ethereum/go-ethereum/trie/bintrie"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStemAndSuffix(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	stem, suffix := stemAndSuffix(key)

	// Stem should be first 31 bytes.
	for i := range core.StemSize {
		assert.Equal(t, byte(i), stem[i])
	}
	// Suffix should be byte 31.
	assert.Equal(t, byte(31), suffix)
}

func TestAccountStemSharedBetweenBasicDataAndCodeHash(t *testing.T) {
	addr := common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678")

	basicDataKey := bintrie.GetBinaryTreeKeyBasicData(addr)
	codeHashKey := bintrie.GetBinaryTreeKeyCodeHash(addr)

	// First 31 bytes (stem) should be identical.
	assert.Equal(t, basicDataKey[:core.StemSize], codeHashKey[:core.StemSize])

	// Suffixes should differ: 0 for basic data, 1 for code hash.
	assert.Equal(t, byte(bintrie.BasicDataLeafKey), basicDataKey[core.StemSize])
	assert.Equal(t, byte(bintrie.CodeHashLeafKey), codeHashKey[core.StemSize])
}

func TestAccountStemMatchesBintrie(t *testing.T) {
	addr := common.HexToAddress("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")

	stem := accountStem(addr)
	bintrieKey := bintrie.GetBinaryTreeKeyBasicData(addr)

	var expectedStem core.StemPath
	copy(expectedStem[:], bintrieKey[:core.StemSize])
	assert.Equal(t, expectedStem, stem)
}

func TestStorageStemAndSuffix(t *testing.T) {
	addr := common.HexToAddress("0xaaaa")
	slot := common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000001")

	stem, suffix := storageStemAndSuffix(addr, slot)
	bintrieKey := bintrie.GetBinaryTreeKeyStorageSlot(addr, slot)

	var expectedStem core.StemPath
	copy(expectedStem[:], bintrieKey[:core.StemSize])
	assert.Equal(t, expectedStem, stem)
	assert.Equal(t, bintrieKey[core.StemSize], suffix)
}

func TestStorageHeaderSlotMapsToAccountStem(t *testing.T) {
	// Storage keys < 64 live in the account header (same stem as basic data).
	addr := common.HexToAddress("0xbbbb")

	// Slot 0 is in the header.
	headerSlot := make([]byte, 32)
	headerSlot[31] = 2 // slot 2 (< 64)

	stem, suffix := storageStemAndSuffix(addr, headerSlot)
	accountSt := accountStem(addr)

	assert.Equal(t, accountSt, stem,
		"header storage slot should share stem with account")
	assert.Equal(t, byte(64+2), suffix,
		"header slot suffix = 64 + slot number")
}

func TestCodeChunkStemAndSuffix(t *testing.T) {
	addr := common.HexToAddress("0xcccc")

	stem, suffix := codeChunkStemAndSuffix(addr, 5)

	// Reproduce the key construction from bintrie.UpdateContractCode.
	var offset [bintrie.HashSize]byte
	binary.LittleEndian.PutUint64(offset[24:], 5+128)
	expectedKey := bintrie.GetBinaryTreeKey(addr, offset[:])

	var expectedStem core.StemPath
	copy(expectedStem[:], expectedKey[:core.StemSize])
	assert.Equal(t, expectedStem, stem)
	assert.Equal(t, expectedKey[core.StemSize], suffix)
}

func TestDifferentAddressesProduceDifferentStems(t *testing.T) {
	addr1 := common.HexToAddress("0x1111111111111111111111111111111111111111")
	addr2 := common.HexToAddress("0x2222222222222222222222222222222222222222")

	stem1 := accountStem(addr1)
	stem2 := accountStem(addr2)
	require.NotEqual(t, stem1, stem2)
}

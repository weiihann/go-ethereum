package nomttrie

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/trie/bintrie"
)

// packBasicData encodes account metadata into a 32-byte value matching the
// EIP-7864 basic data layout used by bintrie.UpdateAccount:
//
//	[4:8]   code size (uint32 big-endian)
//	[8:16]  nonce (uint64 big-endian)
//	[16:32] balance (up to 16 bytes, right-aligned big-endian)
func packBasicData(acc *types.StateAccount, codeLen int) [bintrie.HashSize]byte {
	var data [bintrie.HashSize]byte

	binary.BigEndian.PutUint32(data[bintrie.BasicDataCodeSizeOffset-1:], uint32(codeLen))
	binary.BigEndian.PutUint64(data[bintrie.BasicDataNonceOffset:], acc.Nonce)

	// Truncate balance to 16 bytes (matching bintrie behavior for devmode
	// accounts that exceed 128-bit balance).
	balanceBytes := acc.Balance.Bytes()
	if len(balanceBytes) > 16 {
		balanceBytes = balanceBytes[16:]
	}
	copy(data[bintrie.HashSize-len(balanceBytes):], balanceBytes)

	return data
}

// packStorageValue encodes a storage value into a 32-byte slot matching
// bintrie.UpdateStorage: right-pad short values, truncate long values.
func packStorageValue(value []byte) [bintrie.HashSize]byte {
	var v [bintrie.HashSize]byte
	if len(value) >= bintrie.HashSize {
		copy(v[:], value[:bintrie.HashSize])
	} else {
		copy(v[bintrie.HashSize-len(value):], value)
	}
	return v
}

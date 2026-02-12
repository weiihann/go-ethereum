package nomttrie

import (
	"encoding/binary"
	"testing"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/trie/bintrie"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
)

func TestPackBasicDataNonce(t *testing.T) {
	acc := &types.StateAccount{
		Nonce:   42,
		Balance: uint256.NewInt(0),
	}

	data := packBasicData(acc, 0)

	got := binary.BigEndian.Uint64(data[bintrie.BasicDataNonceOffset:])
	assert.Equal(t, uint64(42), got)
}

func TestPackBasicDataCodeSize(t *testing.T) {
	acc := &types.StateAccount{
		Nonce:   0,
		Balance: uint256.NewInt(0),
	}

	data := packBasicData(acc, 1234)

	got := binary.BigEndian.Uint32(data[bintrie.BasicDataCodeSizeOffset-1:])
	assert.Equal(t, uint32(1234), got)
}

func TestPackBasicDataBalance(t *testing.T) {
	acc := &types.StateAccount{
		Nonce:   0,
		Balance: uint256.NewInt(1000),
	}

	data := packBasicData(acc, 0)

	// Balance is right-aligned in the last 16 bytes.
	// 1000 = 0x03E8, occupies 2 bytes.
	assert.Equal(t, byte(0x03), data[30])
	assert.Equal(t, byte(0xE8), data[31])
}

func TestPackBasicDataMatchesBintrie(t *testing.T) {
	// Match the exact encoding bintrie uses in UpdateAccount.
	acc := &types.StateAccount{
		Nonce:    7,
		Balance:  uint256.NewInt(999),
		CodeHash: types.EmptyCodeHash.Bytes(),
	}
	codeLen := 512

	data := packBasicData(acc, codeLen)

	// Manually reproduce bintrie encoding.
	var expected [bintrie.HashSize]byte
	binary.BigEndian.PutUint32(expected[bintrie.BasicDataCodeSizeOffset-1:], uint32(codeLen))
	binary.BigEndian.PutUint64(expected[bintrie.BasicDataNonceOffset:], acc.Nonce)
	balBytes := acc.Balance.Bytes()
	copy(expected[bintrie.HashSize-len(balBytes):], balBytes)

	assert.Equal(t, expected, data)
}

func TestPackStorageValue(t *testing.T) {
	tests := []struct {
		name     string
		value    []byte
		expected [32]byte
	}{
		{
			name:  "small value right-padded",
			value: []byte{0xFF},
			expected: func() [32]byte {
				var v [32]byte
				v[31] = 0xFF
				return v
			}(),
		},
		{
			name: "full 32 bytes",
			value: func() []byte {
				v := make([]byte, 32)
				for i := range v {
					v[i] = byte(i)
				}
				return v
			}(),
			expected: func() [32]byte {
				var v [32]byte
				for i := range v {
					v[i] = byte(i)
				}
				return v
			}(),
		},
		{
			name:     "empty value",
			value:    nil,
			expected: [32]byte{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := packStorageValue(tc.value)
			assert.Equal(t, tc.expected, got)
		})
	}
}

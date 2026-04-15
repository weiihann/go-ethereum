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

package types

import (
	"fmt"

	"github.com/ethereum/go-ethereum/rlp"
)

// storageSnapshotWrap is the EIP-8188 wrapper around a storage-slot snapshot
// value, pairing the trimmed value bytes with the period in which the slot
// was last written.
type storageSnapshotWrap struct {
	Value  []byte
	Period uint32
}

// EncodeStorageSnapshotValue produces the on-disk bytes for a storage-slot
// snapshot record. When period is zero, the output matches the pre-EIP-8188
// encoding byte-for-byte (a plain RLP byte string). When period is non-zero,
// the output is an RLP list [value, period], whose first byte lies in the
// list-prefix range 0xc0-0xff — a value that is never produced by the legacy
// encoding, so the two shapes are unambiguous on disk.
func EncodeStorageSnapshotValue(value []byte, period uint32) []byte {
	if period == 0 {
		encoded, err := rlp.EncodeToBytes(value)
		if err != nil {
			panic(err)
		}
		return encoded
	}
	encoded, err := rlp.EncodeToBytes(storageSnapshotWrap{Value: value, Period: period})
	if err != nil {
		panic(err)
	}
	return encoded
}

// DecodeStorageSnapshotValue accepts both the legacy byte-string encoding and
// the EIP-8188 list encoding. Legacy records decode with period=0; new records
// decode with the persisted period. Empty input (deleted slot) yields (nil, 0, nil).
func DecodeStorageSnapshotValue(blob []byte) ([]byte, uint32, error) {
	if len(blob) == 0 {
		return nil, 0, nil
	}
	if blob[0] >= 0xc0 {
		var wrap storageSnapshotWrap
		if err := rlp.DecodeBytes(blob, &wrap); err != nil {
			return nil, 0, fmt.Errorf("decode eip-8188 storage wrap: %w", err)
		}
		return wrap.Value, wrap.Period, nil
	}
	var value []byte
	if err := rlp.DecodeBytes(blob, &value); err != nil {
		return nil, 0, fmt.Errorf("decode legacy storage snapshot value: %w", err)
	}
	return value, 0, nil
}

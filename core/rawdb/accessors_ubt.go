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

package rawdb

import (
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

// ubtFlatStateKey returns the database key for the flat-state blob of the
// given UBT stem. Callers must pass a 31-byte stem; behaviour for other
// lengths is undefined.
func ubtFlatStateKey(stem []byte) []byte {
	buf := make([]byte, 0, len(UBTFlatStatePrefix)+len(stem))
	buf = append(buf, UBTFlatStatePrefix...)
	buf = append(buf, stem...)
	return buf
}

// ReadUBTFlatStem retrieves the flat-state blob for the given stem. The blob
// encodes [bitmap(32) || values...] where bitmap[i] indicates whether suffix
// slot i is present. Returns nil if the stem has no blob on disk.
func ReadUBTFlatStem(db ethdb.KeyValueReader, stem []byte) []byte {
	data, _ := db.Get(ubtFlatStateKey(stem))
	return data
}

// WriteUBTFlatStem stores the flat-state blob for the given stem, overwriting
// any prior blob at the same key.
func WriteUBTFlatStem(db ethdb.KeyValueWriter, stem, blob []byte) {
	if err := db.Put(ubtFlatStateKey(stem), blob); err != nil {
		log.Crit("Failed to store UBT flat-state stem", "err", err)
	}
}

// DeleteUBTFlatStem removes the flat-state blob for the given stem.
func DeleteUBTFlatStem(db ethdb.KeyValueWriter, stem []byte) {
	if err := db.Delete(ubtFlatStateKey(stem)); err != nil {
		log.Crit("Failed to delete UBT flat-state stem", "err", err)
	}
}

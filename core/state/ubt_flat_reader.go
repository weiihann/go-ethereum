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
	"encoding/binary"
	"errors"
	"math/bits"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie/bintrie"
	"github.com/holiman/uint256"
)

// errStemNotInFlatState is the sentinel returned by ubtFlatReader when the
// requested account or storage slot cannot be served from the on-disk flat
// state. multiStateReader treats any non-nil error as "try the next reader",
// so returning this pushes the lookup to ubtTrieReader for full traversal.
//
// We use an error (rather than (nil, nil)) for cache-miss cases because
// (nil, nil) is the encoding for "definitively absent" — returning it would
// make the multi-reader short-circuit and skip the trie reader, masking any
// account or slot that exists in the trie but isn't yet in flat state.
var errStemNotInFlatState = errors.New("UBT stem not in flat state")

// ubtFlatReader serves account and storage reads from the UBT flat-state
// blobs persisted under rawdb.UBTFlatStatePrefix. Each stem blob packs all
// present suffix slots into [bitmap(32) || values...].
//
// The reader does NOT walk pathdb's diff-layer tree, so it serves only the
// latest committed state. Historical-state reads must fall through to the
// trie reader via the errStemNotInFlatState sentinel. For benchmark scope
// (latest-root reads only) this is sufficient.
type ubtFlatReader struct {
	disk ethdb.KeyValueReader
}

// Compile-time assertion that ubtFlatReader implements StateReader.
var _ StateReader = (*ubtFlatReader)(nil)

// newUBTFlatReader constructs a reader that pulls UBT flat-state blobs
// directly from the given disk store.
func newUBTFlatReader(disk ethdb.KeyValueReader) *ubtFlatReader {
	return &ubtFlatReader{disk: disk}
}

// Account decodes the basic-data and code-hash suffixes from the address's
// account stem. Returns errStemNotInFlatState whenever the stem blob is
// missing or either expected suffix bit is unset; the trie reader then
// handles the fall-through.
func (r *ubtFlatReader) Account(addr common.Address) (*types.StateAccount, error) {
	var zero [32]byte
	key := bintrie.GetBinaryTreeKey(addr, zero[:])
	stem := key[:bintrie.StemSize]

	blob := rawdb.ReadUBTFlatStem(r.disk, stem)
	if blob == nil {
		return nil, errStemNotInFlatState
	}
	basicData, basicOK := lookupSuffix(blob, bintrie.BasicDataLeafKey)
	codeHash, codeOK := lookupSuffix(blob, bintrie.CodeHashLeafKey)
	if !basicOK || !codeOK {
		// Partial population — let the trie reader decide.
		return nil, errStemNotInFlatState
	}
	// Both fields zeroed-out mark a deleted account (mirrors bintrie.GetAccount).
	if bytes.Equal(basicData, zero[:]) && bytes.Equal(codeHash, zero[:]) {
		return nil, nil
	}
	if len(basicData) < bintrie.BasicDataBalanceOffset+16 {
		return nil, errStemNotInFlatState
	}
	var balance [16]byte
	copy(balance[:], basicData[bintrie.BasicDataBalanceOffset:])
	return &types.StateAccount{
		Nonce:    binary.BigEndian.Uint64(basicData[bintrie.BasicDataNonceOffset:]),
		Balance:  new(uint256.Int).SetBytes(balance[:]),
		CodeHash: bytes.Clone(codeHash),
		Root:     types.EmptyRootHash,
	}, nil
}

// Storage decodes a single storage slot value from its stem blob. Returns
// errStemNotInFlatState if the stem blob is missing or the suffix bit for
// the requested slot is unset.
func (r *ubtFlatReader) Storage(addr common.Address, slotKey common.Hash) (common.Hash, error) {
	key := bintrie.GetBinaryTreeKeyStorageSlot(addr, slotKey[:])
	stem := key[:bintrie.StemSize]
	suffix := key[bintrie.StemSize]

	blob := rawdb.ReadUBTFlatStem(r.disk, stem)
	if blob == nil {
		return common.Hash{}, errStemNotInFlatState
	}
	val, ok := lookupSuffix(blob, suffix)
	if !ok {
		return common.Hash{}, errStemNotInFlatState
	}
	var out common.Hash
	copy(out[:], val)
	return out, nil
}

// lookupSuffix returns the 32-byte value for the given suffix in a stem
// blob, or (nil, false) if the bitmap bit is unset or the blob is malformed.
//
// Blob layout: [bitmap(32) || value₀(32) || value₁(32) || ...].
// Bit `s` of the bitmap lives at bitmap[s/8] >> (7 - s%8) (MSB-first within
// each byte, matching state-actor's serializeStemBlob encoding). Present
// values are packed in suffix order, so the value for suffix s is at offset
// 32 + popcount(bitmap[0..s-1]) * 32.
func lookupSuffix(blob []byte, suffix uint8) ([]byte, bool) {
	const bitmapSize = 32
	const valueSize = 32
	if len(blob) < bitmapSize {
		return nil, false
	}
	bitmap := blob[:bitmapSize]
	byteIdx := suffix / 8
	bitOffset := 7 - suffix%8
	if bitmap[byteIdx]&(1<<bitOffset) == 0 {
		return nil, false
	}
	// Count present bits at suffixes 0..(suffix-1) to get the value index.
	idx := 0
	for i := uint8(0); i < byteIdx; i++ {
		idx += bits.OnesCount8(bitmap[i])
	}
	if rem := suffix % 8; rem != 0 {
		// Within byteIdx, count bits at positions 7..(8-rem) — the top `rem`
		// bits, which correspond to suffixes byteIdx*8 .. suffix-1.
		mask := uint8(0xFF) << (8 - rem)
		idx += bits.OnesCount8(bitmap[byteIdx] & mask)
	}
	off := bitmapSize + idx*valueSize
	if off+valueSize > len(blob) {
		return nil, false
	}
	return blob[off : off+valueSize], true
}

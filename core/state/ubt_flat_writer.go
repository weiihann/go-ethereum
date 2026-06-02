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
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie/bintrie"
)

const valueSize = 32

// writeUBTFlatState applies the per-stem flat-state mutations implied by the
// given StateUpdate to disk, batched as a single write.
//
// For each stem touched by an account or storage change:
//  1. Read the existing 'F'-prefixed blob (nil if no prior blob exists).
//  2. Decode bitmap+values into a per-suffix map.
//  3. Overlay this block's mutations.
//  4. Re-serialize bitmap+values.
//  5. Write back via the batch (or delete if the merged blob is empty).
//
// Requires update.StorageKeyType == StorageKeyPlain — the binary trie needs
// raw slot keys to derive stems, and hashed keys would lose that mapping.
func writeUBTFlatState(disk ethdb.KeyValueStore, codedb *CodeDB, update *StateUpdate) error {
	if update.StorageKeyType != StorageKeyPlain {
		return fmt.Errorf("UBT flat-state requires plain storage keys, got %v", update.StorageKeyType)
	}

	// Collect per-stem suffix changes. Values are always 32-byte buffers;
	// a nil value entry isn't used because deletion at the bintrie level
	// writes 32 zero bytes rather than removing the suffix.
	mods := make(map[[bintrie.StemSize]byte]map[byte][]byte)
	putMod := func(stem []byte, suffix byte, val []byte) {
		var key [bintrie.StemSize]byte
		copy(key[:], stem)
		if mods[key] == nil {
			mods[key] = make(map[byte][]byte, 4)
		}
		mods[key][suffix] = val
	}

	reader := codedb.Reader()

	// Account modifications. AccountsOrigin is keyed by raw address; Accounts
	// is keyed by addrHash. Walk Origin to enumerate the addresses changed.
	for addr := range update.AccountsOrigin {
		addrHash := crypto.Keccak256Hash(addr[:])
		acct := update.Accounts[addrHash]

		stem := bintrie.GetBinaryTreeStemAccount(addr)

		if acct == nil {
			// Deletion: write 32 zero bytes to both basic-data and code-hash
			// (matches BinaryTrie.DeleteAccount).
			z := make([]byte, valueSize)
			z2 := make([]byte, valueSize)
			putMod(stem, bintrie.BasicDataLeafKey, z)
			putMod(stem, bintrie.CodeHashLeafKey, z2)
			continue
		}
		codeSize := lookupCodeSize(update, codedb, reader, addr, acct.CodeHash)
		basicData := encodeBasicData(acct, codeSize)
		ch := make([]byte, valueSize)
		copy(ch, acct.CodeHash)
		putMod(stem, bintrie.BasicDataLeafKey, basicData[:])
		putMod(stem, bintrie.CodeHashLeafKey, ch)
	}

	// Storage modifications. StoragesOrigin is keyed by raw address + raw
	// slot key (when StorageKeyType == StorageKeyPlain); Storages is keyed
	// by addrHash + slotKeyHash. Use Origin's raw keys to derive stems and
	// look up new values in Storages by hash.
	for addr, slotMap := range update.StoragesOrigin {
		addrHash := crypto.Keccak256Hash(addr[:])
		newSlots := update.Storages[addrHash]
		for rawKey := range slotMap {
			slotKeyHash := crypto.Keccak256Hash(rawKey[:])
			newVal := newSlots[slotKeyHash]

			slotFullKey := bintrie.GetBinaryTreeKeyStorageSlot(addr, rawKey[:])
			stem := slotFullKey[:bintrie.StemSize]
			suffix := slotFullKey[bintrie.StemSize]

			val := make([]byte, valueSize)
			copy(val, newVal[:])
			putMod(stem, suffix, val)
		}
	}

	if len(mods) == 0 {
		return nil
	}

	// Read-modify-write each affected stem in a single batch.
	batch := disk.NewBatch()
	for stem, suffixMods := range mods {
		existing := rawdb.ReadUBTFlatStem(disk, stem[:])
		merged := mergeStemBlob(existing, suffixMods)
		if len(merged) == 0 {
			rawdb.DeleteUBTFlatStem(batch, stem[:])
			continue
		}
		rawdb.WriteUBTFlatStem(batch, stem[:], merged)
	}
	if err := batch.Write(); err != nil {
		return fmt.Errorf("UBT flat-state batch write: %w", err)
	}
	return nil
}

// lookupCodeSize resolves the code length for an account in this block's
// commit. New codes appear in update.Codes; unchanged codes are read from
// codedb. Returns 0 for accounts without code (EmptyCodeHash).
func lookupCodeSize(update *StateUpdate, codedb *CodeDB, reader *CodeReader, addr common.Address, codeHash []byte) int {
	if len(codeHash) == 0 || bytes.Equal(codeHash, types.EmptyCodeHash.Bytes()) {
		return 0
	}
	if c, ok := update.Codes[addr]; ok && c != nil {
		return len(c.Blob)
	}
	return reader.CodeSize(addr, common.BytesToHash(codeHash))
}

// encodeBasicData mirrors bintrie.BinaryTrie.UpdateAccount's layout:
//
//	bytes 4..7   = code size (uint32 BE)
//	bytes 8..15  = nonce (uint64 BE)
//	bytes 16..31 = balance (right-aligned BE, max 16 bytes)
func encodeBasicData(acct *types.StateAccount, codeSize int) [32]byte {
	var buf [32]byte
	binary.BigEndian.PutUint32(buf[bintrie.BasicDataCodeSizeOffset-1:], uint32(codeSize))
	binary.BigEndian.PutUint64(buf[bintrie.BasicDataNonceOffset:], acct.Nonce)
	balance := acct.Balance.Bytes()
	if len(balance) > 16 {
		// Mirrors bintrie behaviour for the --dev pre-funded account.
		balance = balance[len(balance)-16:]
	}
	copy(buf[32-len(balance):], balance)
	return buf
}

// mergeStemBlob takes an existing stem blob (nil if no prior blob) and
// overlays the per-suffix modifications, returning the new serialized blob.
// All values in mods are 32-byte buffers; setting a suffix's bitmap bit
// requires re-encoding the full blob.
func mergeStemBlob(existing []byte, mods map[byte][]byte) []byte {
	values := make(map[byte][]byte, len(mods)+8)
	if len(existing) >= 32 {
		bitmap := existing[:32]
		off := 32
		for byteIdx := 0; byteIdx < 32; byteIdx++ {
			b := bitmap[byteIdx]
			if b == 0 {
				continue
			}
			for bit := 7; bit >= 0; bit-- {
				if b&(1<<uint(bit)) == 0 {
					continue
				}
				suffix := byte(byteIdx*8 + (7 - bit))
				if off+valueSize > len(existing) {
					// Malformed blob — treat as if the suffix is absent. The
					// trie reader will surface any real corruption.
					return serializeStemBlob(values)
				}
				cp := make([]byte, valueSize)
				copy(cp, existing[off:off+valueSize])
				values[suffix] = cp
				off += valueSize
			}
		}
	}
	for suffix, val := range mods {
		values[suffix] = val
	}
	return serializeStemBlob(values)
}

// serializeStemBlob packs the per-suffix map into the binary-trie flat-state
// layout: [bitmap(32) || values...] with values in suffix order. Returns
// nil for an empty map (caller should delete the stem key in that case).
func serializeStemBlob(values map[byte][]byte) []byte {
	if len(values) == 0 {
		return nil
	}
	var bitmap [32]byte
	for suffix := range values {
		bitmap[suffix/8] |= 1 << (7 - suffix%8)
	}
	blob := make([]byte, 32+valueSize*len(values))
	copy(blob, bitmap[:])
	offset := 32
	// Walk suffixes in ascending order by iterating the bitmap bytes/bits.
	for byteIdx := 0; byteIdx < 32; byteIdx++ {
		b := bitmap[byteIdx]
		if b == 0 {
			continue
		}
		for bit := 7; bit >= 0; bit-- {
			if b&(1<<uint(bit)) == 0 {
				continue
			}
			suffix := byte(byteIdx*8 + (7 - bit))
			copy(blob[offset:offset+valueSize], values[suffix])
			offset += valueSize
		}
	}
	return blob
}

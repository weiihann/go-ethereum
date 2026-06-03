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

package trie

// Archive-expiry record encoding (gballet/archival-command format). A moved-out
// subtree is stored as a flat list of archive.Record{Path, Value} where Path is
// the full hex-nibble key (with terminator) from the subtree root to a leaf and
// Value is that leaf's value bytes. Interior nodes are dropped; the subtree is
// rebuilt on read by re-inserting every (Path, Value) into a canonical MPT,
// whose hash equals the original subtree root hash. This file is the write side
// (leaf collector + stub encoder) plus the reconstruction used to verify each
// subtree at convert time before its interior nodes are deleted.

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/trie/archive"
)

// EncodeArchiveRecords collects the leaf records of a materialised subtree and
// returns them together with the keccak hash of the subtree reconstructed from
// those records. Callers compare the returned hash against the subtree's
// expected root hash to catch encoder bugs before deleting the original nodes
// (the archive-expiry equivalent of EncodeInactiveBlob's hash invariance check).
func EncodeArchiveRecords(root node) ([]*archive.Record, common.Hash, error) {
	if root == nil {
		return nil, common.Hash{}, errors.New("trie: cannot encode nil root for archive records")
	}
	var recs []*archive.Record
	if err := collectLeafRecords(root, nil, &recs); err != nil {
		return nil, common.Hash{}, err
	}
	rebuilt, err := archiveRecordsToNode(recs)
	if err != nil {
		return nil, common.Hash{}, fmt.Errorf("trie: reconstruct archive records: %w", err)
	}
	h := newHasher(false)
	hash := common.BytesToHash(h.hash(rebuilt, true))
	returnHasherToPool(h)
	return recs, hash, nil
}

// EncodeExpiredNodeBlob creates the raw 17-byte chaindb value for an
// archive-expiry expired node: 1-byte marker (0x00) + 8-byte file offset +
// 8-byte size of the subtree's record block.
func EncodeExpiredNodeBlob(offset, size uint64) []byte {
	buf := make([]byte, 1+2*archive.OffsetSize)
	buf[0] = expiredNodeMarker
	binary.BigEndian.PutUint64(buf[1:], offset)
	binary.BigEndian.PutUint64(buf[1+archive.OffsetSize:], size)
	return buf
}

// collectLeafRecords walks a materialised subtree in pre-order, accumulating the
// hex-nibble path from the subtree root, and appends one record per leaf. prefix
// is copied at every descent so sibling recursions never alias the same backing
// array. Secure tries never place a value in a branch's 17th slot (all keys are
// fixed 32-byte hashes), so only children 0..15 are visited.
func collectLeafRecords(n node, prefix []byte, out *[]*archive.Record) error {
	switch n := n.(type) {
	case nil:
		return nil
	case valueNode:
		*out = append(*out, &archive.Record{Path: prefix, Value: []byte(n)})
		return nil
	case *shortNode:
		return collectLeafRecords(n.Val, concatNibbles(prefix, n.Key), out)
	case *fullNode:
		for i := 0; i < 16; i++ {
			if n.Children[i] == nil {
				continue
			}
			if err := collectLeafRecords(n.Children[i], concatNibbles(prefix, []byte{byte(i)}), out); err != nil {
				return err
			}
		}
		return nil
	case hashNode:
		return fmt.Errorf("trie: unexpected hashNode at path %x in materialised subtree", prefix)
	default:
		return fmt.Errorf("trie: unexpected node type %T in materialised subtree", n)
	}
}

// concatNibbles returns a fresh slice holding a followed by b.
func concatNibbles(a, b []byte) []byte {
	out := make([]byte, len(a)+len(b))
	copy(out, a)
	copy(out[len(a):], b)
	return out
}

// archiveRecordsToNode rebuilds the canonical MPT subtree from its leaf records
// by inserting each (Path, Value) into an initially-nil trie. The reconstructed
// subtree is structurally identical to the original (the same key/value set has
// a unique canonical MPT), so its hash matches the original subtree root hash.
func archiveRecordsToNode(records []*archive.Record) (node, error) {
	if len(records) == 0 {
		return nil, archive.EmptyArchiveRecord
	}
	var root node
	for i, record := range records {
		if err := validateRecordPath(record.Path); err != nil {
			return nil, err
		}
		key, err := normalizeRecordKey(record.Path)
		if err != nil {
			return nil, err
		}
		if len(key) < 1 {
			return nil, fmt.Errorf("empty key in record #%d", i)
		}
		root, err = insertTrieNode(root, key, valueNode(record.Value))
		if err != nil {
			return nil, err
		}
	}
	return root, nil
}

func validateRecordPath(path []byte) error {
	for i, b := range path {
		if b > 16 {
			return fmt.Errorf("invalid nibble in record path: %d", b)
		}
		if b == 16 && i != len(path)-1 {
			return fmt.Errorf("terminator nibble in middle of record path")
		}
	}
	return nil
}

// normalizeRecordKey ensures the record path is a hex-nibble key suitable for
// leaf insertion by guaranteeing a single terminator nibble and preserving any
// already-terminated path. Empty paths are normalized to a sole terminator.
func normalizeRecordKey(path []byte) ([]byte, error) {
	if len(path) == 0 {
		return []byte{16}, nil
	}
	if hasTerm(path) {
		return path, nil
	}
	key := append([]byte{}, path...)
	key = append(key, 16)
	return key, nil
}

func insertTrieNode(n node, key []byte, value node) (node, error) {
	if len(key) == 0 {
		return value, nil
	}
	switch n := n.(type) {
	case *shortNode:
		matchlen := prefixLen(key, n.Key)
		if matchlen == len(n.Key) {
			nn, err := insertTrieNode(n.Val, key[matchlen:], value)
			if err != nil {
				return nil, err
			}
			return &shortNode{Key: n.Key, Val: nn}, nil
		}
		branch := &fullNode{}
		var err error
		branch.Children[n.Key[matchlen]], err = insertTrieNode(nil, n.Key[matchlen+1:], n.Val)
		if err != nil {
			return nil, err
		}
		branch.Children[key[matchlen]], err = insertTrieNode(nil, key[matchlen+1:], value)
		if err != nil {
			return nil, err
		}
		if matchlen == 0 {
			return branch, nil
		}
		return &shortNode{Key: key[:matchlen], Val: branch}, nil

	case *fullNode:
		child, err := insertTrieNode(n.Children[key[0]], key[1:], value)
		if err != nil {
			return nil, err
		}
		n.Children[key[0]] = child
		return n, nil

	case nil:
		return &shortNode{Key: key, Val: value}, nil

	default:
		return nil, fmt.Errorf("invalid node type in trie insert: %T", n)
	}
}

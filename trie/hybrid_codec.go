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

// EIP-8188 hybrid-node format. A hybrid node is the chaindb form of a
// partially-materialised parent: standard MPT RLP plus inline metadata
// pointing at *expiredNode children that remain in the inactive file.
//
// Layout:
//
//   byte 0:        0x01            ← hybrid marker (HybridMarker in inactive pkg)
//   bytes 1..N:    standard RLP    ← the node's pre-conversion RLP, byte-identical
//                                    (off-path children appear as their original
//                                     32-byte hashNode references)
//   metadata:
//     byte 0:      uint8 stubCount
//     bytes 1..8:  uint64 BE blobOffset  ← shared by all sub-stubs in this node
//     [stubCount × 9 bytes]:
//       byte 0:    uint8 childIndex     ← 0..16 for fullNode; always 0 for shortNode
//       bytes 1..4: uint32 BE nodeOffsetInBlob
//       bytes 5..8: uint32 BE nodeSize
//
// On read, the standard RLP decodes into a *fullNode/*shortNode whose hashed
// children are temporarily hashNode references; decodeHybrid then patches
// those positions with *expiredNode using the metadata's (offset, size) and
// the hashNode bytes already in the decoded structure as the claimed hash.
//
// Hash invariance:
//   - The "claimed hash" of a hybrid node — what the grandparent's RLP
//     references — is keccak(standardRLP), where standardRLP is the slice
//     from byte 1 to the end of the RLP. Because *expiredNode.encode()
//     writes the original hashNode bytes verbatim, this RLP is identical
//     to what the original (un-stubbed) parent produced.
//   - The pathdb reader skips its hash check on 0x01-prefixed values; the
//     full hybrid bytes hash differently, but that's fine because the
//     IN-MEMORY node tree's hash (computed via cache()/encode()) uses only
//     the standard RLP component.

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/triedb/inactive"
)

// stubEntryLen is the size of one inline sub-stub entry (childIndex,
// nodeOffsetInBlob, nodeSize).
const stubEntryLen = 1 + 4 + 4

// hasExpiredNodeChildren reports whether n has any *expiredNode children.
// Determines whether the storage encoder emits hybrid bytes or standard RLP.
func hasExpiredNodeChildren(n node) bool {
	switch n := n.(type) {
	case *fullNode:
		for _, c := range &n.Children {
			if _, ok := c.(*expiredNode); ok {
				return true
			}
		}
		return false
	case *shortNode:
		_, ok := n.Val.(*expiredNode)
		return ok
	}
	return false
}

// nodeStorageBytes returns the chaindb-storage form of n. If n has any
// *expiredNode children, it produces hybrid bytes (0x01 marker + standard
// RLP + metadata). Otherwise it returns standard RLP via nodeToBytes.
//
// Used by the committer when emitting trie nodes that include partially-
// materialised subtrees from EIP-8188 lazy materialisation.
func nodeStorageBytes(n node) []byte {
	if !hasExpiredNodeChildren(n) {
		return nodeToBytes(n)
	}
	return assembleHybridBytes(n)
}

// assembleHybridBytes produces the on-disk hybrid representation of n. The
// caller has already verified that n has at least one *expiredNode child.
//
// The standard-RLP component is byte-identical to what nodeToBytes(n) would
// produce, because *expiredNode.encode() writes the original 32-byte
// hashNode RLP element. The metadata records the blob offset (shared) plus
// (childIndex, nodeOffsetInBlob, nodeSize) for each *expiredNode child.
func assembleHybridBytes(n node) []byte {
	stdRLP := nodeToBytes(n)

	var (
		blobOffset uint64
		stubs      []hybridStub
		ok         bool
	)
	switch n := n.(type) {
	case *fullNode:
		blobOffset, stubs, ok = collectFullNodeStubs(n)
	case *shortNode:
		blobOffset, stubs, ok = collectShortNodeStubs(n)
	}
	if !ok {
		// Caller must guarantee at least one *expiredNode child via
		// hasExpiredNodeChildren before calling. Defensive: fall back to RLP.
		return stdRLP
	}

	out := make([]byte, 0, 1+len(stdRLP)+1+8+len(stubs)*stubEntryLen)
	out = append(out, inactive.HybridMarker)
	out = append(out, stdRLP...)
	out = append(out, byte(len(stubs)))
	var blobBuf [8]byte
	binary.BigEndian.PutUint64(blobBuf[:], blobOffset)
	out = append(out, blobBuf[:]...)
	for _, s := range stubs {
		var buf [stubEntryLen]byte
		buf[0] = s.childIndex
		// nodeOffsetInBlob is nodeFileOffset - blobOffset; both fit in uint32
		// because the blob's total size is bounded by uint32 (per the v2 blob
		// header's RootOffset/RootSize fields).
		binary.BigEndian.PutUint32(buf[1:5], uint32(s.nodeFileOffset-blobOffset))
		binary.BigEndian.PutUint32(buf[5:9], s.nodeSize)
		out = append(out, buf[:]...)
	}
	stdRLPHash := common.Hash(crypto.Keccak256Hash(stdRLP))
	cachedHash, _ := n.cache()
	if cachedHash != nil && common.BytesToHash(cachedHash) != stdRLPHash {
		// SMOKING GUN: the trie's standard hasher (fullnodeEncoder, used to
		// compute n.cache()) and the hybrid commit path (fullNode.encode +
		// nodeToBytes via assembleHybridBytes) disagree on this node's RLP.
		// They are supposed to produce byte-identical output. A mismatch
		// here is the most likely root cause of import-time merkle-root
		// drift on a converted chaindata.
		log.Warn("eip8188 hybrid commit: stdRLP/cached hash mismatch",
			"node-type", fmt.Sprintf("%T", n),
			"cached-hash", common.BytesToHash(cachedHash),
			"std-rlp-hash", stdRLPHash,
			"std-rlp-len", len(stdRLP),
			"blob-offset", blobOffset,
			"stub-count", len(stubs))
	} else {
		log.Debug("eip8188 hybrid commit",
			"node-type", fmt.Sprintf("%T", n),
			"std-rlp-hash", stdRLPHash,
			"std-rlp-len", len(stdRLP),
			"blob-offset", blobOffset,
			"stub-count", len(stubs))
	}
	return out
}

// hybridStub captures one *expiredNode child's identity for hybrid metadata.
type hybridStub struct {
	childIndex     uint8
	nodeFileOffset uint64
	nodeSize       uint32
}

func collectFullNodeStubs(n *fullNode) (uint64, []hybridStub, bool) {
	var (
		blobOffset uint64
		stubs      []hybridStub
		seen       bool
	)
	for i, c := range &n.Children {
		en, ok := c.(*expiredNode)
		if !ok {
			continue
		}
		if !seen {
			blobOffset = en.blobOffset
			seen = true
		} else if en.blobOffset != blobOffset {
			// Sub-stubs from different blobs in one parent — shouldn't
			// happen given the lazy materialiser's invariants.
			return 0, nil, false
		}
		stubs = append(stubs, hybridStub{
			childIndex:     uint8(i),
			nodeFileOffset: en.nodeFileOffset,
			nodeSize:       en.size,
		})
	}
	return blobOffset, stubs, seen
}

func collectShortNodeStubs(n *shortNode) (uint64, []hybridStub, bool) {
	en, ok := n.Val.(*expiredNode)
	if !ok {
		return 0, nil, false
	}
	return en.blobOffset, []hybridStub{{
		childIndex:     0, // shortNodes have a single child
		nodeFileOffset: en.nodeFileOffset,
		nodeSize:       en.size,
	}}, true
}

// decodeHybrid parses hybrid bytes back into a live `node` whose hashNode
// children at the named indexes have been replaced with *expiredNode
// references derived from the inline metadata.
//
// `hash` is the parent's claimed hashNode reference — the value cached as
// this node's hash. Equals keccak(standardRLP) by construction.
func decodeHybrid(hash, buf []byte) (node, error) {
	if len(buf) < 1 {
		return nil, errors.New("trie: hybrid: empty buffer")
	}
	if buf[0] != inactive.HybridMarker {
		return nil, fmt.Errorf("trie: not a hybrid node (marker=0x%02x)", buf[0])
	}
	stdStart := 1
	// Find where the standard RLP ends by inspecting its self-describing length.
	if stdStart >= len(buf) {
		return nil, errors.New("trie: hybrid: missing standard RLP")
	}
	_, _, rest, err := rlp.Split(buf[stdStart:])
	if err != nil {
		return nil, fmt.Errorf("trie: hybrid: split standard RLP: %w", err)
	}
	stdEnd := len(buf) - len(rest)

	// Decode the standard RLP into a regular node tree.
	n, err := decodeNodeUnsafe(hash, buf[stdStart:stdEnd])
	if err != nil {
		return nil, fmt.Errorf("trie: hybrid: decode standard RLP: %w", err)
	}

	// Parse the metadata: stubCount, blobOffset, then stub entries.
	meta := buf[stdEnd:]
	if len(meta) < 1+8 {
		return nil, fmt.Errorf("trie: hybrid: truncated metadata header (%d bytes)", len(meta))
	}
	stubCount := int(meta[0])
	blobOffset := binary.BigEndian.Uint64(meta[1:9])
	if len(meta) != 1+8+stubCount*stubEntryLen {
		return nil, fmt.Errorf("trie: hybrid: metadata length mismatch (have %d, want %d)",
			len(meta), 1+8+stubCount*stubEntryLen)
	}

	// Patch each named child with an *expiredNode.
	pos := 1 + 8
	for i := 0; i < stubCount; i++ {
		childIndex := meta[pos]
		nodeOffsetInBlob := binary.BigEndian.Uint32(meta[pos+1 : pos+5])
		nodeSize := binary.BigEndian.Uint32(meta[pos+5 : pos+9])
		pos += stubEntryLen

		if err := patchExpiredChild(n, childIndex, blobOffset, blobOffset+uint64(nodeOffsetInBlob), nodeSize); err != nil {
			return nil, fmt.Errorf("trie: hybrid: patch child %d: %w", childIndex, err)
		}
	}
	stdRLPHash := common.Hash(crypto.Keccak256Hash(buf[stdStart:stdEnd]))
	claimedHash := common.BytesToHash(hash)
	if claimedHash != (common.Hash{}) && stdRLPHash != claimedHash {
		// SMOKING GUN: the parent's hashNode reference says one thing,
		// but the standard RLP we just decoded hashes to something else.
		// This means the hybrid was written with corrupt stdRLP, OR the
		// chaindb retrieval associated the wrong hash with this entry.
		log.Warn("eip8188 hybrid decode: hash mismatch",
			"claimed-hash", claimedHash,
			"std-rlp-hash", stdRLPHash,
			"std-rlp-len", stdEnd-stdStart,
			"blob-offset", blobOffset,
			"stub-count", stubCount)
	} else {
		log.Debug("eip8188 hybrid decode",
			"claimed-hash", claimedHash,
			"std-rlp-len", stdEnd-stdStart,
			"blob-offset", blobOffset,
			"stub-count", stubCount)
	}
	return n, nil
}

// patchExpiredChild replaces the hashNode at the named position with an
// *expiredNode carrying the supplied blob/file/size info. The hash for the
// *expiredNode comes from the existing hashNode bytes — that's the parent's
// claimed hashNode reference, which equals the *expiredNode's pre-conversion
// hash by construction.
func patchExpiredChild(n node, childIndex uint8, blobOffset, nodeFileOffset uint64, nodeSize uint32) error {
	switch n := n.(type) {
	case *fullNode:
		if int(childIndex) >= len(n.Children) {
			return fmt.Errorf("childIndex %d out of range for fullNode", childIndex)
		}
		hn, ok := n.Children[childIndex].(hashNode)
		if !ok {
			return fmt.Errorf("fullNode child %d is %T, want hashNode", childIndex, n.Children[childIndex])
		}
		n.Children[childIndex] = &expiredNode{
			blobOffset:     blobOffset,
			nodeFileOffset: nodeFileOffset,
			size:           nodeSize,
			hash:           common.BytesToHash(hn),
		}
		return nil
	case *shortNode:
		if childIndex != 0 {
			return fmt.Errorf("shortNode supports only childIndex=0 (got %d)", childIndex)
		}
		hn, ok := n.Val.(hashNode)
		if !ok {
			return fmt.Errorf("shortNode val is %T, want hashNode", n.Val)
		}
		n.Val = &expiredNode{
			blobOffset:     blobOffset,
			nodeFileOffset: nodeFileOffset,
			size:           nodeSize,
			hash:           common.BytesToHash(hn),
		}
		return nil
	}
	return fmt.Errorf("unsupported node type %T for hybrid patch", n)
}

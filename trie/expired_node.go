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

import (
	"encoding/binary"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/triedb/database"
)

// expiredNodeMarker is the first byte of every PRIMARY stub written into the
// chaindb in place of an inactive subtree's root. Trie nodes are RLP-encoded
// shortNodes or fullNodes, both of which are RLP lists and therefore start
// with byte 0xc0 or higher. The 0x00 prefix is consequently unambiguous.
//
// Hybrid nodes (partial materialisation) use marker 0x01; see hybrid_codec.go.
const expiredNodeMarker = 0x00

// expiredNodeStubLen is the on-disk length of a primary stub. Layout:
//
//	byte 0:        0x00          (marker)
//	bytes 1..8:    blobOffset    (uint64 BE) — file offset of containing blob's header
//	bytes 9..12:   rootInBlob    (uint32 BE) — offset of root node within the blob
//	bytes 13..16:  rootSize      (uint32 BE) — size of root node entry
const expiredNodeStubLen = 1 + 8 + 4 + 4

// ArchiveResolverFn returns the bytes of a frozen-trie blob residing at the
// given (offset, size) in the inactive file. It's installed on a Trie via
// SetArchiveResolver and consumed when the trie hits an *expiredNode during
// Get/Insert/Delete.
type ArchiveResolverFn = database.ArchiveResolverFn

// expiredNode represents a subtree that has been moved out of the live pathdb
// and into the inactive file. Two flavours share this type:
//
//   - Primary stub: the chaindb entry at the subtree's (owner, path) is a
//     17-byte primary stub. After decode, blobOffset == file offset of the
//     blob's header; nodeFileOffset == blobOffset + rootOffsetInBlob (the
//     root node entry's file position); size == root node entry's size.
//
//   - Sub-stub: created by the lazy materialiser when a write descends into
//     an *expiredNode and substitutes off-path siblings with new
//     *expiredNode references. Lives only in memory until the parent commits
//     as a hybrid node (0x01 marker). blobOffset is inherited from the
//     parent's blob; nodeFileOffset addresses a node entry within that blob.
//
// expiredNodes never participate in the commit pipeline directly — they are
// folded into their parent's hybrid chaindb entry by the storage encoder, or
// fully materialised before commit if their entire subtree must be live.
type expiredNode struct {
	// blobOffset is the file offset of the containing blob's 16-byte header.
	// Used to resolve intra-blob child slot references (which are blob-relative).
	blobOffset uint64
	// nodeFileOffset is the absolute file offset of THIS node entry's bytes.
	// For a primary stub, this is blobOffset + header.RootOffset; for a
	// sub-stub, it is blobOffset + slot.Offset of the substituted child.
	nodeFileOffset uint64
	// size is the byte length of THIS node entry (not the whole blob).
	size uint32
	// hash is the claimed pre-conversion hash of this node — i.e., the
	// parent's hashNode reference. Used by cache() and by *expiredNode.encode()
	// so the parent's standard RLP is byte-identical to its pre-conversion form.
	hash common.Hash
}

// cache returns the original subtree-root hash and reports the node as clean.
// The committer treats clean nodes as already-hashed and skips re-encoding,
// which is exactly what we want for an unchanged inactive subtree.
func (n *expiredNode) cache() (hashNode, bool) {
	return hashNode(n.hash.Bytes()), false
}

// encode emits a standard 32-byte hashNode RLP element. This makes any parent
// fullNode/shortNode whose RLP includes this *expiredNode as a child produce
// a standard RLP byte-identical to its pre-conversion form, preserving the
// parent's MPT hash. The (blobOffset, nodeFileOffset, size) information
// travels separately via hybrid metadata appended by the storage encoder.
func (n *expiredNode) encode(w rlp.EncoderBuffer) {
	w.WriteBytes(n.hash[:])
}

func (n *expiredNode) fstring(ind string) string {
	return fmt.Sprintf("<expired blob=%d node=%d size=%d hash=%x>",
		n.blobOffset, n.nodeFileOffset, n.size, n.hash)
}

// decodeStub parses the 17-byte primary stub and returns an *expiredNode
// initialised with `hash` as its identity. The hash argument is the parent's
// hashNode reference — the original (pre-conversion) subtree root hash —
// and is what cache() returns later.
func decodeStub(hash, buf []byte) (*expiredNode, error) {
	if len(buf) != expiredNodeStubLen {
		return nil, fmt.Errorf("invalid stub length: got %d, want %d", len(buf), expiredNodeStubLen)
	}
	blobOffset := binary.BigEndian.Uint64(buf[1:9])
	rootInBlob := binary.BigEndian.Uint32(buf[9:13])
	rootSize := binary.BigEndian.Uint32(buf[13:17])
	return &expiredNode{
		blobOffset:     blobOffset,
		nodeFileOffset: blobOffset + uint64(rootInBlob),
		size:           rootSize,
		hash:           common.BytesToHash(hash),
	}, nil
}

// EncodeStub returns the 17 bytes that will be written to the chaindb at the
// stub's (owner, path) location. The converter computes (blobOffset,
// rootInBlob, rootSize) from the blob it just appended: blobOffset is the
// file position where the blob starts, and (rootInBlob, rootSize) come
// directly from the blob's header.
func EncodeStub(blobOffset uint64, rootInBlob, rootSize uint32) []byte {
	out := make([]byte, expiredNodeStubLen)
	out[0] = expiredNodeMarker
	binary.BigEndian.PutUint64(out[1:9], blobOffset)
	binary.BigEndian.PutUint32(out[9:13], rootInBlob)
	binary.BigEndian.PutUint32(out[13:17], rootSize)
	return out
}

// IsStub reports whether the given encoded-node bytes are a primary stub.
// Useful for the pathdb reader to skip its hash check on stub blobs.
//
// IsStub matches ONLY the primary-stub marker (0x00). For hybrid nodes
// (0x01), use IsStubOrHybrid (or inactive.IsStubOrHybrid).
func IsStub(blob []byte) bool {
	return len(blob) > 0 && blob[0] == expiredNodeMarker
}

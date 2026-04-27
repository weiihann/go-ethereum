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
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/triedb/inactive"
)

// TestHybridFullNodeRoundTrip: build a *fullNode with one *expiredNode child
// and one regular hashNode child. Encode hybrid → decode hybrid → recover
// the same structure with the *expiredNode patched in.
func TestHybridFullNodeRoundTrip(t *testing.T) {
	// Construct a *fullNode with:
	//   - Children[2] = a regular 32-byte hashNode
	//   - Children[5] = an *expiredNode (sub-stub)
	//   - Children[16] = a valueNode
	regularHash := common.HexToHash("0xaa")
	expiredHash := common.HexToHash("0xbb")
	value := []byte("VALUE")

	fn := &fullNode{flags: nodeFlag{dirty: true}}
	fn.Children[2] = hashNode(regularHash.Bytes())
	fn.Children[5] = &expiredNode{
		blobOffset:     1024,
		nodeFileOffset: 1024 + 256,
		size:           48,
		hash:           expiredHash,
	}
	fn.Children[16] = valueNode(value)

	// Encode as hybrid bytes.
	hybrid := assembleHybridBytes(fn)
	if hybrid[0] != inactive.HybridMarker {
		t.Fatalf("hybrid marker = 0x%02x, want 0x01", hybrid[0])
	}

	// Decode and verify the round-trip.
	parentHash := computeStandardHash(t, fn)
	decoded, err := decodeHybrid(parentHash[:], hybrid)
	if err != nil {
		t.Fatalf("decodeHybrid: %v", err)
	}
	dn, ok := decoded.(*fullNode)
	if !ok {
		t.Fatalf("decoded type = %T, want *fullNode", decoded)
	}
	// Children[2] should be a regular hashNode.
	hn, ok := dn.Children[2].(hashNode)
	if !ok {
		t.Errorf("Children[2] type = %T, want hashNode", dn.Children[2])
	} else if !bytes.Equal(hn, regularHash.Bytes()) {
		t.Errorf("Children[2] hash = %x, want %x", []byte(hn), regularHash.Bytes())
	}
	// Children[5] should be an *expiredNode with the same metadata we put in.
	en, ok := dn.Children[5].(*expiredNode)
	if !ok {
		t.Fatalf("Children[5] type = %T, want *expiredNode", dn.Children[5])
	}
	if en.blobOffset != 1024 || en.nodeFileOffset != 1024+256 || en.size != 48 {
		t.Errorf("Children[5] = %+v, want blobOffset=1024 nodeFileOffset=1280 size=48", en)
	}
	if en.hash != expiredHash {
		t.Errorf("Children[5].hash = %x, want %x", en.hash, expiredHash)
	}
	// Children[16] should be the valueNode.
	vn, ok := dn.Children[16].(valueNode)
	if !ok {
		t.Errorf("Children[16] type = %T, want valueNode", dn.Children[16])
	} else if !bytes.Equal(vn, value) {
		t.Errorf("Children[16] value = %x, want %x", []byte(vn), value)
	}
}

// TestHybridShortNodeRoundTrip: build a *shortNode whose Val is an
// *expiredNode. Encode → decode → verify.
func TestHybridShortNodeRoundTrip(t *testing.T) {
	expiredHash := common.HexToHash("0xcafebabe")
	sn := &shortNode{
		Key:   []byte{0x1, 0x2, 0x3}, // hex (no terminator → extension)
		flags: nodeFlag{dirty: true},
	}
	sn.Val = &expiredNode{
		blobOffset:     500,
		nodeFileOffset: 500 + 100,
		size:           64,
		hash:           expiredHash,
	}

	hybrid := assembleHybridBytes(sn)
	if hybrid[0] != inactive.HybridMarker {
		t.Fatalf("hybrid marker = 0x%02x, want 0x01", hybrid[0])
	}

	parentHash := computeStandardHash(t, sn)
	decoded, err := decodeHybrid(parentHash[:], hybrid)
	if err != nil {
		t.Fatalf("decodeHybrid: %v", err)
	}
	dn, ok := decoded.(*shortNode)
	if !ok {
		t.Fatalf("decoded type = %T, want *shortNode", decoded)
	}
	en, ok := dn.Val.(*expiredNode)
	if !ok {
		t.Fatalf("shortNode.Val type = %T, want *expiredNode", dn.Val)
	}
	if en.blobOffset != 500 || en.nodeFileOffset != 600 || en.size != 64 {
		t.Errorf("Val = %+v, want blobOffset=500 nodeFileOffset=600 size=64", en)
	}
	if en.hash != expiredHash {
		t.Errorf("Val.hash = %x, want %x", en.hash, expiredHash)
	}
}

// TestHybridHashInvariance: the hybrid bytes' standard-RLP component is
// byte-identical to what the original (un-stubbed) parent would produce —
// i.e., expiredNode.encode() emits the original hashNode bytes.
func TestHybridHashInvariance(t *testing.T) {
	expiredHash := common.HexToHash("0xfeedface")

	// Reference parent: same shape but with a regular hashNode in place of
	// the *expiredNode.
	original := &fullNode{flags: nodeFlag{dirty: true}}
	original.Children[3] = hashNode(common.HexToHash("0x11").Bytes())
	original.Children[7] = hashNode(expiredHash.Bytes())

	hybridForm := &fullNode{flags: nodeFlag{dirty: true}}
	hybridForm.Children[3] = hashNode(common.HexToHash("0x11").Bytes())
	hybridForm.Children[7] = &expiredNode{
		blobOffset:     2048,
		nodeFileOffset: 2048 + 64,
		size:           96,
		hash:           expiredHash,
	}

	// Standard RLPs should be byte-identical.
	originalRLP := nodeToBytes(original)
	hybridStdRLP := nodeToBytes(hybridForm)
	if !bytes.Equal(originalRLP, hybridStdRLP) {
		t.Fatalf("hybrid form's standard RLP differs from original\n  original: %x\n  hybrid:   %x",
			originalRLP, hybridStdRLP)
	}
	// Therefore their keccak hashes also match.
	if crypto.Keccak256Hash(originalRLP) != crypto.Keccak256Hash(hybridStdRLP) {
		t.Fatalf("hash mismatch despite equal RLP")
	}
}

// TestDecodeHybridRejectsMalformed: malformed hybrid inputs return errors
// rather than panicking.
func TestDecodeHybridRejectsMalformed(t *testing.T) {
	cases := []struct {
		name string
		buf  []byte
	}{
		{"empty", []byte{}},
		{"wrong marker", []byte{0xc0, 0x80}},
		{"missing metadata", []byte{0x01, 0xc0}},                // marker + empty RLP list, no metadata
		{"truncated metadata header", []byte{0x01, 0xc0, 0x01}}, // stubCount=1 but no blobOffset
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := decodeHybrid(make([]byte, 32), c.buf); err == nil {
				t.Errorf("expected error, got nil")
			}
		})
	}
}

// computeStandardHash returns keccak256(standard RLP of n).
func computeStandardHash(t *testing.T, n node) common.Hash {
	t.Helper()
	return crypto.Keccak256Hash(nodeToBytes(n))
}

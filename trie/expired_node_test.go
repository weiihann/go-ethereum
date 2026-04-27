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
	"github.com/ethereum/go-ethereum/rlp"
)

func TestEncodeStubRoundTrip(t *testing.T) {
	cases := []struct {
		blobOffset uint64
		rootInBlob uint32
		rootSize   uint32
	}{
		{0, 16, 1},
		{16, 16, 64},
		{1 << 32, 1 << 16, 1024},
		{^uint64(0) - 1024, ^uint32(0), ^uint32(0)},
	}
	hash := common.HexToHash("0x" +
		"deadbeefdeadbeefdeadbeefdeadbeef" +
		"deadbeefdeadbeefdeadbeefdeadbeef")
	for _, tc := range cases {
		stub := EncodeStub(tc.blobOffset, tc.rootInBlob, tc.rootSize)
		if len(stub) != expiredNodeStubLen {
			t.Errorf("stub length = %d, want %d", len(stub), expiredNodeStubLen)
			continue
		}
		if stub[0] != expiredNodeMarker {
			t.Errorf("stub marker = 0x%x, want 0x%x", stub[0], expiredNodeMarker)
		}
		n, err := decodeStub(hash.Bytes(), stub)
		if err != nil {
			t.Errorf("decodeStub: %v", err)
			continue
		}
		if n.blobOffset != tc.blobOffset {
			t.Errorf("blobOffset: got %d, want %d", n.blobOffset, tc.blobOffset)
		}
		wantNodeFileOffset := tc.blobOffset + uint64(tc.rootInBlob)
		if n.nodeFileOffset != wantNodeFileOffset {
			t.Errorf("nodeFileOffset: got %d, want %d", n.nodeFileOffset, wantNodeFileOffset)
		}
		if n.size != tc.rootSize {
			t.Errorf("size: got %d, want %d", n.size, tc.rootSize)
		}
		if n.hash != hash {
			t.Errorf("hash: got %x, want %x", n.hash, hash)
		}
	}
}

// TestDecodeNodeUnsafeRecognisesStub: feeding decodeNodeUnsafe a stub byte
// sequence yields an *expiredNode. Other RLP-encoded trie nodes still parse
// to their respective types.
func TestDecodeNodeUnsafeRecognisesStub(t *testing.T) {
	hash := common.HexToHash("0x01").Bytes()
	stub := EncodeStub(42, 16, 1024)

	n, err := decodeNodeUnsafe(hash, stub)
	if err != nil {
		t.Fatalf("decodeNodeUnsafe(stub): %v", err)
	}
	en, ok := n.(*expiredNode)
	if !ok {
		t.Fatalf("decoded node type = %T, want *expiredNode", n)
	}
	if en.blobOffset != 42 || en.size != 1024 {
		t.Errorf("expiredNode = %+v", en)
	}
}

// TestExpiredNodeCacheReturnsHash: cache() returns the original subtree hash
// and reports clean (dirty=false). This is what allows the committer to
// skip an unmodified inactive subtree without re-encoding.
func TestExpiredNodeCacheReturnsHash(t *testing.T) {
	hash := common.HexToHash("0xabcd")
	en := &expiredNode{blobOffset: 0, nodeFileOffset: 16, size: 17, hash: hash}
	gotHash, dirty := en.cache()
	if dirty {
		t.Errorf("cache() reports dirty; want clean")
	}
	if !bytes.Equal(gotHash, hash.Bytes()) {
		t.Errorf("cache hash = %x, want %x", gotHash, hash.Bytes())
	}
}

// TestExpiredNodeEncodeEmitsHash: encode() must write the original 32-byte
// hashNode RLP element so the parent's standard RLP is byte-identical to
// its pre-conversion form. (The (offset, size) info travels separately via
// hybrid metadata appended by the storage encoder.)
func TestExpiredNodeEncodeEmitsHash(t *testing.T) {
	hash := common.HexToHash("0x" +
		"112233445566778899aabbccddeeff00" +
		"112233445566778899aabbccddeeff00")
	en := &expiredNode{blobOffset: 0, nodeFileOffset: 16, size: 32, hash: hash}

	var buf bytes.Buffer
	w := rlp.NewEncoderBuffer(&buf)
	en.encode(w)
	if err := w.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	out := buf.Bytes()

	// RLP byte-string of 32 bytes is `0xa0 || 32-byte payload` (33 bytes total).
	if len(out) != 33 {
		t.Fatalf("encoded length = %d, want 33", len(out))
	}
	if out[0] != 0xa0 {
		t.Errorf("encoded prefix = 0x%02x, want 0xa0", out[0])
	}
	if !bytes.Equal(out[1:], hash.Bytes()) {
		t.Errorf("encoded payload = %x, want %x", out[1:], hash.Bytes())
	}
}

// TestIsStub correctly identifies stubs vs other byte sequences.
func TestIsStub(t *testing.T) {
	if !IsStub(EncodeStub(1, 16, 1)) {
		t.Errorf("IsStub on stub = false")
	}
	if IsStub(nil) {
		t.Errorf("IsStub(nil) = true")
	}
	// RLP list — first byte 0xc0+. Definitely not a stub.
	if IsStub([]byte{0xc0, 0x80}) {
		t.Errorf("IsStub on RLP list = true")
	}
	// Hybrid marker (0x01) is NOT a primary stub.
	if IsStub([]byte{0x01, 0x02}) {
		t.Errorf("IsStub on hybrid bytes = true")
	}
}

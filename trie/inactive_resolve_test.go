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
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/triedb/inactive"
)

// buildSubtree returns a small in-memory trie root to feed the encoder. The
// trie holds a few key/value pairs at known keys.
func buildSubtree(t *testing.T) (root node, kv map[string]string) {
	t.Helper()
	tr := NewEmpty(newTestDatabase(rawdb.NewMemoryDatabase(), rawdb.HashScheme))
	kv = map[string]string{
		"alpha":   "AAA",
		"alphaXX": "AXAXAX",
		"beta":    "BBB",
		"gamma":   "GGG",
		"delta":   "DDD",
	}
	for k, v := range kv {
		if err := tr.Update([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Update(%q): %v", k, err)
		}
	}
	// Force hashing/structuring.
	_ = tr.Hash()
	return tr.root, kv
}

// blobReaderFor returns a reader closure that serves bytes from the supplied
// in-memory blob (treating fileOffset 0 as the blob start). Used by tests
// to drive the per-step navigator without an actual inactive file.
func blobReaderFor(blob []byte) ArchiveResolverFn {
	return func(offset, size uint64) ([]byte, error) {
		end := offset + size
		if end > uint64(len(blob)) {
			return nil, errors.New("test reader: out of range")
		}
		out := make([]byte, size)
		copy(out, blob[offset:end])
		return out, nil
	}
}

// TestEncodeNavigateRoundTrip: encode a real trie subtree, navigate every
// known key, expect the original values back.
func TestEncodeNavigateRoundTrip(t *testing.T) {
	root, kv := buildSubtree(t)
	blob, err := EncodeInactiveBlob(root)
	if err != nil {
		t.Fatalf("EncodeInactiveBlob: %v", err)
	}
	if len(blob) <= inactive.HeaderSize {
		t.Fatalf("blob suspiciously small: %d bytes", len(blob))
	}
	for k, v := range kv {
		got, err := inactive.NavigateBlob(blob, []byte(k))
		if err != nil {
			t.Errorf("NavigateBlob(%q): %v", k, err)
			continue
		}
		if !bytes.Equal(got, []byte(v)) {
			t.Errorf("NavigateBlob(%q) = %q, want %q", k, got, v)
		}
	}
	// A key that's not in the trie returns ErrNotFound.
	if _, err := inactive.NavigateBlob(blob, []byte("missing")); !errors.Is(err, inactive.ErrNotFound) {
		t.Errorf("NavigateBlob(missing): want ErrNotFound, got %v", err)
	}
}

// TestEncodeNavigateViaTrie: encode then mount the blob behind an
// *expiredNode in a trie. Get queries traverse via the per-step reader.
func TestEncodeNavigateViaTrie(t *testing.T) {
	root, kv := buildSubtree(t)
	blob, err := EncodeInactiveBlob(root)
	if err != nil {
		t.Fatalf("EncodeInactiveBlob: %v", err)
	}

	hdr, err := inactive.ParseHeader(blob)
	if err != nil {
		t.Fatalf("ParseHeader: %v", err)
	}

	tr := NewEmpty(newTestDatabase(rawdb.NewMemoryDatabase(), rawdb.HashScheme))
	tr.SetArchiveResolver(blobReaderFor(blob))
	tr.root = &expiredNode{
		blobOffset:     0,
		nodeFileOffset: uint64(hdr.RootOffset),
		size:           hdr.RootSize,
		hash:           common.HexToHash("0xdead"),
	}

	for k, v := range kv {
		got, err := tr.Get([]byte(k))
		if err != nil {
			t.Errorf("Get(%q): %v", k, err)
			continue
		}
		if !bytes.Equal(got, []byte(v)) {
			t.Errorf("Get(%q) = %q, want %q", k, got, v)
		}
	}
}

// TestInsertOverExpiredNode: Update a key that lives inside an inactive
// subtree. The trie lazy-materialises the path, modifies, and the new value
// is observable via Get on the resulting partial subtree.
func TestInsertOverExpiredNode(t *testing.T) {
	root, kv := buildSubtree(t)
	blob, err := EncodeInactiveBlob(root)
	if err != nil {
		t.Fatalf("EncodeInactiveBlob: %v", err)
	}

	hdr, _ := inactive.ParseHeader(blob)

	tr := NewEmpty(newTestDatabase(rawdb.NewMemoryDatabase(), rawdb.HashScheme))
	tr.SetArchiveResolver(blobReaderFor(blob))
	tr.root = &expiredNode{
		blobOffset:     0,
		nodeFileOffset: uint64(hdr.RootOffset),
		size:           hdr.RootSize,
		hash:           common.HexToHash("0xdead"),
	}

	// Modify "alpha" → "NEW".
	if err := tr.Update([]byte("alpha"), []byte("NEW")); err != nil {
		t.Fatalf("Update: %v", err)
	}

	// "alpha" should now read "NEW".
	got, err := tr.Get([]byte("alpha"))
	if err != nil {
		t.Fatalf("Get(alpha) after Update: %v", err)
	}
	if !bytes.Equal(got, []byte("NEW")) {
		t.Errorf("Get(alpha) = %q, want NEW", got)
	}

	// Other keys unchanged. Reads off the modification path go through the
	// remaining *expiredNode children back into the blob.
	for k, v := range kv {
		if k == "alpha" {
			continue
		}
		got, err := tr.Get([]byte(k))
		if err != nil {
			t.Errorf("Get(%q): %v", k, err)
			continue
		}
		if !bytes.Equal(got, []byte(v)) {
			t.Errorf("Get(%q) = %q, want %q", k, got, v)
		}
	}
}

// TestNavigateInactiveNoResolverErrors: when an *expiredNode is reached but
// the trie has no resolver, the read returns an error rather than panicking
// or returning empty.
func TestNavigateInactiveNoResolverErrors(t *testing.T) {
	tr := NewEmpty(newTestDatabase(rawdb.NewMemoryDatabase(), rawdb.HashScheme))
	tr.root = &expiredNode{blobOffset: 0, nodeFileOffset: 16, size: 17, hash: common.HexToHash("0x01")}
	_, err := tr.Get([]byte("anything"))
	if err == nil || !contains(err.Error(), "no archive resolver attached") {
		t.Errorf("expected 'no archive resolver' error, got %v", err)
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && bytes.Contains([]byte(haystack), []byte(needle))
}

// TestEncodeRejectsHashNode: encoding a tree that still has unresolved
// hashNodes returns a clear error so callers know they must materialise the
// subtree fully before encoding.
func TestEncodeRejectsHashNode(t *testing.T) {
	root := &fullNode{}
	root.Children[1] = hashNode(make([]byte, 32))
	if _, err := EncodeInactiveBlob(root); err == nil {
		t.Errorf("expected error for hashNode in subtree, got nil")
	} else if !contains(err.Error(), "unresolved hashNode") {
		t.Errorf("error message %q lacks 'unresolved hashNode'", err.Error())
	}
}

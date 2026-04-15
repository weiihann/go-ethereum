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
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/holiman/uint256"
)

// TestSlimAccountEIP8188 exercises the EIP-8188 optional LastWrittenPeriod
// field: legacy 4-tuple input decodes with period=0, new 5-tuple input
// round-trips, and zero-period encode is byte-identical to legacy encode.
func TestSlimAccountEIP8188(t *testing.T) {
	// Build a 4-element RLP list identical to what pre-EIP-8188 geth writes.
	legacyBlob, err := rlp.EncodeToBytes([]any{
		uint64(42),
		uint256.NewInt(1000),
		[]byte(nil), // empty Root → EmptyRootHash
		[]byte(nil), // empty CodeHash → EmptyCodeHash
	})
	if err != nil {
		t.Fatalf("encode legacy: %v", err)
	}

	var got SlimAccount
	if err := rlp.DecodeBytes(legacyBlob, &got); err != nil {
		t.Fatalf("decode legacy 4-tuple into 5-field SlimAccount: %v", err)
	}
	if got.Nonce != 42 {
		t.Errorf("legacy nonce: got %d, want 42", got.Nonce)
	}
	if got.Balance.Uint64() != 1000 {
		t.Errorf("legacy balance: got %s, want 1000", got.Balance)
	}
	if got.LastWrittenPeriod != 0 {
		t.Errorf("legacy period: got %d, want 0", got.LastWrittenPeriod)
	}

	// New 5-tuple round-trip.
	newAccount := SlimAccount{
		Nonce:             42,
		Balance:           uint256.NewInt(1000),
		LastWrittenPeriod: 7,
	}
	newBlob, err := rlp.EncodeToBytes(newAccount)
	if err != nil {
		t.Fatalf("encode new: %v", err)
	}
	var roundTrip SlimAccount
	if err := rlp.DecodeBytes(newBlob, &roundTrip); err != nil {
		t.Fatalf("decode new 5-tuple: %v", err)
	}
	if roundTrip.LastWrittenPeriod != 7 {
		t.Errorf("round-trip period: got %d, want 7", roundTrip.LastWrittenPeriod)
	}

	// Zero-period encode must match legacy encode byte-for-byte.
	zeroPeriod := SlimAccount{
		Nonce:             42,
		Balance:           uint256.NewInt(1000),
		LastWrittenPeriod: 0,
	}
	zeroBlob, err := rlp.EncodeToBytes(zeroPeriod)
	if err != nil {
		t.Fatalf("encode zero period: %v", err)
	}
	if !bytes.Equal(zeroBlob, legacyBlob) {
		t.Errorf("zero-period encode diverges from legacy:\n  got  %x\n  want %x",
			zeroBlob, legacyBlob)
	}
}

// TestSlimAccountRLPWithPeriod covers the helper that injects a period while
// encoding a StateAccount. It must agree with SlimAccountRLP when period=0.
func TestSlimAccountRLPWithPeriod(t *testing.T) {
	acc := StateAccount{
		Nonce:    5,
		Balance:  uint256.NewInt(999),
		Root:     EmptyRootHash,
		CodeHash: EmptyCodeHash[:],
	}

	legacy := SlimAccountRLP(acc)
	zeroP := SlimAccountRLPWithPeriod(acc, 0)
	if !bytes.Equal(legacy, zeroP) {
		t.Errorf("zero-period helper diverges from legacy helper:\n  got  %x\n  want %x",
			zeroP, legacy)
	}

	withP := SlimAccountRLPWithPeriod(acc, 123)
	var decoded SlimAccount
	if err := rlp.DecodeBytes(withP, &decoded); err != nil {
		t.Fatalf("decode period-tagged: %v", err)
	}
	if decoded.LastWrittenPeriod != 123 {
		t.Errorf("period: got %d, want 123", decoded.LastWrittenPeriod)
	}
}

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
)

func TestStorageSnapshotValueRoundTrip(t *testing.T) {
	cases := []struct {
		name   string
		value  []byte
		period uint32
	}{
		{"single byte", []byte{0x2a}, 5},
		{"32-byte full value", bytes.Repeat([]byte{0xab}, 32), 1<<24 - 1},
		{"short value", []byte{0xde, 0xad, 0xbe, 0xef}, 123},
		{"zero-period with value", []byte{0x01}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			blob := EncodeStorageSnapshotValue(tc.value, tc.period)
			gotValue, gotPeriod, err := DecodeStorageSnapshotValue(blob)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !bytes.Equal(gotValue, tc.value) {
				t.Errorf("value: got %x, want %x", gotValue, tc.value)
			}
			if gotPeriod != tc.period {
				t.Errorf("period: got %d, want %d", gotPeriod, tc.period)
			}
		})
	}
}

// TestStorageSnapshotValueLegacyDecode proves that bytes produced by pre-EIP-8188
// geth (plain RLP byte strings) decode with period=0.
func TestStorageSnapshotValueLegacyDecode(t *testing.T) {
	raw := []byte{0xde, 0xad, 0xbe, 0xef}
	legacy, err := rlp.EncodeToBytes(raw)
	if err != nil {
		t.Fatalf("encode legacy: %v", err)
	}
	if legacy[0] >= 0xc0 {
		t.Fatalf("legacy first byte 0x%02x must be < 0xc0", legacy[0])
	}
	value, period, err := DecodeStorageSnapshotValue(legacy)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !bytes.Equal(value, raw) {
		t.Errorf("value: got %x, want %x", value, raw)
	}
	if period != 0 {
		t.Errorf("period: got %d, want 0", period)
	}
}

// TestStorageSnapshotZeroPeriodMatchesLegacy ensures records with period=0
// encode byte-for-byte identically to the pre-EIP-8188 output, so injection
// of a zero period doesn't rewrite the disk layout.
func TestStorageSnapshotZeroPeriodMatchesLegacy(t *testing.T) {
	value := []byte{0x01, 0x02, 0x03}
	legacy, err := rlp.EncodeToBytes(value)
	if err != nil {
		t.Fatalf("encode legacy: %v", err)
	}
	zeroP := EncodeStorageSnapshotValue(value, 0)
	if !bytes.Equal(legacy, zeroP) {
		t.Errorf("zero period diverges from legacy:\n  got  %x\n  want %x", zeroP, legacy)
	}
}

// TestStorageSnapshotValueEmpty ensures the deleted/absent case is handled.
func TestStorageSnapshotValueEmpty(t *testing.T) {
	value, period, err := DecodeStorageSnapshotValue(nil)
	if err != nil {
		t.Fatalf("decode empty: %v", err)
	}
	if value != nil || period != 0 {
		t.Errorf("empty input: got (%x, %d), want (nil, 0)", value, period)
	}
}

// TestStorageSnapshotFirstByteInvariant nails the invariant the decoder
// relies on: the legacy encoding NEVER produces a first byte in the list
// prefix range. If this invariant ever breaks (e.g. geth changes its storage
// snapshot encoding), the fuzz-like scan below fails loudly.
func TestStorageSnapshotFirstByteInvariant(t *testing.T) {
	// Every slot value length 0-32 with every possible first-byte value.
	for length := 0; length <= 32; length++ {
		for first := 0; first < 256; first++ {
			buf := make([]byte, length)
			if length > 0 {
				buf[0] = byte(first)
				for i := 1; i < length; i++ {
					buf[i] = 0x5a
				}
			}
			encoded, err := rlp.EncodeToBytes(buf)
			if err != nil {
				t.Fatalf("encode len=%d first=%#x: %v", length, first, err)
			}
			if len(encoded) > 0 && encoded[0] >= 0xc0 {
				t.Fatalf("legacy encoding produced list prefix 0x%02x for len=%d first=0x%02x",
					encoded[0], length, first)
			}
		}
	}
}

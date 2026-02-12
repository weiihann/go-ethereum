package core

import (
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTerminatorIsZero(t *testing.T) {
	var zero [32]byte
	assert.Equal(t, zero, Terminator)
}

func TestNodeKindOf(t *testing.T) {
	tests := []struct {
		name string
		node Node
		want NodeKind
	}{
		{
			name: "terminator",
			node: Terminator,
			want: NodeTerminator,
		},
		{
			name: "non-zero hash is internal",
			node: Node{0x80, 0x01, 0x02},
			want: NodeInternal,
		},
		{
			name: "any non-zero is internal",
			node: Node{0x01, 0x02, 0x03},
			want: NodeInternal,
		},
		{
			name: "high bits set is still internal",
			node: Node{0xFF, 0xFF, 0xFF},
			want: NodeInternal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NodeKindOf(&tt.node)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIsTerminator(t *testing.T) {
	assert.True(t, IsTerminator(&Terminator))

	nonZero := Node{0x01}
	assert.False(t, IsTerminator(&nonZero))
}

func TestHashInternalDeterministic(t *testing.T) {
	data := &InternalData{
		Left:  Node{0x11, 0x22},
		Right: Node{0x33, 0x44},
	}
	h1 := HashInternal(data)
	h2 := HashInternal(data)
	assert.Equal(t, h1, h2, "same inputs must produce same hash")
}

func TestHashInternalMatchesSHA256(t *testing.T) {
	left := Node{0x01, 0x02, 0x03}
	right := Node{0x04, 0x05, 0x06}

	data := &InternalData{Left: left, Right: right}
	got := HashInternal(data)

	// Manual SHA256(left || right)
	h := sha256.New()
	h.Write(left[:])
	h.Write(right[:])
	expected := h.Sum(nil)

	assert.Equal(t, expected, got[:])
}

func TestHashInternalNoMSBTagging(t *testing.T) {
	// With SHA256, the MSB is determined by the hash output, not forced.
	// Just verify it produces a non-zero, non-terminator result.
	data := &InternalData{
		Left:  Node{0xFF, 0x01},
		Right: Node{0x80, 0x02},
	}
	result := HashInternal(data)
	require.False(t, IsTerminator(&result))
}

func TestHashStemDeterministic(t *testing.T) {
	var stem StemPath
	stem[0] = 0xAB
	stem[1] = 0xCD

	var values [StemNodeWidth][]byte
	values[0] = make([]byte, 32)
	values[0][0] = 0x01

	h1 := HashStem(stem, values)
	h2 := HashStem(stem, values)
	assert.Equal(t, h1, h2, "same inputs must produce same hash")
}

func TestHashStemAllNilIsNotZero(t *testing.T) {
	// Even with all nil values, the stem hash includes the stem bytes,
	// so it should NOT be the zero hash (unless stem is also zero and
	// subtree root is zero... let's check).
	var stem StemPath
	var values [StemNodeWidth][]byte

	// With all-zero stem and all-nil values, the subtree root is zero.
	// Final = SHA256(zero_stem || 0x00 || zero_hash)
	result := HashStem(stem, values)
	assert.False(t, IsTerminator(&result),
		"stem hash should not be terminator even with empty values")
}

func TestHashStemSingleValue(t *testing.T) {
	var stem StemPath
	stem[0] = 0x42

	var values [StemNodeWidth][]byte
	val := make([]byte, 32)
	val[0] = 0xFF
	values[0] = val

	result := HashStem(stem, values)
	assert.False(t, IsTerminator(&result))
}

func TestStemPathType(t *testing.T) {
	// Verify StemPath is 31 bytes.
	var sp StemPath
	assert.Equal(t, StemSize, len(sp))
	assert.Equal(t, 31, len(sp))
}

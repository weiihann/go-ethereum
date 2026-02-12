package core

import (
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
			name: "leaf with MSB set",
			node: Node{0x80, 0x01, 0x02},
			want: NodeLeaf,
		},
		{
			name: "leaf with all bits set in first byte",
			node: Node{0xFF, 0x01},
			want: NodeLeaf,
		},
		{
			name: "internal node",
			node: Node{0x01, 0x02, 0x03},
			want: NodeInternal,
		},
		{
			name: "internal with MSB clear",
			node: Node{0x7F, 0xFF, 0xFF},
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

func TestIsLeaf(t *testing.T) {
	leaf := Node{0x80}
	assert.True(t, IsLeaf(&leaf))

	internal := Node{0x7F, 0xFF}
	assert.False(t, IsLeaf(&internal))
}

func TestIsInternal(t *testing.T) {
	internal := Node{0x01}
	assert.True(t, IsInternal(&internal))

	leaf := Node{0x80}
	assert.False(t, IsInternal(&leaf))

	assert.False(t, IsInternal(&Terminator))
}

func TestHashLeafSetsMSB(t *testing.T) {
	data := &LeafData{
		KeyPath:   KeyPath{0x01, 0x02, 0x03},
		ValueHash: ValueHash{0x04, 0x05, 0x06},
	}
	result := HashLeaf(data)
	require.True(t, IsLeaf(&result), "HashLeaf must produce a leaf node")
	require.False(t, IsTerminator(&result))
}

func TestHashInternalClearsMSB(t *testing.T) {
	data := &InternalData{
		Left:  Node{0xFF, 0x01},
		Right: Node{0x80, 0x02},
	}
	result := HashInternal(data)
	require.True(t, IsInternal(&result),
		"HashInternal must produce an internal node")
	require.False(t, IsLeaf(&result))
}

func TestHashLeafDeterministic(t *testing.T) {
	data := &LeafData{
		KeyPath:   KeyPath{0xAB, 0xCD},
		ValueHash: ValueHash{0xEF, 0x01},
	}
	h1 := HashLeaf(data)
	h2 := HashLeaf(data)
	assert.Equal(t, h1, h2, "same inputs must produce same hash")
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

func TestHashLeafDiffersFromInternal(t *testing.T) {
	// Using the same 64-byte preimage for both should produce different
	// hashes due to MSB tagging (even if the raw keccak is the same,
	// the MSB bit will differ).
	var key KeyPath
	var val ValueHash
	for i := range key {
		key[i] = byte(i)
	}
	for i := range val {
		val[i] = byte(i + 32)
	}

	leaf := HashLeaf(&LeafData{KeyPath: key, ValueHash: val})
	internal := HashInternal(&InternalData{Left: Node(key), Right: Node(val)})

	// They share the same keccak input, but MSB tagging makes them differ.
	assert.NotEqual(t, leaf, internal,
		"leaf and internal hashes must differ due to MSB tagging")
}

func TestHashValue(t *testing.T) {
	v1 := HashValue([]byte("hello"))
	v2 := HashValue([]byte("hello"))
	v3 := HashValue([]byte("world"))

	assert.Equal(t, v1, v2, "same value must produce same hash")
	assert.NotEqual(t, v1, v3, "different values must differ")
}

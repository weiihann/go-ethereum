package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRootPageIDEncodeDecode(t *testing.T) {
	root := RootPageID()
	encoded := root.Encode()
	assert.Equal(t, [32]byte{}, encoded, "root encodes to all zeros")

	decoded, err := DecodePageID(encoded)
	require.NoError(t, err)
	assert.True(t, decoded.IsRoot())
	assert.Equal(t, 0, decoded.Depth())
}

func TestPageIDEncodeDecodeRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		path []uint8
	}{
		{"root", nil},
		{"child 0", []uint8{0}},
		{"child 6", []uint8{6}},
		{"child 63", []uint8{63}},
		{"depth 2", []uint8{6, 4}},
		{"depth 3", []uint8{6, 4, 63}},
		{"depth 9 (u64 boundary)", []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9}},
		{"depth 10 (big.Int)", []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
		{"all zeros depth 5", []uint8{0, 0, 0, 0, 0}},
		{"all 63s depth 5", []uint8{63, 63, 63, 63, 63}},
		{"mixed deep", []uint8{0, 63, 0, 63, 0, 63, 0, 63, 0, 63, 0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := NewPageID(tt.path)
			encoded := id.Encode()
			decoded, err := DecodePageID(encoded)
			require.NoError(t, err)
			assert.True(t, id.Equal(decoded),
				"path=%v decoded=%v encoded=%x",
				tt.path, decoded.Path(), encoded)
		})
	}
}

func TestPageIDKnownValues(t *testing.T) {
	// Shift-then-add encoding:
	// encode([6]) = 0*64 + (6+1) = 7
	id1 := NewPageID([]uint8{6})
	enc1 := id1.Encode()
	assert.Equal(t, byte(7), enc1[31])
	for i := 0; i < 31; i++ {
		assert.Equal(t, byte(0), enc1[i])
	}

	// encode([6, 4]) = 7*64 + (4+1) = 448 + 5 = 453 = 0x01C5
	id2 := NewPageID([]uint8{6, 4})
	enc2 := id2.Encode()
	assert.Equal(t, byte(0x01), enc2[30])
	assert.Equal(t, byte(0xC5), enc2[31])

	// encode([6, 4, 63]) = 453*64 + (63+1) = 28992 + 64 = 29056 = 0x7180
	id3 := NewPageID([]uint8{6, 4, 63})
	enc3 := id3.Encode()
	assert.Equal(t, byte(0x71), enc3[30])
	assert.Equal(t, byte(0x80), enc3[31])
}

func TestChildAndParentPageID(t *testing.T) {
	root := RootPageID()

	page1, err := root.ChildPageID(6)
	require.NoError(t, err)
	assert.Equal(t, []uint8{6}, page1.Path())
	assert.True(t, page1.ParentPageID().Equal(root))

	page2, err := page1.ChildPageID(4)
	require.NoError(t, err)
	assert.Equal(t, []uint8{6, 4}, page2.Path())
	assert.True(t, page2.ParentPageID().Equal(page1))

	page3, err := page2.ChildPageID(63)
	require.NoError(t, err)
	assert.Equal(t, []uint8{6, 4, 63}, page3.Path())
	assert.True(t, page3.ParentPageID().Equal(page2))

	// Verify encode/decode matches child construction.
	decoded1, err := DecodePageID(page1.Encode())
	require.NoError(t, err)
	assert.True(t, page1.Equal(decoded1))

	decoded2, err := DecodePageID(page2.Encode())
	require.NoError(t, err)
	assert.True(t, page2.Equal(decoded2))
}

func TestPageIDOverflow(t *testing.T) {
	current := RootPageID()
	for range MaxPageDepth {
		var err error
		current, err = current.ChildPageID(0)
		require.NoError(t, err)
	}
	assert.Equal(t, MaxPageDepth, current.Depth())

	_, err := current.ChildPageID(0)
	assert.ErrorIs(t, err, ErrPageIDOverflow)
}

func TestPageIDOverflowMaxChild(t *testing.T) {
	current := RootPageID()
	for range MaxPageDepth {
		var err error
		current, err = current.ChildPageID(63)
		require.NoError(t, err)
	}
	_, err := current.ChildPageID(0)
	assert.ErrorIs(t, err, ErrPageIDOverflow)
}

func TestInvalidPageIDBytes(t *testing.T) {
	var bytes [32]byte
	bytes[0] = 128 // bit 255 set
	_, err := DecodePageID(bytes)
	assert.ErrorIs(t, err, ErrInvalidPageIDBytes)
}

func TestPageIDSiblingOrdering(t *testing.T) {
	root := RootPageID()
	var lastEnc [32]byte
	for i := range uint8(NumChildren) {
		child, err := root.ChildPageID(i)
		require.NoError(t, err)
		enc := child.Encode()

		assert.NotEqual(t, [32]byte{}, enc)

		if i > 0 {
			assert.True(t, compareBE(enc, lastEnc) > 0,
				"child %d should sort after child %d", i, i-1)
		}
		lastEnc = enc
	}
}

func TestPageIDIsDescendantOf(t *testing.T) {
	root := RootPageID()
	child, _ := root.ChildPageID(5)
	grandchild, _ := child.ChildPageID(10)

	assert.True(t, child.IsDescendantOf(root))
	assert.True(t, grandchild.IsDescendantOf(root))
	assert.True(t, grandchild.IsDescendantOf(child))
	assert.False(t, root.IsDescendantOf(child))
	assert.True(t, root.IsDescendantOf(root))
}

func TestRootMinMaxKeyPath(t *testing.T) {
	root := RootPageID()
	assert.Equal(t, [32]byte{}, root.MinKeyPath())

	var allOnes [32]byte
	for i := range allOnes {
		allOnes[i] = 0xFF
	}
	assert.Equal(t, allOnes, root.MaxKeyPath())
}

func TestPageMinMaxKeyPath(t *testing.T) {
	root := RootPageID()

	// Child 0: first 6 bits are 000000, so min key is all zeros.
	minPage, _ := root.ChildPageID(0)
	assert.Equal(t, [32]byte{}, minPage.MinKeyPath())

	// Child 63: first 6 bits are 111111 → 0xFC in first byte.
	maxPage, _ := root.ChildPageID(63)
	minKey := maxPage.MinKeyPath()
	assert.Equal(t, byte(0xFC), minKey[0])
	for i := 1; i < 32; i++ {
		assert.Equal(t, byte(0), minKey[i])
	}

	// Child 0: max key has 000000 prefix then all ones → 0x03 then 0xFF.
	maxKey := minPage.MaxKeyPath()
	assert.Equal(t, byte(0x03), maxKey[0])
	for i := 1; i < 32; i++ {
		assert.Equal(t, byte(0xFF), maxKey[i])
	}
}

func TestPageIDsForKeyPath(t *testing.T) {
	// Key path: first 6 bits = 000001 (=1), next 6 bits = 000010 (=2)
	var keyPath KeyPath
	keyPath[0] = 0b00000100 // bits: 000001|00...
	keyPath[1] = 0b00100000 // bits: ...0010|0000...

	ids := PageIDsForKeyPath(keyPath)
	require.True(t, len(ids) >= 3)

	assert.True(t, ids[0].IsRoot())
	assert.Equal(t, []uint8{1}, ids[1].Path())
	assert.Equal(t, []uint8{1, 2}, ids[2].Path())
}

func TestMaxDepthEncodeDecodeRoundTrip(t *testing.T) {
	// Build a max-depth page with all zeros.
	path := make([]uint8, MaxPageDepth)
	id := NewPageID(path)
	enc := id.Encode()
	dec, err := DecodePageID(enc)
	require.NoError(t, err)
	assert.True(t, id.Equal(dec))

	// Build a max-depth page with all 63s.
	for i := range path {
		path[i] = 63
	}
	id = NewPageID(path)
	enc = id.Encode()
	dec, err = DecodePageID(enc)
	require.NoError(t, err)
	assert.True(t, id.Equal(dec))
}

// compareBE compares two [32]byte big-endian values.
func compareBE(a, b [32]byte) int {
	for i := range 32 {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return 0
}

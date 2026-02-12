package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPageDiffSetAndCheck(t *testing.T) {
	var d PageDiff

	d.SetChanged(0)
	assert.True(t, d.IsChanged(0))
	assert.False(t, d.IsChanged(1))

	d.SetChanged(63)
	assert.True(t, d.IsChanged(63))

	d.SetChanged(64)
	assert.True(t, d.IsChanged(64))

	d.SetChanged(125)
	assert.True(t, d.IsChanged(125))
}

func TestPageDiffCount(t *testing.T) {
	var d PageDiff
	assert.Equal(t, 0, d.Count())

	d.SetChanged(0)
	d.SetChanged(10)
	d.SetChanged(64)
	assert.Equal(t, 3, d.Count())
}

func TestPageDiffClearedFlag(t *testing.T) {
	var d PageDiff
	assert.False(t, d.IsCleared())

	d.SetCleared()
	assert.True(t, d.IsCleared())

	// Setting a changed node clears the cleared flag.
	d.SetChanged(0)
	assert.False(t, d.IsCleared())
}

func TestPageDiffJoin(t *testing.T) {
	var d1, d2 PageDiff
	d1.SetChanged(0)
	d1.SetChanged(10)
	d2.SetChanged(10)
	d2.SetChanged(64)

	joined := d1.Join(d2)
	assert.True(t, joined.IsChanged(0))
	assert.True(t, joined.IsChanged(10))
	assert.True(t, joined.IsChanged(64))
	assert.Equal(t, 3, joined.Count())
}

func TestPageDiffChangedIndices(t *testing.T) {
	var d PageDiff
	indices := []int{0, 2, 4, 63, 64, 100, 125}
	for _, idx := range indices {
		d.SetChanged(idx)
	}

	got := d.ChangedIndices()
	assert.Equal(t, indices, got)
}

func TestPageDiffEncodeDecode(t *testing.T) {
	var d PageDiff
	d.SetChanged(5)
	d.SetChanged(70)

	encoded := d.Encode()
	decoded := DecodePageDiff(encoded)
	assert.True(t, decoded.IsChanged(5))
	assert.True(t, decoded.IsChanged(70))
	assert.Equal(t, 2, decoded.Count())
}

func TestPageDiffPackUnpack(t *testing.T) {
	var page RawPage
	n5 := Node{0x05}
	n70 := Node{0x46}
	page.SetNodeAt(5, n5)
	page.SetNodeAt(70, n70)

	var d PageDiff
	d.SetChanged(5)
	d.SetChanged(70)

	packed := d.PackChangedNodes(&page)
	require.Len(t, packed, 2)
	assert.Equal(t, n5, packed[0])
	assert.Equal(t, n70, packed[1])

	// Unpack into a fresh page.
	var newPage RawPage
	d.UnpackChangedNodes(packed, &newPage)
	assert.Equal(t, n5, newPage.NodeAt(5))
	assert.Equal(t, n70, newPage.NodeAt(70))
}

func TestPageDiffAlternatingBits(t *testing.T) {
	var d PageDiff
	setIndices := make([]int, 0, 63)
	for i := 0; i < 126; i += 2 {
		d.SetChanged(i)
		setIndices = append(setIndices, i)
	}

	for _, i := range setIndices {
		assert.True(t, d.IsChanged(i), "bit %d should be set", i)
	}

	got := d.ChangedIndices()
	assert.Equal(t, setIndices, got)
}

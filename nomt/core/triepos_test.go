package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTriePositionNew(t *testing.T) {
	p := NewTriePosition()
	assert.True(t, p.IsRoot())
	assert.Equal(t, uint16(0), p.Depth())
	assert.Equal(t, 0, p.NodeIndex())
}

func TestTriePositionDown(t *testing.T) {
	p := NewTriePosition()

	// Go left: node index 0 (first in new page).
	p.Down(false)
	assert.Equal(t, uint16(1), p.Depth())
	assert.Equal(t, 0, p.NodeIndex())
	assert.False(t, p.PeekLastBit())

	// Go right: node index 0*2+2+1 = 3 (right child of node 0).
	p.Down(true)
	assert.Equal(t, uint16(2), p.Depth())
	assert.Equal(t, 3, p.NodeIndex())
	assert.True(t, p.PeekLastBit())
}

func TestTriePositionDownPageBoundary(t *testing.T) {
	p := NewTriePosition()
	// Descend 6 levels (fills one page).
	for range 6 {
		p.Down(false)
	}
	assert.Equal(t, uint16(6), p.Depth())
	// At depth 6: DepthInPage = 6 - ((6-1)/6)*6 = 6-0 = 6.
	// This is the last layer (bottom) of the first page.
	assert.Equal(t, 6, p.DepthInPage())

	// Going one more enters a new page.
	p.Down(false)
	assert.Equal(t, uint16(7), p.Depth())
	assert.Equal(t, 1, p.DepthInPage())
	assert.Equal(t, 0, p.NodeIndex()) // first node in new page
}

func TestTriePositionNodeIndex(t *testing.T) {
	// Manual verification of node_index formula.
	tests := []struct {
		name     string
		bits     []bool // bits to descend
		expected int    // expected node index
	}{
		{"left", []bool{false}, 0},
		{"right", []bool{true}, 1},
		{"left-left", []bool{false, false}, 2},
		{"left-right", []bool{false, true}, 3},
		{"right-left", []bool{true, false}, 4},
		{"right-right", []bool{true, true}, 5},
		{"3 deep all left", []bool{false, false, false}, 6},
		{"3 deep LLR", []bool{false, false, true}, 7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewTriePosition()
			for _, bit := range tt.bits {
				p.Down(bit)
			}
			assert.Equal(t, tt.expected, p.NodeIndex())
		})
	}
}

func TestTriePositionUp(t *testing.T) {
	p := NewTriePosition()
	p.Down(false) // depth 1, index 0
	p.Down(true)  // depth 2, index 3
	p.Down(false) // depth 3, index 8

	p.Up(1) // back to depth 2, index 3
	assert.Equal(t, uint16(2), p.Depth())
	assert.Equal(t, 3, p.NodeIndex())

	p.Up(2) // back to root
	assert.True(t, p.IsRoot())
}

func TestTriePositionSibling(t *testing.T) {
	p := NewTriePosition()
	p.Down(false) // left child, index 0
	assert.Equal(t, 0, p.NodeIndex())
	assert.False(t, p.PeekLastBit())

	p.Sibling() // now right child, index 1
	assert.Equal(t, 1, p.NodeIndex())
	assert.True(t, p.PeekLastBit())

	p.Sibling() // back to left
	assert.Equal(t, 0, p.NodeIndex())
}

func TestTriePositionDepthInPage(t *testing.T) {
	tests := []struct {
		depth    uint16
		expected int
	}{
		{0, 0},
		{1, 1},
		{6, 6},
		{7, 1}, // New page starts at depth 7.
		{12, 6},
		{13, 1},
	}

	for _, tt := range tests {
		p := NewTriePosition()
		for range tt.depth {
			p.Down(false)
		}
		assert.Equal(t, tt.expected, p.DepthInPage(),
			"depth=%d", tt.depth)
	}
}

func TestTriePositionPageID(t *testing.T) {
	p := NewTriePosition()
	assert.Nil(t, p.PageID(), "root has no page ID")

	// Descend 1: still in root page.
	p.Down(false)
	pageID := p.PageID()
	require.NotNil(t, pageID)
	assert.True(t, pageID.IsRoot())

	// Descend 6 more (total depth 7): now in child page.
	for range 6 {
		p.Down(false)
	}
	pageID = p.PageID()
	require.NotNil(t, pageID)
	assert.Equal(t, 1, pageID.Depth())
	assert.Equal(t, uint8(0), pageID.ChildIndexAt(0))
}

func TestTriePositionChildPageIndex(t *testing.T) {
	p := NewTriePosition()
	// Descend to depth 6 (bottom of root page) → all left.
	for range 6 {
		p.Down(false)
	}
	// At depth 6, node_index should be 62 (first of bottom layer).
	assert.Equal(t, 62, p.NodeIndex())
	assert.Equal(t, uint8(0), p.ChildPageIndex())
}

func TestTriePositionMax248Depth(t *testing.T) {
	p := NewTriePosition()
	for range 247 {
		p.Down(true)
	}
	assert.Equal(t, uint16(247), p.Depth())
	// One more descent should work (to 248, the max).
	p.Down(false)
	assert.Equal(t, uint16(248), p.Depth())
}

func TestTriePositionPanicsBeyond248(t *testing.T) {
	p := NewTriePosition()
	for range 248 {
		p.Down(true)
	}
	assert.Equal(t, uint16(248), p.Depth())
	// Going one more should panic.
	assert.Panics(t, func() { p.Down(false) })
}

package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPageConstants(t *testing.T) {
	assert.Equal(t, 6, PageDepth)
	assert.Equal(t, 126, NodesPerPage)
	assert.Equal(t, 64, NumChildren)
	assert.Equal(t, 4096, PageSize)
}

func TestPageNodeRoundTrip(t *testing.T) {
	var page RawPage
	node := Node{0xAB, 0xCD, 0xEF}

	tests := []struct {
		name  string
		index int
	}{
		{"first node", 0},
		{"middle node", 63},
		{"last node", NodesPerPage - 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			page.SetNodeAt(tt.index, node)
			got := page.NodeAt(tt.index)
			assert.Equal(t, node, got)
		})
	}
}

func TestPageElidedChildrenRoundTrip(t *testing.T) {
	var page RawPage

	tests := []struct {
		name string
		ec   uint64
	}{
		{"zero", 0},
		{"all set", ^uint64(0)},
		{"first bit", 1},
		{"last bit", 1 << 63},
		{"alternating", 0xAAAAAAAAAAAAAAAA},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			page.SetElidedChildren(tt.ec)
			got := page.ElidedChildren()
			assert.Equal(t, tt.ec, got)
		})
	}
}

func TestPageIDRoundTrip(t *testing.T) {
	var page RawPage
	id := [32]byte{0x01, 0x02, 0x03}

	page.SetPageIDBytes(id)
	got := page.PageIDBytes()
	assert.Equal(t, id, got)
}

func TestPageRegionsDontOverlap(t *testing.T) {
	var page RawPage

	// Write to last node
	lastNode := Node{0xFF, 0xFF, 0xFF, 0xFF}
	page.SetNodeAt(NodesPerPage-1, lastNode)

	// Write elided children
	page.SetElidedChildren(0xDEADBEEFCAFEBABE)

	// Write PageID
	var id [32]byte
	for i := range id {
		id[i] = byte(i)
	}
	page.SetPageIDBytes(id)

	// Verify none of them corrupted each other
	require.Equal(t, lastNode, page.NodeAt(NodesPerPage-1))
	require.Equal(t, uint64(0xDEADBEEFCAFEBABE), page.ElidedChildren())
	require.Equal(t, id, page.PageIDBytes())
}

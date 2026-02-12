package merkle

import (
	"testing"

	"github.com/ethereum/go-ethereum/nomt/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryPageSetRootInit(t *testing.T) {
	ps := NewMemoryPageSet(true)
	root := core.RootPageID()
	assert.True(t, ps.Contains(root))

	page, origin, ok := ps.Get(root)
	require.True(t, ok)
	assert.Equal(t, PageOriginFresh, origin.Kind)
	assert.NotNil(t, page)
}

func TestMemoryPageSetInsertGet(t *testing.T) {
	ps := NewMemoryPageSet(false)
	root := core.RootPageID()
	assert.False(t, ps.Contains(root))

	page := new(core.RawPage)
	page.SetNodeAt(0, core.Node{0x42})
	ps.Insert(root, page, PageOrigin{Kind: PageOriginPersisted})

	got, origin, ok := ps.Get(root)
	require.True(t, ok)
	assert.Equal(t, PageOriginPersisted, origin.Kind)
	assert.Equal(t, core.Node{0x42}, got.NodeAt(0))
}

func TestMemoryPageSetGetReturnsCopy(t *testing.T) {
	ps := NewMemoryPageSet(false)
	root := core.RootPageID()
	page := new(core.RawPage)
	page.SetNodeAt(0, core.Node{0x01})
	ps.Insert(root, page, PageOrigin{Kind: PageOriginFresh})

	got, _, _ := ps.Get(root)
	got.SetNodeAt(0, core.Node{0xFF}) // mutate the copy

	original, _, _ := ps.Get(root)
	assert.Equal(t, core.Node{0x01}, original.NodeAt(0),
		"mutation should not affect the stored page")
}

func TestMemoryPageSetFresh(t *testing.T) {
	ps := NewMemoryPageSet(false)
	root := core.RootPageID()
	page := ps.Fresh(root)
	assert.NotNil(t, page)
	assert.Equal(t, core.Terminator, page.NodeAt(0))
}

func TestMemoryPageSetChildPage(t *testing.T) {
	ps := NewMemoryPageSet(false)
	root := core.RootPageID()
	child, err := root.ChildPageID(5)
	require.NoError(t, err)

	page := new(core.RawPage)
	page.SetNodeAt(0, core.Node{0xAB})
	ps.Insert(child, page, PageOrigin{Kind: PageOriginPersisted})

	assert.True(t, ps.Contains(child))
	assert.False(t, ps.Contains(root))
}

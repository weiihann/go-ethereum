package merkle

import (
	"github.com/ethereum/go-ethereum/nomt/core"
)

// PageOriginKind discriminates the origin of a page in the PageSet.
type PageOriginKind int

const (
	// PageOriginPersisted indicates the page was loaded from on-disk storage.
	PageOriginPersisted PageOriginKind = iota
	// PageOriginFresh indicates the page was freshly created (zeroed).
	PageOriginFresh
)

// PageOrigin tracks where a page came from, used by the PageWalker to decide
// how to handle page elision and diff tracking.
type PageOrigin struct {
	Kind PageOriginKind
}

// PageSet is the interface through which the PageWalker reads and creates
// pages during trie updates.
type PageSet interface {
	// Get retrieves a page by its ID. Returns the page, its origin, and
	// whether it was found.
	Get(pageID core.PageID) (*core.RawPage, PageOrigin, bool)

	// Contains reports whether a page exists in the set.
	Contains(pageID core.PageID) bool

	// Fresh creates a new zeroed page for the given ID.
	Fresh(pageID core.PageID) *core.RawPage

	// Insert adds or replaces a page in the set.
	Insert(pageID core.PageID, page *core.RawPage, origin PageOrigin)
}

// MemoryPageSet is an in-memory PageSet implementation backed by a map.
type MemoryPageSet struct {
	pages map[string]memoryPageEntry
}

type memoryPageEntry struct {
	page   *core.RawPage
	origin PageOrigin
}

// NewMemoryPageSet creates a MemoryPageSet, optionally pre-populated with a
// root page.
func NewMemoryPageSet(withRoot bool) *MemoryPageSet {
	ps := &MemoryPageSet{
		pages: make(map[string]memoryPageEntry, 16),
	}
	if withRoot {
		root := core.RootPageID()
		page := new(core.RawPage)
		ps.pages[pageIDKey(root)] = memoryPageEntry{
			page:   page,
			origin: PageOrigin{Kind: PageOriginFresh},
		}
	}
	return ps
}

// Get retrieves a page from the in-memory set.
func (m *MemoryPageSet) Get(pageID core.PageID) (*core.RawPage, PageOrigin, bool) {
	entry, ok := m.pages[pageIDKey(pageID)]
	if !ok {
		return nil, PageOrigin{}, false
	}
	// Return a copy so the walker can mutate freely.
	pageCopy := new(core.RawPage)
	*pageCopy = *entry.page
	return pageCopy, entry.origin, true
}

// Contains reports whether the page exists.
func (m *MemoryPageSet) Contains(pageID core.PageID) bool {
	_, ok := m.pages[pageIDKey(pageID)]
	return ok
}

// Fresh creates a new zeroed page.
func (m *MemoryPageSet) Fresh(pageID core.PageID) *core.RawPage {
	return new(core.RawPage)
}

// Insert stores a page in the set.
func (m *MemoryPageSet) Insert(
	pageID core.PageID, page *core.RawPage, origin PageOrigin,
) {
	m.pages[pageIDKey(pageID)] = memoryPageEntry{page: page, origin: origin}
}

// Apply applies a list of UpdatedPages into the page set, making them
// available for subsequent reads.
func (m *MemoryPageSet) Apply(updates []UpdatedPage) {
	for _, up := range updates {
		pageCopy := new(core.RawPage)
		*pageCopy = *up.Page
		m.pages[pageIDKey(up.PageID)] = memoryPageEntry{
			page:   pageCopy,
			origin: PageOrigin{Kind: PageOriginPersisted},
		}
	}
}

func pageIDKey(id core.PageID) string {
	encoded := id.Encode()
	return string(encoded[:])
}

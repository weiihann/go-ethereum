package merkle

import (
	"github.com/ethereum/go-ethereum/nomt/core"
)

// UpdatedPage is a page that was modified during a trie update.
type UpdatedPage struct {
	PageID core.PageID
	Page   *core.RawPage
	Diff   core.PageDiff
}

// Output is the result of concluding a PageWalker.
type Output struct {
	// Root is the new root node hash (set when no parent page was supplied).
	Root core.Node
	// Pages is the list of all pages modified during the update.
	Pages []UpdatedPage
	// ChildPageRoots holds (position, node) pairs for nodes that should be
	// placed in the parent page's bottom layer (set when a parent page was
	// supplied).
	ChildPageRoots []childPageRoot
}

type childPageRoot struct {
	Position core.TriePosition
	Node     core.Node
}

// stackPage is a page currently held in the walker's ascending page stack.
type stackPage struct {
	pageID core.PageID
	page   *core.RawPage
	diff   core.PageDiff
	elided ElidedChildren
	origin PageOrigin
}

// PageWalker performs left-to-right walking and updating of the page tree.
//
// Usage: create a PageWalker, make repeated calls to AdvanceAndReplace (and
// optionally Advance / AdvanceAndPlaceNode), then call Conclude to get the
// new root and all updated pages.
type PageWalker struct {
	lastPosition *core.TriePosition // nil before first advance
	position     core.TriePosition
	parentPage   *core.PageID
	root         core.Node

	stack        []stackPage
	siblingStack []siblingEntry
	prevNode     *core.Node

	outputPages    []UpdatedPage
	childPageRoots []childPageRoot
}

type siblingEntry struct {
	node  core.Node
	depth int
}

// NewPageWalker creates a new PageWalker starting from the given root.
// If parentPage is non-nil, the walker is constrained to pages below the
// parent, and Conclude returns ChildPageRoots instead of Root.
func NewPageWalker(root core.Node, parentPage *core.PageID) *PageWalker {
	return &PageWalker{
		position:     core.NewTriePosition(),
		root:         root,
		parentPage:   parentPage,
		stack:        make([]stackPage, 0, 8),
		siblingStack: make([]siblingEntry, 0, 16),
		outputPages:  make([]UpdatedPage, 0, 16),
	}
}

// AdvanceAndReplace advances to the given position and replaces the terminal
// node there with a sub-trie built from the provided stem key-value pairs.
//
// The pairs must be sorted and must all share the prefix corresponding to
// the position. An empty slice deletes the existing terminal node.
//
// Panics if the position is not greater than the previous position.
func (w *PageWalker) AdvanceAndReplace(
	pageSet PageSet,
	newPos core.TriePosition,
	ops []core.StemKeyValue,
) {
	if w.lastPosition != nil {
		w.assertForward(&newPos)
		w.compactUp(&newPos)
	}
	pos := newPos
	w.lastPosition = &pos
	w.buildStack(pageSet, newPos)
	w.replaceTerminal(pageSet, ops)
}

// AdvanceAndPlaceNode advances to the given position and sets the given node.
//
// This is used to place child-page root nodes computed by parallel workers.
func (w *PageWalker) AdvanceAndPlaceNode(
	pageSet PageSet,
	newPos core.TriePosition,
	node core.Node,
) {
	if w.lastPosition != nil {
		w.assertForward(&newPos)
		w.compactUp(&newPos)
	}
	pos := newPos
	w.lastPosition = &pos
	w.buildStack(pageSet, newPos)
	w.placeNode(node)
}

// Advance moves the walker to a new position without modifying the trie.
func (w *PageWalker) Advance(newPos core.TriePosition) {
	if w.lastPosition != nil {
		w.assertForward(&newPos)
		w.compactUp(&newPos)
	}
	pos := newPos
	w.lastPosition = &pos
}

// Conclude finalizes the walk and returns the output: a new root node and
// all updated pages.
func (w *PageWalker) Conclude() Output {
	w.compactUpToRoot()

	out := Output{
		Root:           w.root,
		Pages:          w.outputPages,
		ChildPageRoots: w.childPageRoots,
	}
	return out
}

// --- core operations ---

func (w *PageWalker) placeNode(node core.Node) {
	if w.position.IsRoot() {
		prev := w.root
		w.prevNode = &prev
		w.root = node
	} else {
		prev := w.node()
		w.prevNode = &prev
		w.setNode(node)
	}
}

func (w *PageWalker) replaceTerminal(pageSet PageSet, ops []core.StemKeyValue) {
	var existingNode core.Node
	if w.position.IsRoot() {
		existingNode = w.root
	} else {
		existingNode = w.node()
	}
	w.prevNode = &existingNode

	startDepth := w.position.Depth()

	core.BuildInternalTree(int(w.position.Depth()), ops, func(wn core.WriteNode) {
		node := wn.Node

		// For internal nodes, clear garbage in the sibling slot if the
		// sibling is a terminator.
		if wn.Kind == core.WriteNodeInternal && wn.InternalData != nil {
			lastBit := w.position.PeekLastBit()
			var zeroSibling bool
			if lastBit {
				zeroSibling = core.IsTerminator(&wn.InternalData.Left)
			} else {
				zeroSibling = core.IsTerminator(&wn.InternalData.Right)
			}
			if zeroSibling {
				w.setSibling(core.Terminator)
			}
		}

		// Navigate: up then down.
		if wn.GoUp && len(wn.DownBits) > 0 {
			// Optimization: if the first down bit goes to the sibling,
			// use Sibling() instead of going up + down.
			if wn.DownBits[0] != w.position.PeekLastBit() {
				w.position.Sibling()
				w.downBits(pageSet, wn.DownBits[1:], true)
			} else {
				w.up()
				w.downBits(pageSet, wn.DownBits, true)
			}
		} else if wn.GoUp {
			w.up()
		} else if len(wn.DownBits) > 0 {
			// First bit is only fresh if we are at the start position and
			// start is at end of page (or root). After that, definitely fresh.
			fresh := w.position.DepthInPage() == core.PageDepth ||
				w.position.IsRoot()
			if w.position.Depth() <= startDepth {
				w.downBits(pageSet, wn.DownBits[:1], fresh)
				w.downBits(pageSet, wn.DownBits[1:], true)
			} else {
				w.downBits(pageSet, wn.DownBits, true)
			}
		}

		if w.position.IsRoot() {
			w.root = node
		} else {
			w.setNode(node)
		}
	})
}

// --- stack navigation ---

// up moves one level toward the root. If crossing a page boundary, pops the
// stack page and emits it as output.
func (w *PageWalker) up() {
	if w.position.DepthInPage() == 1 {
		w.popStackPage()
	}
	w.position.Up(1)
}

// downBits descends along the given bit path, pushing new pages onto the
// stack as needed.
func (w *PageWalker) downBits(pageSet PageSet, bits []bool, fresh bool) {
	for _, bit := range bits {
		if w.position.IsRoot() {
			rootID := core.RootPageID()
			var page *core.RawPage
			var origin PageOrigin
			if fresh {
				page = pageSet.Fresh(rootID)
				origin = PageOrigin{Kind: PageOriginFresh}
			} else {
				var ok bool
				page, origin, ok = pageSet.Get(rootID)
				if !ok {
					panic("pagewalker: root page not in page set")
				}
			}
			w.pushPage(rootID, page, origin)
		} else if w.position.DepthInPage() == core.PageDepth {
			// Crossing into a child page.
			parentSP := w.stack[len(w.stack)-1]
			childIdx := w.position.ChildPageIndex()
			childID, err := parentSP.pageID.ChildPageID(childIdx)
			if err != nil {
				panic("pagewalker: child page ID overflow")
			}
			var page *core.RawPage
			var origin PageOrigin
			if fresh {
				page = pageSet.Fresh(childID)
				origin = PageOrigin{Kind: PageOriginFresh}
			} else {
				var ok bool
				page, origin, ok = pageSet.Get(childID)
				if !ok {
					panic("pagewalker: child page not in page set")
				}
			}
			w.pushPage(childID, page, origin)
		}
		w.position.Down(bit)
	}
}

func (w *PageWalker) pushPage(
	pageID core.PageID, page *core.RawPage, origin PageOrigin,
) {
	w.stack = append(w.stack, stackPage{
		pageID: pageID,
		page:   page,
		diff:   core.PageDiff{},
		elided: ElidedChildrenFromUint64(page.ElidedChildren()),
		origin: origin,
	})
}

func (w *PageWalker) popStackPage() {
	if len(w.stack) == 0 {
		return
	}
	sp := w.stack[len(w.stack)-1]
	w.stack = w.stack[:len(w.stack)-1]

	// Store elided children back into the page before emitting.
	if !sp.pageID.IsRoot() {
		sp.page.SetElidedChildren(sp.elided.Raw())
	}

	w.outputPages = append(w.outputPages, UpdatedPage{
		PageID: sp.pageID,
		Page:   sp.page,
		Diff:   sp.diff,
	})
}

// buildStack pushes pages onto the stack from the current position down to
// the target position's page.
func (w *PageWalker) buildStack(pageSet PageSet, position core.TriePosition) {
	w.position = position
	newPageID := position.PageID()
	if newPageID == nil {
		// Target is at the root — pop all remaining stack pages.
		for len(w.stack) > 0 {
			w.popStackPage()
		}
		return
	}

	// Determine which ancestor to push down from.
	var target *core.PageID
	if len(w.stack) > 0 {
		t := w.stack[len(w.stack)-1].pageID
		target = &t
	} else if w.parentPage != nil {
		target = w.parentPage
	}

	// Collect pages from newPageID up to target.
	var toPush []core.PageID
	cur := *newPageID
	for {
		if target != nil && cur.Equal(*target) {
			break
		}
		toPush = append(toPush, cur)
		if cur.IsRoot() {
			break
		}
		cur = cur.ParentPageID()
	}

	// Push in ascending order (root-ward first).
	for i := len(toPush) - 1; i >= 0; i-- {
		pid := toPush[i]
		page, origin, ok := pageSet.Get(pid)
		if !ok {
			panic("pagewalker: page not in page set during build_stack")
		}
		w.pushPage(pid, page, origin)
	}
}

// --- compaction ---

// compactUp hashes upward from the current position toward the shared
// ancestor with the target position. This is the core "partial compaction"
// that avoids redundant hashing.
func (w *PageWalker) compactUp(targetPos *core.TriePosition) {
	if len(w.stack) == 0 {
		return
	}

	currentDepth := int(w.position.Depth())
	sharedDepth := w.position.SharedDepth(targetPos)

	// Prune sibling stack entries beyond shared depth.
	keepLen := 0
	for _, s := range w.siblingStack {
		if s.depth <= sharedDepth {
			keepLen++
		} else {
			break
		}
	}
	w.siblingStack = w.siblingStack[:keepLen]

	compactLayers := currentDepth - (sharedDepth + 1)
	if compactLayers == 0 {
		if w.prevNode != nil {
			w.siblingStack = append(w.siblingStack, siblingEntry{
				node:  *w.prevNode,
				depth: currentDepth,
			})
			w.prevNode = nil
		}
	} else {
		w.prevNode = nil
	}

	for i := range compactLayers {
		nextNode := w.compactStep()
		w.up()

		if len(w.stack) == 0 {
			if w.parentPage == nil {
				w.root = nextNode
			} else {
				w.childPageRoots = append(w.childPageRoots, childPageRoot{
					Position: w.position,
					Node:     nextNode,
				})
			}
			break
		} else {
			// Save the final relevant sibling.
			if i == compactLayers-1 {
				w.siblingStack = append(w.siblingStack, siblingEntry{
					node:  w.node(),
					depth: int(w.position.Depth()),
				})
			}
			w.setNode(nextNode)
		}
	}
}

// compactUpToRoot is called by Conclude to hash all remaining layers to root.
func (w *PageWalker) compactUpToRoot() {
	if len(w.stack) == 0 {
		return
	}

	w.siblingStack = w.siblingStack[:0]
	compactLayers := int(w.position.Depth())

	for range compactLayers {
		nextNode := w.compactStep()
		w.up()

		if len(w.stack) == 0 {
			if w.parentPage == nil {
				w.root = nextNode
			} else {
				w.childPageRoots = append(w.childPageRoots, childPageRoot{
					Position: w.position,
					Node:     nextNode,
				})
			}
			break
		} else {
			w.setNode(nextNode)
		}
	}
}

// compactStep performs one layer of compaction: reads the current node and
// its sibling, then hashes them as an internal node. In EIP-7864, there is
// no leaf compaction — stem hashes are opaque values that never float up.
func (w *PageWalker) compactStep() core.Node {
	node := w.node()
	sibling := w.siblingNode()

	if core.IsTerminator(&node) && core.IsTerminator(&sibling) {
		return core.Terminator
	}

	bit := w.position.PeekLastBit()
	var id core.InternalData
	if bit {
		id = core.InternalData{Left: sibling, Right: node}
	} else {
		id = core.InternalData{Left: node, Right: sibling}
	}
	return core.HashInternal(&id)
}

// --- page node access ---

// node reads the node at the current position from the top stack page.
func (w *PageWalker) node() core.Node {
	sp := &w.stack[len(w.stack)-1]
	return sp.page.NodeAt(w.position.NodeIndex())
}

// siblingNode reads the sibling of the current position.
func (w *PageWalker) siblingNode() core.Node {
	sp := &w.stack[len(w.stack)-1]
	return sp.page.NodeAt(w.position.SiblingIndex())
}

// setNode writes a node at the current position and records it in the diff.
func (w *PageWalker) setNode(node core.Node) {
	idx := w.position.NodeIndex()
	sibNode := w.siblingNode()

	sp := &w.stack[len(w.stack)-1]
	sp.page.SetNodeAt(idx, node)

	// If both the node and its sibling are terminators at the first layer,
	// mark the page as cleared instead of changed.
	if w.position.IsFirstLayerInPage() &&
		core.IsTerminator(&node) &&
		core.IsTerminator(&sibNode) {
		sp.diff.SetCleared()
	} else {
		sp.diff.SetChanged(idx)
	}
}

// setSibling writes a node at the sibling position and records the change.
func (w *PageWalker) setSibling(node core.Node) {
	sibIdx := w.position.SiblingIndex()
	sp := &w.stack[len(w.stack)-1]
	sp.page.SetNodeAt(sibIdx, node)
	sp.diff.SetChanged(sibIdx)
}

// --- assertions ---

func (w *PageWalker) assertForward(newPos *core.TriePosition) {
	if w.lastPosition == nil {
		return
	}
	newPath := newPos.Path()
	lastPath := w.lastPosition.Path()
	for i := range newPath {
		if newPath[i] > lastPath[i] {
			return
		}
		if newPath[i] < lastPath[i] {
			panic("pagewalker: positions must advance left-to-right")
		}
	}
	panic("pagewalker: positions must advance left-to-right (equal)")
}

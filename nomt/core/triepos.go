package core

// TriePosition tracks a position within the paged binary trie, combining a
// key path prefix with a node index within the current page.
type TriePosition struct {
	path      [32]byte
	depth     uint16
	nodeIndex int
}

// NewTriePosition creates a TriePosition at the root.
func NewTriePosition() TriePosition {
	return TriePosition{}
}

// MaxTrieDepth is the maximum depth of the internal node tree (248 bits = 31 bytes).
// Stem nodes exist at this depth; the last 8 bits are the stem suffix.
const MaxTrieDepth = StemSize * 8 // 248

// TriePositionFromPathAndDepth creates a TriePosition at the given depth
// within the path. Panics if depth is 0.
func TriePositionFromPathAndDepth(path KeyPath, depth uint16) TriePosition {
	if depth == 0 {
		panic("triepos: depth must be non-zero")
	}
	if depth > MaxTrieDepth {
		panic("triepos: depth out of range")
	}
	pagePath := lastPagePath(path[:], depth)
	return TriePosition{
		path:      path,
		depth:     depth,
		nodeIndex: computeNodeIndex(pagePath),
	}
}

// IsRoot reports whether the position is at the root.
func (p *TriePosition) IsRoot() bool {
	return p.depth == 0
}

// Depth returns the current depth in the trie.
func (p *TriePosition) Depth() uint16 {
	return p.depth
}

// Path returns the raw 32-byte path.
func (p *TriePosition) Path() [32]byte {
	return p.path
}

// Bit returns the bit at position n in the path (0 = MSB of byte 0).
func (p *TriePosition) Bit(n int) bool {
	return (p.path[n/8]>>(7-n%8))&1 == 1
}

// NodeIndex returns the index of the current node within its page.
func (p *TriePosition) NodeIndex() int {
	return p.nodeIndex
}

// Down moves the position down by 1 bit (left if bit=false, right if bit=true).
func (p *TriePosition) Down(bit bool) {
	if p.depth >= MaxTrieDepth {
		panic("triepos: can't descend past 248 bits")
	}
	if int(p.depth)%PageDepth == 0 {
		// Entering a new page: node index resets.
		if bit {
			p.nodeIndex = 1
		} else {
			p.nodeIndex = 0
		}
	} else {
		left := p.nodeIndex*2 + 2
		if bit {
			p.nodeIndex = left + 1
		} else {
			p.nodeIndex = left
		}
	}
	setBit(&p.path, int(p.depth), bit)
	p.depth++
}

// Up moves the position up by d bits.
func (p *TriePosition) Up(d uint16) {
	if d > p.depth {
		panic("triepos: can't move up past root")
	}
	newDepth := p.depth - d
	if newDepth == 0 {
		*p = NewTriePosition()
		return
	}

	prevPageDepth := (int(p.depth) + PageDepth - 1) / PageDepth
	newPageDepth := (int(newDepth) + PageDepth - 1) / PageDepth
	p.depth = newDepth

	if prevPageDepth == newPageDepth {
		// Same page — walk up parent indices.
		for range d {
			p.nodeIndex = (p.nodeIndex - 2) / 2
		}
	} else {
		// Crossed a page boundary — recompute.
		pagePath := lastPagePath(p.path[:], p.depth)
		p.nodeIndex = computeNodeIndex(pagePath)
	}
}

// Sibling moves to the sibling node. Panics at root.
func (p *TriePosition) Sibling() {
	if p.depth == 0 {
		panic("triepos: can't sibling at root")
	}
	i := int(p.depth) - 1
	flipBit(&p.path, i)
	p.nodeIndex = siblingIndex(p.nodeIndex)
}

// PeekLastBit returns the last bit of the path. Panics at root.
func (p *TriePosition) PeekLastBit() bool {
	if p.depth == 0 {
		panic("triepos: can't peek at root")
	}
	return p.Bit(int(p.depth) - 1)
}

// PageID returns the PageID for the page this position lands in.
// Returns nil if at the root.
func (p *TriePosition) PageID() *PageID {
	if p.IsRoot() {
		return nil
	}

	pageID := RootPageID()
	d := int(p.depth)
	// Number of complete 6-bit chunks before the current partial chunk.
	fullChunks := (d - 1) / PageDepth
	for i := range fullChunks {
		childIndex := extractChildIndex(p.path, i*PageDepth)
		pageID, _ = pageID.ChildPageID(childIndex)
	}
	return &pageID
}

// DepthInPage returns the number of bits traversed in the current page (1-6),
// or 0 if at the root.
func (p *TriePosition) DepthInPage() int {
	if p.depth == 0 {
		return 0
	}
	d := int(p.depth)
	return d - ((d-1)/PageDepth)*PageDepth
}

// IsFirstLayerInPage reports whether this position is at the top of its page
// (node index 0 or 1).
func (p *TriePosition) IsFirstLayerInPage() bool {
	return p.nodeIndex&^1 == 0
}

// ChildNodeIndices returns the left and right child node indices within the
// page. Panics if not at depth 1-5 within the page.
func (p *TriePosition) ChildNodeIndices() (left, right int) {
	dip := p.DepthInPage()
	if dip == 0 || dip > PageDepth-1 {
		panic("triepos: child indices out of bounds")
	}
	left = p.nodeIndex*2 + 2
	right = left + 1
	return
}

// ChildPageIndex returns the ChildPageIndex for the current node.
// Panics if not at the last layer of the page (indices 62-125).
func (p *TriePosition) ChildPageIndex() uint8 {
	if p.nodeIndex < 62 {
		panic("triepos: not at last layer")
	}
	return uint8(p.nodeIndex - 62)
}

// SiblingIndex returns the index of the sibling node.
func (p *TriePosition) SiblingIndex() int {
	return siblingIndex(p.nodeIndex)
}

// SharedDepth returns the number of leading path bits shared between two
// TriePositions, considering only bits up to the shorter depth.
func (p *TriePosition) SharedDepth(other *TriePosition) int {
	maxBits := min(int(p.depth), int(other.depth))
	for i := range maxBits {
		pBit := (p.path[i/8] >> (7 - i%8)) & 1
		oBit := (other.path[i/8] >> (7 - i%8)) & 1
		if pBit != oBit {
			return i
		}
	}
	return maxBits
}

// --- internal helpers ---

// computeNodeIndex converts a page-local bit path to a level-order node index.
// Formula: (2^depth - 2) + bits_as_uint, where depth is 1-6.
func computeNodeIndex(pagePath pageBitPath) int {
	depth := pagePath.len
	if depth == 0 {
		return 0
	}
	if depth > PageDepth {
		depth = PageDepth
	}
	return (1 << depth) - 2 + pagePath.asUint(depth)
}

// pageBitPath represents a sub-slice of bits within a path for node indexing.
type pageBitPath struct {
	path   []byte // the full 32-byte key path
	offset int    // bit offset where this page's path starts
	len    int    // number of bits (1-6)
}

// asUint interprets the first `n` bits as an unsigned integer (MSB first).
func (p pageBitPath) asUint(n int) int {
	var val int
	for i := range n {
		byteIdx := (p.offset + i) / 8
		bitIdx := 7 - (p.offset+i)%8
		bit := int((p.path[byteIdx] >> bitIdx) & 1)
		val = (val << 1) | bit
	}
	return val
}

// lastPagePath extracts the relevant bit path for the current page.
func lastPagePath(path []byte, depth uint16) pageBitPath {
	d := int(depth)
	prevPageEnd := ((d - 1) / PageDepth) * PageDepth
	return pageBitPath{
		path:   path,
		offset: prevPageEnd,
		len:    d - prevPageEnd,
	}
}

func setBit(path *[32]byte, idx int, val bool) {
	byteIdx := idx / 8
	bitIdx := uint(7 - idx%8)
	if val {
		path[byteIdx] |= 1 << bitIdx
	} else {
		path[byteIdx] &^= 1 << bitIdx
	}
}

func flipBit(path *[32]byte, idx int) {
	byteIdx := idx / 8
	bitIdx := uint(7 - idx%8)
	path[byteIdx] ^= 1 << bitIdx
}

func siblingIndex(nodeIndex int) int {
	if nodeIndex%2 == 0 {
		return nodeIndex + 1
	}
	return nodeIndex - 1
}

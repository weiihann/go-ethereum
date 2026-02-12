package core

import "math/bits"

// StemKeyValue is a resolved (stemPath, stemHash) pair for the page tree.
// The stem hash is precomputed by the integration layer using HashStem.
type StemKeyValue struct {
	Stem StemPath // 31-byte stem (248 bits)
	Hash Node     // precomputed SHA256 stem hash
}

// WriteNodeKind enumerates the types of write commands from BuildInternalTree.
type WriteNodeKind int

const (
	WriteNodeStem       WriteNodeKind = iota // opaque stem hash placed at tree bottom
	WriteNodeInternal                        // internal node (hash of left+right)
	WriteNodeTerminator                      // empty sub-trie
)

// WriteNode represents a node to be written during trie building.
type WriteNode struct {
	Kind         WriteNodeKind
	Node         Node
	InternalData *InternalData // set for internal writes

	// Navigation: move up 1 before writing (true for internal nodes and
	// non-first stem placements).
	GoUp bool
	// Navigation: bits to descend after going up (only for stem writes).
	DownBits []bool
}

// StemSharedBits counts the number of shared prefix bits between two stem
// paths, starting after `skip` bits.
func StemSharedBits(a, b *StemPath, skip int) int {
	startByte := skip / 8

	// Handle partial first byte if skip is not byte-aligned.
	if skip%8 != 0 {
		mask := byte(0xFF >> (skip % 8))
		xor := (a[startByte] ^ b[startByte]) & mask
		if xor != 0 {
			return bits.LeadingZeros8(xor) - (skip % 8)
		}
		startByte++
	}

	// Compare full bytes.
	for i := startByte; i < StemSize; i++ {
		xor := a[i] ^ b[i]
		if xor != 0 {
			return i*8 + bits.LeadingZeros8(xor) - skip
		}
	}
	return StemSize*8 - skip
}

// BuildInternalTree builds a compact internal-node sub-trie from sorted
// (stem, hash) pairs.
//
// skip: the number of prefix bits already consumed (all ops share this prefix).
// ops: sorted StemKeyValue pairs (by stem path).
// visit: callback invoked for each computed node, bottom-up.
//
// Returns the root node of the built sub-trie.
//
// This replaces the old BuildTrie. The key difference: there are no leaf
// nodes — stem hashes are opaque values placed at tree positions, and
// internal nodes are always SHA256(left || right) with no MSB tagging
// or leaf compaction.
func BuildInternalTree(skip int, ops []StemKeyValue, visit func(WriteNode)) Node {
	if len(ops) == 0 {
		visit(WriteNode{Kind: WriteNodeTerminator, Node: Terminator})
		return Terminator
	}

	if len(ops) == 1 {
		visit(WriteNode{
			Kind: WriteNodeStem,
			Node: ops[0].Hash,
			GoUp: false,
		})
		return ops[0].Hash
	}

	// 3-pointer left-frontier algorithm (same structure as old BuildTrie
	// but without leaf hashing or leaf compaction).
	type pendingSibling struct {
		node  Node
		layer int
	}
	pendingSiblings := make([]pendingSibling, 0, 16)

	commonAfterPrefix := func(s1, s2 *StemPath) int {
		return StemSharedBits(s1, s2, skip)
	}

	var aStem *StemPath

	for bIdx := 0; bIdx < len(ops); bIdx++ {
		thisStem := &ops[bIdx].Stem
		thisHash := ops[bIdx].Hash

		var n1 *int
		if aStem != nil {
			v := commonAfterPrefix(aStem, thisStem)
			n1 = &v
		}

		var n2 *int
		if bIdx+1 < len(ops) {
			v := commonAfterPrefix(&ops[bIdx+1].Stem, thisStem)
			n2 = &v
		}

		var stemDepth, hashUpLayers int
		switch {
		case n1 == nil && n2 == nil:
			stemDepth = 0
			hashUpLayers = 0
		case n1 == nil && n2 != nil:
			stemDepth = *n2 + 1
			hashUpLayers = 0
		case n1 != nil && n2 == nil:
			stemDepth = *n1 + 1
			hashUpLayers = *n1 + 1
		default:
			stemDepth = max(*n1, *n2) + 1
			hashUpLayers = 0
			if *n1 > *n2 {
				hashUpLayers = *n1 - *n2
			}
		}

		layer := stemDepth
		lastNode := thisHash

		// Compute down bits for the visitor.
		downStart := skip
		if n1 != nil {
			downStart = skip + *n1
		}
		stemEndBit := skip + stemDepth

		var downBuf [StemSize * 8]bool
		var downBits []bool
		if stemEndBit > downStart {
			downBits = downBuf[:stemEndBit-downStart]
			for i := downStart; i < stemEndBit; i++ {
				downBits[i-downStart] = stemBitAt(thisStem, i)
			}
		}

		visit(WriteNode{
			Kind:     WriteNodeStem,
			Node:     thisHash,
			GoUp:     n1 != nil,
			DownBits: downBits,
		})

		// Hash upward.
		for h := 0; h < hashUpLayers; h++ {
			layer--
			bitIdx := skip + layer
			bit := stemBitAt(thisStem, bitIdx)

			var sibling Node
			if len(pendingSiblings) > 0 &&
				pendingSiblings[len(pendingSiblings)-1].layer == layer+1 {
				sibling = pendingSiblings[len(pendingSiblings)-1].node
				pendingSiblings = pendingSiblings[:len(pendingSiblings)-1]
			}

			var id InternalData
			if bit {
				id = InternalData{Left: sibling, Right: lastNode}
			} else {
				id = InternalData{Left: lastNode, Right: sibling}
			}

			lastNode = HashInternal(&id)
			visit(WriteNode{
				Kind:         WriteNodeInternal,
				Node:         lastNode,
				InternalData: &id,
				GoUp:         true,
			})
		}

		pendingSiblings = append(pendingSiblings,
			pendingSibling{node: lastNode, layer: layer})

		aStem = thisStem
	}

	if len(pendingSiblings) > 0 {
		return pendingSiblings[len(pendingSiblings)-1].node
	}
	return Terminator
}

func stemBitAt(stem *StemPath, idx int) bool {
	return (stem[idx/8]>>(7-idx%8))&1 == 1
}

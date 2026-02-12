package core

import "sort"

// LeafOp represents a leaf operation: set or delete.
// A nil ValueHash pointer means delete.
type LeafOp struct {
	Key   KeyPath
	Value *ValueHash
}

// KeyValue is a resolved (key, value) pair for trie building.
type KeyValue struct {
	Key   KeyPath
	Value ValueHash
}

// WriteNodeKind enumerates the types of write commands from BuildTrie.
type WriteNodeKind int

const (
	WriteNodeLeaf WriteNodeKind = iota
	WriteNodeInternal
	WriteNodeTerminator
)

// WriteNode represents a node to be written during trie building.
type WriteNode struct {
	Kind         WriteNodeKind
	Node         Node
	LeafData     *LeafData     // set for leaf writes
	InternalData *InternalData // set for internal writes

	// Navigation: move up 1 before writing (true for internal nodes and
	// non-first leaves).
	GoUp bool
	// Navigation: bits to descend after going up (only for leaf writes).
	DownBits []bool
}

// SharedBits counts the number of shared prefix bits between two key paths,
// starting after `skip` bits.
func SharedBits(a, b *KeyPath, skip int) int {
	count := 0
	for i := skip; i < 256; i++ {
		aBit := (a[i/8] >> (7 - i%8)) & 1
		bBit := (b[i/8] >> (7 - i%8)) & 1
		if aBit != bBit {
			break
		}
		count++
	}
	return count
}

// LeafOpsSpliced creates a combined operation list from an existing leaf and
// new operations. If the existing leaf's key is not in ops, it is spliced in.
// Deletions (nil value) are filtered out.
func LeafOpsSpliced(existingLeaf *LeafData, ops []LeafOp) []KeyValue {
	// Find splice position: where the existing leaf would be inserted.
	spliceIndex := -1
	if existingLeaf != nil {
		idx := sort.Search(len(ops), func(i int) bool {
			return keyPathCmp(&ops[i].Key, &existingLeaf.KeyPath) >= 0
		})
		if idx >= len(ops) || ops[idx].Key != existingLeaf.KeyPath {
			spliceIndex = idx
		}
	}

	result := make([]KeyValue, 0, len(ops)+1)

	if spliceIndex < 0 {
		// No splicing needed — just filter out deletes.
		for _, op := range ops {
			if op.Value != nil {
				result = append(result, KeyValue{op.Key, *op.Value})
			}
		}
		return result
	}

	// Before splice point.
	for _, op := range ops[:spliceIndex] {
		if op.Value != nil {
			result = append(result, KeyValue{op.Key, *op.Value})
		}
	}

	// The existing leaf.
	result = append(result, KeyValue{
		existingLeaf.KeyPath,
		existingLeaf.ValueHash,
	})

	// After splice point.
	for _, op := range ops[spliceIndex:] {
		if op.Value != nil {
			result = append(result, KeyValue{op.Key, *op.Value})
		}
	}

	return result
}

// BuildTrie builds a compact sub-trie from sorted (key, value) pairs.
//
// skip: the number of prefix bits already consumed (all ops share this prefix).
// ops: sorted (KeyPath, ValueHash) pairs.
// visit: callback invoked for each computed node, bottom-up.
//
// Returns the root node of the built sub-trie.
//
// The algorithm uses a 3-pointer sliding window (a, b, c) over the sorted
// ops to determine each leaf's depth based on shared bits with its neighbors.
// Internal nodes are computed by hashing up a left-frontier stack.
func BuildTrie(skip int, ops []KeyValue, visit func(WriteNode)) Node {
	if len(ops) == 0 {
		visit(WriteNode{Kind: WriteNodeTerminator, Node: Terminator})
		return Terminator
	}

	if len(ops) == 1 {
		ld := LeafData{
			KeyPath:   ops[0].Key,
			ValueHash: ops[0].Value,
		}
		h := HashLeaf(&ld)
		visit(WriteNode{
			Kind:     WriteNodeLeaf,
			Node:     h,
			LeafData: &ld,
			GoUp:     false,
		})
		return h
	}

	// 3-pointer left-frontier algorithm.
	type pendingSibling struct {
		node  Node
		layer int
	}
	pendingSiblings := make([]pendingSibling, 0, 16)

	commonAfterPrefix := func(k1, k2 *KeyPath) int {
		return SharedBits(k1, k2, skip)
	}

	// Sliding window: a, b, c.
	var aKey *KeyPath
	var aVal *ValueHash

	for bIdx := 0; bIdx < len(ops); bIdx++ {
		thisKey := &ops[bIdx].Key
		thisVal := &ops[bIdx].Value

		var n1 *int
		if aKey != nil {
			v := commonAfterPrefix(aKey, thisKey)
			n1 = &v
		}

		var n2 *int
		if bIdx+1 < len(ops) {
			v := commonAfterPrefix(&ops[bIdx+1].Key, thisKey)
			n2 = &v
		}

		ld := LeafData{KeyPath: *thisKey, ValueHash: *thisVal}
		leaf := HashLeaf(&ld)

		var leafDepth, hashUpLayers int
		switch {
		case n1 == nil && n2 == nil:
			leafDepth = 0
			hashUpLayers = 0
		case n1 == nil && n2 != nil:
			leafDepth = *n2 + 1
			hashUpLayers = 0
		case n1 != nil && n2 == nil:
			leafDepth = *n1 + 1
			hashUpLayers = *n1 + 1
		default:
			leafDepth = max(*n1, *n2) + 1
			hashUpLayers = 0
			if *n1 > *n2 {
				hashUpLayers = *n1 - *n2
			}
		}

		layer := leafDepth
		lastNode := leaf

		// Compute down bits for the visitor.
		downStart := skip
		if n1 != nil {
			downStart = skip + *n1
		}
		leafEndBit := skip + leafDepth

		var downBits []bool
		if leafEndBit > downStart {
			downBits = make([]bool, leafEndBit-downStart)
			for i := downStart; i < leafEndBit; i++ {
				downBits[i-downStart] = bitAt(thisKey, i)
			}
		}

		visit(WriteNode{
			Kind:     WriteNodeLeaf,
			Node:     leaf,
			LeafData: &ld,
			GoUp:     n1 != nil,
			DownBits: downBits,
		})

		// Hash upward.
		for h := 0; h < hashUpLayers; h++ {
			layer--
			bitIdx := skip + layer // the bit at this layer
			bit := bitAt(thisKey, bitIdx)

			// Pop sibling from pending if it matches.
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

		aKey = thisKey
		aVal = thisVal
	}
	_ = aVal // used in the loop to track state

	if len(pendingSiblings) > 0 {
		return pendingSiblings[len(pendingSiblings)-1].node
	}
	return Terminator
}

func bitAt(key *KeyPath, idx int) bool {
	return (key[idx/8]>>(7-idx%8))&1 == 1
}

func keyPathCmp(a, b *KeyPath) int {
	for i := range a {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return 0
}

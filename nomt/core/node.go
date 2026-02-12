// Package core defines the fundamental data structures for a NOMT binary
// merkle trie. All types are pure computation with no I/O dependencies.
package core

// Node is a 256-bit hash representing a node in the binary merkle trie.
// The MSB of byte 0 discriminates leaves (MSB=1) from internal nodes (MSB=0).
// The all-zeros value is reserved as the Terminator.
type Node = [32]byte

// KeyPath is the 256-bit lookup path for a key in the trie.
type KeyPath = [32]byte

// ValueHash is the 256-bit hash of a value stored at a leaf.
type ValueHash = [32]byte

// Terminator is the special node value denoting an empty sub-trie.
// When this appears at a location, no key with a matching path prefix has a value.
var Terminator Node

// NodeKind discriminates the three kinds of trie nodes.
type NodeKind int

const (
	// NodeTerminator indicates an empty sub-trie (all-zero node).
	NodeTerminator NodeKind = iota
	// NodeLeaf indicates a leaf node (MSB of byte 0 is 1).
	NodeLeaf
	// NodeInternal indicates an internal (branch) node (MSB of byte 0 is 0, non-zero).
	NodeInternal
)

// NodeKindOf returns the kind of the given node using MSB discrimination.
//
// If the MSB of byte 0 is set, it is a leaf. If the node is all zeros,
// it is a terminator. Otherwise it is an internal node.
func NodeKindOf(n *Node) NodeKind {
	if n[0]>>7 == 1 {
		return NodeLeaf
	}
	if *n == Terminator {
		return NodeTerminator
	}
	return NodeInternal
}

// IsTerminator reports whether the node is the all-zero terminator.
func IsTerminator(n *Node) bool {
	return *n == Terminator
}

// IsLeaf reports whether the node's MSB indicates a leaf.
func IsLeaf(n *Node) bool {
	return n[0]>>7 == 1
}

// IsInternal reports whether the node is a non-terminator internal node.
func IsInternal(n *Node) bool {
	return n[0]>>7 == 0 && *n != Terminator
}

// InternalData holds the preimage of an internal (branch) node.
type InternalData struct {
	Left  Node
	Right Node
}

// LeafData holds the preimage of a leaf node.
type LeafData struct {
	KeyPath   KeyPath
	ValueHash ValueHash
}

// Package core defines the fundamental data structures for a NOMT binary
// merkle trie aligned with EIP-7864. All types are pure computation with
// no I/O dependencies.
package core

// Node is a 256-bit hash representing a node in the binary merkle trie.
// The all-zeros value is reserved as the Terminator (empty sub-trie).
// Unlike the previous NOMT design, there is no MSB tagging — nodes are
// either terminators (zero) or opaque hashes (non-zero).
type Node = [32]byte

// KeyPath is a full 256-bit key (31-byte stem + 1-byte suffix).
// Used for flat state lookups where the full 32-byte key is needed.
type KeyPath = [32]byte

// StemPath is the 248-bit (31-byte) stem portion of a key.
// In the EIP-7864 trie, internal nodes traverse bits 0-247, then
// stem nodes hold 256 value slots indexed by the last byte.
type StemPath = [StemSize]byte

// Terminator is the special node value denoting an empty sub-trie.
// When this appears at a location, no key with a matching path prefix has a value.
var Terminator Node

// NodeKind discriminates the two kinds of trie nodes in the page tree.
type NodeKind int

const (
	// NodeTerminator indicates an empty sub-trie (all-zero node).
	NodeTerminator NodeKind = iota
	// NodeInternal indicates a non-zero hash (internal node or stem hash).
	NodeInternal
)

// NodeKindOf returns the kind of the given node.
// In EIP-7864, the page tree only stores terminators and opaque hashes.
func NodeKindOf(n *Node) NodeKind {
	if *n == Terminator {
		return NodeTerminator
	}
	return NodeInternal
}

// IsTerminator reports whether the node is the all-zero terminator.
func IsTerminator(n *Node) bool {
	return *n == Terminator
}

// InternalData holds the preimage of an internal (branch) node.
type InternalData struct {
	Left  Node
	Right Node
}

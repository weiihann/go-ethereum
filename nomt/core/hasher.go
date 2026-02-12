package core

import "golang.org/x/crypto/sha3"

// HashLeaf computes the hash of a leaf node: keccak256(keyPath || valueHash)
// with the MSB of byte 0 set to 1.
func HashLeaf(data *LeafData) Node {
	h := sha3.NewLegacyKeccak256()
	h.Write(data.KeyPath[:])
	h.Write(data.ValueHash[:])
	var out Node
	h.Sum(out[:0])
	setMSB(&out)
	return out
}

// HashInternal computes the hash of an internal node: keccak256(left || right)
// with the MSB of byte 0 cleared to 0.
func HashInternal(data *InternalData) Node {
	h := sha3.NewLegacyKeccak256()
	h.Write(data.Left[:])
	h.Write(data.Right[:])
	var out Node
	h.Sum(out[:0])
	clearMSB(&out)
	return out
}

// HashValue computes keccak256 of an arbitrary-length value.
func HashValue(value []byte) ValueHash {
	h := sha3.NewLegacyKeccak256()
	h.Write(value)
	var out ValueHash
	h.Sum(out[:0])
	return out
}

// setMSB sets the most significant bit (bit 7 of byte 0) to 1.
func setMSB(n *Node) {
	n[0] |= 0x80
}

// clearMSB clears the most significant bit (bit 7 of byte 0) to 0.
func clearMSB(n *Node) {
	n[0] &= 0x7F
}

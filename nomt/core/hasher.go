package core

import "crypto/sha256"

const (
	// StemSize is the number of bytes in a stem path (248 bits).
	StemSize = 31

	// StemNodeWidth is the number of value slots per stem node.
	StemNodeWidth = 256

	// HashSize is the size of a SHA256 hash in bytes.
	HashSize = 32
)

// HashInternal computes SHA256(left || right) matching EIP-7864's InternalNode.Hash().
func HashInternal(data *InternalData) Node {
	h := sha256.New()
	h.Write(data.Left[:])
	h.Write(data.Right[:])
	var out Node
	h.Sum(out[:0])
	return out
}

// HashStem computes the stem node hash matching EIP-7864's StemNode.Hash().
//
// Algorithm:
//  1. SHA256 each non-nil value to get 256 leaf hashes (nil → zero hash)
//  2. Build an 8-level binary SHA256 tree (256 → 128 → ... → 1 root)
//     Skip pairs where both children are zero (produce zero parent)
//  3. Final hash: SHA256(stem || 0x00 || subtreeRoot)
func HashStem(stem [StemSize]byte, values [StemNodeWidth][]byte) Node {
	var data [StemNodeWidth]Node
	for i, v := range values {
		if v != nil {
			data[i] = sha256.Sum256(v)
		}
	}

	h := sha256.New()
	for level := 1; level <= 8; level++ {
		for i := range StemNodeWidth / (1 << level) {
			if data[i*2] == (Node{}) && data[i*2+1] == (Node{}) {
				data[i] = Node{}
				continue
			}
			h.Reset()
			h.Write(data[i*2][:])
			h.Write(data[i*2+1][:])
			h.Sum(data[i][:0])
		}
	}

	h.Reset()
	h.Write(stem[:])
	h.Write([]byte{0x00})
	h.Write(data[0][:])
	var out Node
	h.Sum(out[:0])
	return out
}

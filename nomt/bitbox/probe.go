package bitbox

import (
	"encoding/binary"

	"github.com/cespare/xxhash/v2"
	"github.com/ethereum/go-ethereum/nomt/core"
)

// HashPageID computes the xxhash64 of seed||encodedPageID.
func HashPageID(seed [16]byte, pageID core.PageID) uint64 {
	encoded := pageID.Encode()
	var buf [48]byte
	copy(buf[:16], seed[:])
	copy(buf[16:], encoded[:])
	return xxhash.Sum64(buf[:])
}

// HashPageIDBytes computes the xxhash64 from seed and raw encoded page ID.
func HashPageIDBytes(seed [16]byte, encodedPageID [32]byte) uint64 {
	var buf [48]byte
	copy(buf[:16], seed[:])
	copy(buf[16:], encodedPageID[:])
	return xxhash.Sum64(buf[:])
}

// HashSeedFromBytes creates a [16]byte seed from a byte slice.
func HashSeedFromBytes(b []byte) [16]byte {
	var seed [16]byte
	copy(seed[:], b)
	return seed
}

// HashSeedFromUint64 creates a deterministic seed from two uint64 values.
func HashSeedFromUint64(a, b uint64) [16]byte {
	var seed [16]byte
	binary.LittleEndian.PutUint64(seed[:8], a)
	binary.LittleEndian.PutUint64(seed[8:], b)
	return seed
}

// ProbeSequence implements triangular probing over the hash table.
//
// Bucket(step) = (initial + step*(step+1)/2) mod capacity
//
// With a power-of-2 capacity, triangular probing visits every bucket before
// repeating, guaranteeing termination.
type ProbeSequence struct {
	hash     uint64
	bucket   uint64
	step     uint64
	capacity uint64
}

// NewProbeSequence creates a new probe sequence for the given hash and
// capacity. The capacity MUST be a power of 2.
func NewProbeSequence(hash, capacity uint64) ProbeSequence {
	initial := hash % capacity
	return ProbeSequence{
		hash:     hash,
		bucket:   initial,
		step:     0,
		capacity: capacity,
	}
}

// Bucket returns the current bucket index.
func (p *ProbeSequence) Bucket() uint64 {
	return p.bucket
}

// Hash returns the hash used to seed this probe.
func (p *ProbeSequence) Hash() uint64 {
	return p.hash
}

// Next advances to the next bucket in the triangular probe sequence.
func (p *ProbeSequence) Next() {
	p.step++
	p.bucket = (p.bucket + p.step) % p.capacity
}

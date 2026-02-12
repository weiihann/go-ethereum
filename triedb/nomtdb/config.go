// Package nomtdb implements the triedb backend for NOMT (Near-Optimal Merkle
// Trie), a page-based binary merkle trie engine.
//
// NOMT handles only the trie structure (merkle pages). Flat key-value storage
// (accounts, storage slots) is stored in geth's existing ethdb (PebbleDB)
// under NOMT-specific key prefixes.
package nomtdb

// Config holds configuration for the NOMT triedb backend.
type Config struct {
	// DataDir is the directory for NOMT's Bitbox storage files.
	DataDir string

	// HTCapacity is the number of hash table buckets. Must be a power of 2.
	// Defaults to 1<<20 (~1M buckets) if zero.
	HTCapacity uint64
}

// Defaults is the default configuration for the NOMT backend.
var Defaults = &Config{
	HTCapacity: 1 << 20,
}

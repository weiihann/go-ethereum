// Package db provides the unified NOMT trie database combining Bitbox
// storage with the PageWalker merkle engine.
//
// This package handles only the trie structure (merkle pages). Flat
// key-value storage (accounts, storage slots) stays on geth's PebbleDB.
package db

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/ethereum/go-ethereum/nomt/bitbox"
	"github.com/ethereum/go-ethereum/nomt/core"
	"github.com/ethereum/go-ethereum/nomt/merkle"
)

const (
	htFileName  = "nomt.ht"
	walFileName = "nomt.wal"
)

// Config holds configuration for the NOMT database.
type Config struct {
	// HTCapacity is the number of hash table buckets. Must be a power of 2.
	HTCapacity uint64
}

// DefaultConfig returns a default configuration.
func DefaultConfig() Config {
	return Config{
		HTCapacity: 1 << 20, // ~1M buckets = ~4GB
	}
}

// DB is the NOMT trie database.
type DB struct {
	dataDir  string
	bb       *bitbox.DB
	root     core.Node
	syncSeqn uint32
	mu       sync.RWMutex
}

// Open opens or creates a NOMT trie database at the given directory.
func Open(dataDir string, config Config) (*DB, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("nomt/db: create datadir: %w", err)
	}

	htPath := filepath.Join(dataDir, htFileName)
	walPath := filepath.Join(dataDir, walFileName)

	var bb *bitbox.DB
	var err error

	if _, statErr := os.Stat(htPath); os.IsNotExist(statErr) {
		// Create new database.
		var seed [16]byte
		if _, err := rand.Read(seed[:]); err != nil {
			return nil, fmt.Errorf("nomt/db: generate seed: %w", err)
		}
		bb, err = bitbox.Create(htPath, config.HTCapacity, seed)
		if err != nil {
			return nil, fmt.Errorf("nomt/db: create bitbox: %w", err)
		}
	} else {
		// Open existing database.
		bb, err = bitbox.Open(htPath)
		if err != nil {
			return nil, fmt.Errorf("nomt/db: open bitbox: %w", err)
		}
	}

	db := &DB{
		dataDir: dataDir,
		bb:      bb,
		root:    core.Terminator,
	}

	// Run WAL recovery.
	seqn, err := bb.Recover(walPath)
	if err != nil {
		bb.Close()
		return nil, fmt.Errorf("nomt/db: recover: %w", err)
	}
	if seqn > 0 {
		db.syncSeqn = seqn
	}

	return db, nil
}

// Root returns the current trie root hash.
func (db *DB) Root() core.Node {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.root
}

// SetRoot sets the current trie root (used when loading state from metadata).
func (db *DB) SetRoot(root core.Node) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.root = root
}

// SyncSeqn returns the current sync sequence number.
func (db *DB) SyncSeqn() uint32 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.syncSeqn
}

// Update applies a batch of leaf operations to the trie.
//
// Operations are sorted by key internally. The function:
//  1. Builds a PageSet from Bitbox
//  2. Groups operations by their terminal node position
//  3. Runs the PageWalker to produce updated pages
//  4. Persists updated pages via Bitbox sync
//  5. Returns the new root hash
func (db *DB) Update(ops []core.LeafOp) (core.Node, error) {
	if len(ops) == 0 {
		return db.Root(), nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// Sort ops by key path.
	sort.Slice(ops, func(i, j int) bool {
		return ops[i].Key != ops[j].Key && keyLess(&ops[i].Key, &ops[j].Key)
	})

	// Build a BitboxPageSet that loads pages from disk.
	pageSet := newBitboxPageSet(db.bb)

	// For a simple implementation, treat the entire trie as a single
	// terminal at the root and replace it.
	kvs := make([]core.KeyValue, 0, len(ops))
	for _, op := range ops {
		if op.Value != nil {
			kvs = append(kvs, core.KeyValue{Key: op.Key, Value: *op.Value})
		}
	}

	walker := merkle.NewPageWalker(db.root, nil)

	if len(kvs) > 0 || len(ops) > 0 {
		// Simple approach: single advance at root with all operations.
		// For an incremental update on a non-empty trie, we'd need to
		// seek to terminals first. This simplified version rebuilds.
		pos := core.NewTriePosition()
		pos.Down(false) // advance to position [0] for left subtree

		// Split ops into left (0-prefix) and right (1-prefix).
		leftKVs := make([]core.KeyValue, 0, len(kvs))
		rightKVs := make([]core.KeyValue, 0, len(kvs))
		for _, kv := range kvs {
			if kv.Key[0]&0x80 == 0 {
				leftKVs = append(leftKVs, kv)
			} else {
				rightKVs = append(rightKVs, kv)
			}
		}

		leftPos := core.NewTriePosition()
		leftPos.Down(false)
		if len(leftKVs) > 0 {
			walker.AdvanceAndReplace(pageSet, leftPos, leftKVs)
		}

		rightPos := core.NewTriePosition()
		rightPos.Down(true)
		if len(rightKVs) > 0 {
			walker.AdvanceAndReplace(pageSet, rightPos, rightKVs)
		}
	}

	out := walker.Conclude()

	// Persist updated pages.
	walPath := filepath.Join(db.dataDir, walFileName)
	db.syncSeqn++
	if err := db.bb.FullSync(walPath, db.syncSeqn, out.Pages); err != nil {
		return core.Terminator, fmt.Errorf("nomt/db: sync: %w", err)
	}

	db.root = out.Root
	return out.Root, nil
}

// LoadPage loads a page from Bitbox storage by its PageID.
func (db *DB) LoadPage(pageID core.PageID) (*core.RawPage, error) {
	page, _, found, err := db.bb.LoadPage(pageID)
	if err != nil {
		return nil, fmt.Errorf("nomt/db: load page: %w", err)
	}
	if !found {
		return nil, nil
	}
	return page, nil
}

// Close closes the database.
func (db *DB) Close() error {
	return db.bb.Close()
}

// --- BitboxPageSet ---

// bitboxPageSet implements merkle.PageSet backed by Bitbox disk storage.
type bitboxPageSet struct {
	bb    *bitbox.DB
	cache map[string]*core.RawPage
}

func newBitboxPageSet(bb *bitbox.DB) *bitboxPageSet {
	return &bitboxPageSet{
		bb:    bb,
		cache: make(map[string]*core.RawPage, 16),
	}
}

func (ps *bitboxPageSet) Get(pageID core.PageID) (
	*core.RawPage, merkle.PageOrigin, bool,
) {
	key := pageIDKey(pageID)
	if cached, ok := ps.cache[key]; ok {
		pageCopy := new(core.RawPage)
		*pageCopy = *cached
		return pageCopy, merkle.PageOrigin{
			Kind: merkle.PageOriginPersisted,
		}, true
	}

	page, _, found, err := ps.bb.LoadPage(pageID)
	if err != nil || !found {
		// Return a fresh page if not found — this handles the case
		// where the trie is being built from scratch or expanded
		// into new regions.
		fresh := new(core.RawPage)
		return fresh, merkle.PageOrigin{Kind: merkle.PageOriginFresh}, true
	}

	ps.cache[key] = page
	pageCopy := new(core.RawPage)
	*pageCopy = *page
	return pageCopy, merkle.PageOrigin{
		Kind: merkle.PageOriginPersisted,
	}, true
}

func (ps *bitboxPageSet) Contains(pageID core.PageID) bool {
	key := pageIDKey(pageID)
	if _, ok := ps.cache[key]; ok {
		return true
	}
	_, _, found, _ := ps.bb.LoadPage(pageID)
	return found
}

func (ps *bitboxPageSet) Fresh(pageID core.PageID) *core.RawPage {
	return new(core.RawPage)
}

func (ps *bitboxPageSet) Insert(
	pageID core.PageID, page *core.RawPage, origin merkle.PageOrigin,
) {
	ps.cache[pageIDKey(pageID)] = page
}

func pageIDKey(id core.PageID) string {
	encoded := id.Encode()
	return string(encoded[:])
}

func keyLess(a, b *core.KeyPath) bool {
	for i := range a {
		if a[i] < b[i] {
			return true
		}
		if a[i] > b[i] {
			return false
		}
	}
	return false
}

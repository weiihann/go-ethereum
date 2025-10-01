package trie

import (
	"maps"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

type AccessTrie struct {
	Nodes map[common.Hash]map[string]struct{}
	rw    sync.RWMutex
}

func NewAccessTrie() *AccessTrie {
	return &AccessTrie{
		Nodes: make(map[common.Hash]map[string]struct{}),
	}
}

func (t *AccessTrie) AddNode(owner common.Hash, path string) {
	t.rw.Lock()
	defer t.rw.Unlock()

	t.addNode(owner, path)
}

func (t *AccessTrie) addNode(owner common.Hash, path string) {
	if _, ok := t.Nodes[owner]; !ok {
		t.Nodes[owner] = make(map[string]struct{})
	}
	if _, ok := t.Nodes[owner][path]; ok {
		return
	}
	t.Nodes[owner][path] = struct{}{}
}

func (t *AccessTrie) AddNodeSet(nodes *trienode.NodeSet) {
	t.rw.Lock()
	defer t.rw.Unlock()

	owner := nodes.Owner
	for path, node := range nodes.Nodes {
		if !node.IsDeleted() {
			t.addNode(owner, path)
		}
	}
}

func (t *AccessTrie) Commit(db ethdb.KeyValueStore) {
	t.rw.Lock()
	defer t.rw.Unlock()

	dbw := db.NewBatch()
	for owner, nodes := range t.Nodes {
		if owner == (common.Hash{}) {
			for path := range nodes {
				rawdb.WriteAccessNodeAccount(dbw, []byte(path))
			}
		} else {
			for path := range nodes {
				rawdb.WriteAccessNodeSlot(dbw, owner, []byte(path))
			}
		}
	}

	if err := dbw.Write(); err != nil {
		log.Crit("Failed to write access trie", "err", err)
	}
}

func (t *AccessTrie) Reset() {
	t.rw.Lock()
	defer t.rw.Unlock()

	t.Nodes = make(map[common.Hash]map[string]struct{})
}

func (t *AccessTrie) Copy() *AccessTrie {
	t.rw.RLock()
	defer t.rw.RUnlock()

	// Create new AccessTrie with deep-copied Nodes
	copied := &AccessTrie{
		Nodes: make(map[common.Hash]map[string]struct{}, len(t.Nodes)),
	}

	// Deep copy each inner map
	for owner, paths := range t.Nodes {
		copied.Nodes[owner] = maps.Clone(paths)
	}

	return copied
}

package trie

import (
	"maps"
	"sync"

	"github.com/ethereum/go-ethereum/common"
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
			t.AddNode(owner, path)
		}
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

	return &AccessTrie{
		Nodes: maps.Clone(t.Nodes),
	}
}

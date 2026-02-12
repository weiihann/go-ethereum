// Package nomttrie implements a state.Trie backed by the NOMT binary merkle
// trie engine, targeting EIP-7864 compatibility.
//
// Read operations delegate to geth's ethdb flat state. Write operations
// accumulate stem updates and flush them to the NOMT page tree on Hash()/Commit().
package nomttrie

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/nomt/db"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb/nomtdb"
)

// stemUpdate represents a pending value change at a specific (stem, suffix)
// position in the EIP-7864 trie.
type stemUpdate struct {
	Stem   [31]byte // stem path
	Suffix byte     // value slot index (0-255)
	Value  []byte   // 32-byte value, nil = delete
}

// NomtTrie implements the state.Trie interface using NOMT's page-based binary
// merkle trie. It accumulates stem updates during block execution and flushes
// them to the NOMT engine on Hash()/Commit().
type NomtTrie struct {
	nomtDB  *db.DB           // NOMT trie engine (page storage + walker)
	backend *nomtdb.Database // NOMT triedb backend (for flat state access)
	root    common.Hash      // current trie root
	pending []stemUpdate     // accumulated stem updates
	dirty   bool             // whether pending updates exist
}

// New creates a new NomtTrie. The root parameter is the current state root.
func New(root common.Hash, backend *nomtdb.Database) (*NomtTrie, error) {
	return &NomtTrie{
		nomtDB:  backend.NomtDB(),
		backend: backend,
		root:    root,
		pending: make([]stemUpdate, 0, 64),
	}, nil
}

// GetKey returns the sha3 preimage of a hashed key.
func (t *NomtTrie) GetKey(key []byte) []byte {
	return key
}

// GetAccount reads an account from flat state storage.
// TODO(Phase E): implement using EIP-7864 key encoding.
func (t *NomtTrie) GetAccount(_ common.Address) (*types.StateAccount, error) {
	return nil, nil
}

// PrefetchAccount is a no-op.
func (t *NomtTrie) PrefetchAccount(_ []common.Address) error {
	return nil
}

// GetStorage reads a storage slot from flat state storage.
// TODO(Phase E): implement using EIP-7864 key encoding.
func (t *NomtTrie) GetStorage(_ common.Address, _ []byte) ([]byte, error) {
	return nil, nil
}

// PrefetchStorage is a no-op.
func (t *NomtTrie) PrefetchStorage(_ common.Address, _ [][]byte) error {
	return nil
}

// UpdateAccount encodes the account and queues stem updates.
// TODO(Phase E): implement using packBasicData + stem grouping.
func (t *NomtTrie) UpdateAccount(_ common.Address, _ *types.StateAccount, _ int) error {
	return nil
}

// UpdateStorage queues a storage value update.
// TODO(Phase E): implement using storageStemAndSuffix + packStorageValue.
func (t *NomtTrie) UpdateStorage(_ common.Address, _, _ []byte) error {
	return nil
}

// DeleteAccount queues deletion of account values.
// TODO(Phase E): implement.
func (t *NomtTrie) DeleteAccount(_ common.Address) error {
	return nil
}

// DeleteStorage queues deletion of a storage slot.
// TODO(Phase E): implement.
func (t *NomtTrie) DeleteStorage(_ common.Address, _ []byte) error {
	return nil
}

// UpdateContractCode queues code chunk updates.
// TODO(Phase E): implement using ChunkifyCode + codeChunkStemAndSuffix.
func (t *NomtTrie) UpdateContractCode(_ common.Address, _ common.Hash, _ []byte) error {
	return nil
}

// Hash returns the current root hash. Flushes pending updates first.
// TODO(Phase E): implement using groupAndHashStems + nomtDB.Update.
func (t *NomtTrie) Hash() common.Hash {
	return t.root
}

// Commit flushes pending operations and returns the root hash.
func (t *NomtTrie) Commit(_ bool) (common.Hash, *trienode.NodeSet) {
	root := t.Hash()
	return root, trienode.NewNodeSet(common.Hash{})
}

// Witness returns accessed trie nodes. Not yet implemented.
func (t *NomtTrie) Witness() map[string][]byte {
	return nil
}

// NodeIterator returns an iterator over trie nodes. Not yet implemented.
func (t *NomtTrie) NodeIterator(_ []byte) (trie.NodeIterator, error) {
	return nil, nil
}

// Prove constructs a merkle proof. Not yet implemented.
func (t *NomtTrie) Prove(_ []byte, _ ethdb.KeyValueWriter) error {
	return nil
}

// IsVerkle returns true — NOMT uses EIP-7864's single-trie semantics
// which requires the verkle-like statedb path (no separate storage tries).
func (t *NomtTrie) IsVerkle() bool {
	return true
}

// Copy creates a deep copy of the trie.
func (t *NomtTrie) Copy() *NomtTrie {
	pending := make([]stemUpdate, len(t.pending))
	copy(pending, t.pending)
	return &NomtTrie{
		nomtDB:  t.nomtDB,
		backend: t.backend,
		root:    t.root,
		pending: pending,
		dirty:   t.dirty,
	}
}

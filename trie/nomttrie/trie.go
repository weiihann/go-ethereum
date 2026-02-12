// Package nomttrie implements a state.Trie backed by the NOMT binary merkle
// trie engine. It accumulates leaf operations during block execution and
// commits them to the NOMT page-based storage on Hash()/Commit().
//
// Read operations (GetAccount, GetStorage) delegate to geth's ethdb flat state
// since NOMT's trie stores only hashes, not actual values.
package nomttrie

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/nomt/core"
	"github.com/ethereum/go-ethereum/nomt/db"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb/nomtdb"
)

// NomtTrie implements the state.Trie interface using NOMT's page-based binary
// merkle trie. It accumulates changes as LeafOps during block execution and
// flushes them to the NOMT engine on Hash()/Commit().
type NomtTrie struct {
	nomtDB  *db.DB           // NOMT trie engine (page storage + walker)
	backend *nomtdb.Database // NOMT triedb backend (for flat state access)
	root    common.Hash      // current trie root as common.Hash
	pending []core.LeafOp    // accumulated leaf operations
	dirty   bool             // whether pending ops exist
}

// New creates a new NomtTrie. The root parameter is the current state root.
// If root is empty or the zero hash, the trie starts empty.
func New(root common.Hash, backend *nomtdb.Database) (*NomtTrie, error) {
	return &NomtTrie{
		nomtDB:  backend.NomtDB(),
		backend: backend,
		root:    root,
		pending: make([]core.LeafOp, 0, 64),
	}, nil
}

// GetKey returns the sha3 preimage of a hashed key. NOMT doesn't maintain
// preimage mappings; returns the key as-is.
func (t *NomtTrie) GetKey(key []byte) []byte {
	return key
}

// GetAccount reads an account from flat state storage (ethdb).
// NOMT's trie stores only hashes, so actual data comes from flat KV.
func (t *NomtTrie) GetAccount(address common.Address) (*types.StateAccount, error) {
	data, err := t.backend.DiskDB().Get(nomtdb.NomtAccountKey(crypto.Keccak256Hash(address.Bytes())))
	if err != nil {
		return nil, nil // not found
	}
	if len(data) == 0 {
		return nil, nil
	}
	return types.FullAccount(data)
}

// PrefetchAccount is a no-op for NOMT — flat state reads are fast by design.
func (t *NomtTrie) PrefetchAccount(addresses []common.Address) error {
	return nil
}

// GetStorage reads a storage slot from flat state storage (ethdb).
func (t *NomtTrie) GetStorage(addr common.Address, key []byte) ([]byte, error) {
	addrHash := crypto.Keccak256Hash(addr.Bytes())
	slotHash := crypto.Keccak256Hash(key)
	data, err := t.backend.DiskDB().Get(nomtdb.NomtStorageKey(addrHash, slotHash))
	if err != nil {
		return nil, nil // not found
	}
	return data, nil
}

// PrefetchStorage is a no-op for NOMT.
func (t *NomtTrie) PrefetchStorage(addr common.Address, keys [][]byte) error {
	return nil
}

// UpdateAccount encodes the account and adds a leaf op to the pending batch.
// The trie keypath is keccak256(address), value hash is keccak256(slimRLP).
func (t *NomtTrie) UpdateAccount(address common.Address, account *types.StateAccount, codeLen int) error {
	slimData := types.SlimAccountRLP(*account)
	keyPath := crypto.Keccak256Hash(address.Bytes())
	valueHash := crypto.Keccak256Hash(slimData)

	var kp core.KeyPath
	copy(kp[:], keyPath[:])
	var vh core.ValueHash
	copy(vh[:], valueHash[:])

	t.pending = append(t.pending, core.LeafOp{
		Key:   kp,
		Value: &vh,
	})
	t.dirty = true
	return nil
}

// UpdateStorage adds a storage update leaf op to the pending batch.
// The trie keypath is keccak256(keccak256(address) || keccak256(slot)),
// value hash is keccak256(value).
func (t *NomtTrie) UpdateStorage(addr common.Address, key, value []byte) error {
	kp := storageKeyPath(addr, key)

	if len(value) == 0 {
		// Empty value = delete.
		t.pending = append(t.pending, core.LeafOp{
			Key:   kp,
			Value: nil,
		})
	} else {
		valueHash := crypto.Keccak256Hash(value)
		var vh core.ValueHash
		copy(vh[:], valueHash[:])
		t.pending = append(t.pending, core.LeafOp{
			Key:   kp,
			Value: &vh,
		})
	}
	t.dirty = true
	return nil
}

// DeleteAccount adds a delete leaf op for the account.
func (t *NomtTrie) DeleteAccount(address common.Address) error {
	keyPath := crypto.Keccak256Hash(address.Bytes())
	var kp core.KeyPath
	copy(kp[:], keyPath[:])

	t.pending = append(t.pending, core.LeafOp{
		Key:   kp,
		Value: nil,
	})
	t.dirty = true
	return nil
}

// DeleteStorage adds a delete leaf op for a storage slot.
func (t *NomtTrie) DeleteStorage(addr common.Address, key []byte) error {
	kp := storageKeyPath(addr, key)
	t.pending = append(t.pending, core.LeafOp{
		Key:   kp,
		Value: nil,
	})
	t.dirty = true
	return nil
}

// UpdateContractCode is a no-op for NOMT — code is stored in ethdb by the
// state layer, not in the trie.
func (t *NomtTrie) UpdateContractCode(address common.Address, codeHash common.Hash, code []byte) error {
	return nil
}

// Hash returns the current root hash. If there are pending operations, they
// are flushed to the NOMT engine first.
func (t *NomtTrie) Hash() common.Hash {
	if !t.dirty || len(t.pending) == 0 {
		return t.root
	}
	newRoot, err := t.nomtDB.Update(t.pending)
	if err != nil {
		// On error, return current root without updating.
		return t.root
	}
	t.root = common.Hash(newRoot)
	t.pending = t.pending[:0]
	t.dirty = false
	return t.root
}

// Commit flushes pending operations and returns the root hash.
// The returned NodeSet is empty since NOMT uses page-based storage, not
// individual node persistence.
func (t *NomtTrie) Commit(collectLeaf bool) (common.Hash, *trienode.NodeSet) {
	root := t.Hash()
	return root, trienode.NewNodeSet(common.Hash{})
}

// Witness returns accessed trie nodes. NOMT doesn't track witnesses yet.
func (t *NomtTrie) Witness() map[string][]byte {
	return nil
}

// NodeIterator returns an iterator over trie nodes. Not yet implemented.
func (t *NomtTrie) NodeIterator(startKey []byte) (trie.NodeIterator, error) {
	return nil, nil
}

// Prove constructs a merkle proof. Not yet implemented.
func (t *NomtTrie) Prove(key []byte, proofDb ethdb.KeyValueWriter) error {
	return nil
}

// IsVerkle returns false — NOMT is a binary merkle trie, not verkle.
func (t *NomtTrie) IsVerkle() bool {
	return false
}

// Copy creates a deep copy of the trie.
func (t *NomtTrie) Copy() *NomtTrie {
	pending := make([]core.LeafOp, len(t.pending))
	copy(pending, t.pending)
	return &NomtTrie{
		nomtDB:  t.nomtDB,
		backend: t.backend,
		root:    t.root,
		pending: pending,
		dirty:   t.dirty,
	}
}

// storageKeyPath computes the NOMT keypath for a storage slot.
// Format: keccak256(keccak256(address) || keccak256(slot))
func storageKeyPath(addr common.Address, slot []byte) core.KeyPath {
	addrHash := crypto.Keccak256Hash(addr.Bytes())
	slotHash := crypto.Keccak256Hash(slot)
	combined := crypto.Keccak256Hash(addrHash.Bytes(), slotHash.Bytes())
	var kp core.KeyPath
	copy(kp[:], combined[:])
	return kp
}

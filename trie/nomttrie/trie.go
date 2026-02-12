// Package nomttrie implements a state.Trie backed by the NOMT binary merkle
// trie engine, targeting EIP-7864 compatibility.
//
// Read operations delegate to geth's ethdb flat state (stem value keys).
// Write operations accumulate stem updates and flush them to flat state + the
// NOMT page tree on Hash()/Commit().
package nomttrie

import (
	"encoding/binary"
	"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/nomt/core"
	"github.com/ethereum/go-ethereum/nomt/db"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/bintrie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb/nomtdb"
	"github.com/holiman/uint256"
)

// stemUpdate represents a pending value change at a specific (stem, suffix)
// position in the EIP-7864 trie.
type stemUpdate struct {
	Stem   core.StemPath // 31-byte stem path
	Suffix byte          // value slot index (0-255)
	Value  []byte        // 32-byte value, nil = delete
}

// NomtTrie implements the state.Trie interface using NOMT's page-based binary
// merkle trie. It accumulates stem updates during block execution and flushes
// them to the NOMT engine on Hash()/Commit().
//
// The canonical root hash is computed via BuildInternalTree(skip=0) over all
// stems, producing roots identical to bintrie. The NOMT page tree is updated
// separately for persistent storage (its internal root may differ due to the
// depth-7 worker split, but the canonical root returned by Hash() matches
// bintrie exactly).
type NomtTrie struct {
	nomtDB  *db.DB           // NOMT trie engine (page storage + walker)
	backend *nomtdb.Database // NOMT triedb backend (for flat state access)
	root    common.Hash      // current canonical trie root
	pending []stemUpdate     // accumulated stem updates
	dirty   bool             // whether pending updates exist

	// allStems tracks the stem hash for every active stem in the trie.
	// Updated on each Hash() with results from groupAndHashStems.
	// Used to compute the canonical root via BuildInternalTree(skip=0).
	allStems map[core.StemPath]core.Node
}

// New creates a new NomtTrie. The root parameter is the current state root.
func New(root common.Hash, backend *nomtdb.Database) (*NomtTrie, error) {
	return &NomtTrie{
		nomtDB:   backend.NomtDB(),
		backend:  backend,
		root:     root,
		pending:  make([]stemUpdate, 0, 64),
		allStems: make(map[core.StemPath]core.Node, 64),
	}, nil
}

// GetKey returns the sha3 preimage of a hashed key.
func (t *NomtTrie) GetKey(key []byte) []byte {
	return key
}

// GetAccount reads an account from flat state using EIP-7864 stem keys.
// Reads basic data (slot 0) and code hash (slot 1) from the account stem.
func (t *NomtTrie) GetAccount(addr common.Address) (*types.StateAccount, error) {
	stem := accountStem(addr)
	diskdb := t.backend.DiskDB()

	basicData, err := diskdb.Get(stemValueDBKey(stem, bintrie.BasicDataLeafKey))
	if err != nil {
		basicData = nil
	}
	codeHash, err := diskdb.Get(stemValueDBKey(stem, bintrie.CodeHashLeafKey))
	if err != nil {
		codeHash = nil
	}

	if basicData == nil && codeHash == nil {
		return nil, nil
	}

	acc := &types.StateAccount{
		Balance: new(uint256.Int),
	}

	// Unpack basic data: nonce at [8:16], balance at [16:32].
	if len(basicData) >= bintrie.HashSize {
		acc.Nonce = binary.BigEndian.Uint64(
			basicData[bintrie.BasicDataNonceOffset:],
		)
		var balance [16]byte
		copy(balance[:], basicData[bintrie.BasicDataBalanceOffset:])
		acc.Balance = new(uint256.Int).SetBytes(balance[:])
	}

	if len(codeHash) > 0 {
		acc.CodeHash = make([]byte, len(codeHash))
		copy(acc.CodeHash, codeHash)
	}

	return acc, nil
}

// PrefetchAccount is a no-op for NOMT (flat state reads are already fast).
func (t *NomtTrie) PrefetchAccount(_ []common.Address) error {
	return nil
}

// GetStorage reads a storage slot from flat state using EIP-7864 stem keys.
func (t *NomtTrie) GetStorage(addr common.Address, key []byte) ([]byte, error) {
	stem, suffix := storageStemAndSuffix(addr, key)
	data, err := t.backend.DiskDB().Get(stemValueDBKey(stem, suffix))
	if err != nil {
		return nil, nil
	}
	return data, nil
}

// PrefetchStorage is a no-op for NOMT.
func (t *NomtTrie) PrefetchStorage(_ common.Address, _ [][]byte) error {
	return nil
}

// UpdateAccount encodes account metadata and queues stem updates for basic
// data (slot 0) and code hash (slot 1) matching bintrie.UpdateAccount.
func (t *NomtTrie) UpdateAccount(addr common.Address, acc *types.StateAccount, codeLen int) error {
	stem := accountStem(addr)

	basicData := packBasicData(acc, codeLen)
	t.pending = append(t.pending, stemUpdate{
		Stem:   stem,
		Suffix: bintrie.BasicDataLeafKey,
		Value:  basicData[:],
	})

	codeHashVal := make([]byte, bintrie.HashSize)
	copy(codeHashVal, acc.CodeHash)
	t.pending = append(t.pending, stemUpdate{
		Stem:   stem,
		Suffix: bintrie.CodeHashLeafKey,
		Value:  codeHashVal,
	})

	t.dirty = true
	return nil
}

// UpdateStorage queues a storage value update. The value is right-aligned
// and padded to 32 bytes, matching bintrie.UpdateStorage.
func (t *NomtTrie) UpdateStorage(addr common.Address, key, value []byte) error {
	stem, suffix := storageStemAndSuffix(addr, key)
	v := packStorageValue(value)

	t.pending = append(t.pending, stemUpdate{
		Stem:   stem,
		Suffix: suffix,
		Value:  v[:],
	})

	t.dirty = true
	return nil
}

// DeleteAccount is a no-op, matching bintrie behavior.
func (t *NomtTrie) DeleteAccount(_ common.Address) error {
	return nil
}

// DeleteStorage queues a zero-value write for the storage slot,
// matching bintrie.DeleteStorage which inserts 32 zero bytes.
func (t *NomtTrie) DeleteStorage(addr common.Address, key []byte) error {
	stem, suffix := storageStemAndSuffix(addr, key)
	t.pending = append(t.pending, stemUpdate{
		Stem:   stem,
		Suffix: suffix,
		Value:  make([]byte, bintrie.HashSize),
	})
	t.dirty = true
	return nil
}

// UpdateContractCode chunks the bytecode using EIP-7864's ChunkifyCode and
// queues stem updates for each chunk, matching bintrie's group-based key
// derivation exactly. Chunks are grouped into stems of 256 slots; the stem
// key is computed only at group boundaries, and groupOffset is the suffix.
func (t *NomtTrie) UpdateContractCode(addr common.Address, _ common.Hash, code []byte) error {
	chunks := bintrie.ChunkifyCode(code)
	var stem core.StemPath
	for i, chunknr := 0, uint64(0); i < len(chunks); i, chunknr = i+bintrie.HashSize, chunknr+1 {
		groupOffset := byte((chunknr + 128) % bintrie.StemNodeWidth)
		if groupOffset == 0 || chunknr == 0 {
			var offset [bintrie.HashSize]byte
			binary.LittleEndian.PutUint64(offset[24:], chunknr+128)
			key := bintrie.GetBinaryTreeKey(addr, offset[:])
			copy(stem[:], key[:core.StemSize])
		}
		val := make([]byte, bintrie.HashSize)
		copy(val, chunks[i:i+bintrie.HashSize])
		t.pending = append(t.pending, stemUpdate{
			Stem:   stem,
			Suffix: groupOffset,
			Value:  val,
		})
	}
	if len(chunks) > 0 {
		t.dirty = true
	}
	return nil
}

// Hash flushes pending updates to flat state and the NOMT page tree,
// returning the new trie root hash.
//
// The canonical root is computed via BuildInternalTree(skip=0) over all known
// stems, producing roots identical to bintrie (EIP-7864). The NOMT page tree
// is also updated for persistent storage.
func (t *NomtTrie) Hash() common.Hash {
	if !t.dirty {
		return t.root
	}

	stemKVs, err := groupAndHashStems(t.pending, t.backend.DiskDB())
	if err != nil {
		log.Error("NOMT groupAndHashStems failed", "err", err)
		return t.root
	}

	// Update allStems with new/changed stem hashes.
	for _, kv := range stemKVs {
		t.allStems[kv.Stem] = kv.Hash
	}

	// Update the page tree for persistent storage.
	if len(stemKVs) > 0 {
		if _, err := t.nomtDB.Update(stemKVs); err != nil {
			log.Error("NOMT page tree update failed", "err", err)
			return t.root
		}
	}

	// Compute the canonical root via BuildInternalTree(skip=0).
	// This produces roots identical to bintrie by avoiding the depth-7
	// worker split that adds extra wrapping levels.
	t.root = common.Hash(t.canonicalRoot())

	t.pending = t.pending[:0]
	t.dirty = false
	return t.root
}

// canonicalRoot computes the bintrie-compatible root hash from all known stems
// using BuildInternalTree at skip=0.
func (t *NomtTrie) canonicalRoot() core.Node {
	if len(t.allStems) == 0 {
		return core.Terminator
	}
	sorted := make([]core.StemKeyValue, 0, len(t.allStems))
	for stem, hash := range t.allStems {
		sorted = append(sorted, core.StemKeyValue{Stem: stem, Hash: hash})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return stemLess(&sorted[i].Stem, &sorted[j].Stem)
	})
	return core.BuildInternalTree(0, sorted, func(_ core.WriteNode) {})
}

// Commit flushes pending operations and returns the root hash.
func (t *NomtTrie) Commit(_ bool) (common.Hash, *trienode.NodeSet) {
	root := t.Hash()
	return root, trienode.NewNodeSet(common.Hash{})
}

// Witness returns accessed trie nodes. Not yet implemented for NOMT.
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
	allStems := make(map[core.StemPath]core.Node, len(t.allStems))
	for k, v := range t.allStems {
		allStems[k] = v
	}
	return &NomtTrie{
		nomtDB:   t.nomtDB,
		backend:  t.backend,
		root:     t.root,
		pending:  pending,
		dirty:    t.dirty,
		allStems: allStems,
	}
}

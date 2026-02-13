// Package nomttrie implements a state.Trie backed by the NOMT binary merkle
// trie engine, targeting EIP-7864 compatibility.
//
// Read operations delegate to geth's ethdb flat state (stem value keys).
// Write operations accumulate stem updates and flush them to flat state + the
// NOMT page tree on Hash()/Commit().
package nomttrie

import (
	"bytes"
	"encoding/binary"

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

	// allStems tracks the stem hash for every active stem in the trie,
	// kept sorted by stem path. Updated on each Hash() via sorted merge
	// with results from groupAndHashStems.
	allStems []core.StemKeyValue

	// mergeBuf is reused across Hash() calls to avoid allocating a new
	// slice on every merge. After merge, allStems and mergeBuf swap roles.
	mergeBuf []core.StemKeyValue
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

	// Merge sorted stemKVs into allStems (both are sorted by stem path).
	// Swap allStems and mergeBuf to reuse backing arrays across calls.
	merged := mergeStemKVs(t.allStems, stemKVs, t.mergeBuf)
	t.mergeBuf = t.allStems
	t.allStems = merged

	// Update the page tree for persistent storage.
	// stemKVs is already sorted, so skip the redundant sort in db.Update.
	if len(stemKVs) > 0 {
		if _, err := t.nomtDB.Update(stemKVs); err != nil {
			log.Error("NOMT page tree update failed", "err", err)
			return t.root
		}
	}

	// Compute the canonical root via BuildInternalTree(skip=0).
	// allStems is already sorted, so no additional sort needed.
	t.root = common.Hash(t.canonicalRoot())

	t.pending = t.pending[:0]
	t.dirty = false
	return t.root
}

// canonicalRoot computes the bintrie-compatible root hash from all known stems
// using BuildInternalTree at skip=0. allStems is already sorted.
func (t *NomtTrie) canonicalRoot() core.Node {
	if len(t.allStems) == 0 {
		return core.Terminator
	}
	return core.BuildInternalTree(0, t.allStems, func(_ core.WriteNode) {})
}

// mergeStemKVs merges sorted new stemKVs into sorted existing allStems.
// Existing entries with the same stem are replaced. The result is sorted.
// The buf parameter is reused for the result to avoid allocation when new
// stems need to be inserted.
func mergeStemKVs(existing, updates, buf []core.StemKeyValue) []core.StemKeyValue {
	if len(updates) == 0 {
		return existing
	}
	if len(existing) == 0 {
		return updates
	}

	// Fast path: check if all updates are in-place replacements (no new stems).
	// This is the common case for incremental block updates where accounts
	// already exist in the trie.
	allInPlace := true
	ei := 0
	for _, u := range updates {
		for ei < len(existing) && bytes.Compare(existing[ei].Stem[:], u.Stem[:]) < 0 {
			ei++
		}
		if ei >= len(existing) || existing[ei].Stem != u.Stem {
			allInPlace = false
			break
		}
	}

	if allInPlace {
		// Update hashes in place — zero allocation.
		ei = 0
		for _, u := range updates {
			for existing[ei].Stem != u.Stem {
				ei++
			}
			existing[ei].Hash = u.Hash
		}
		return existing
	}

	// Slow path: some new stems need inserting. Use merge with buffer.
	needed := len(existing) + len(updates)
	if cap(buf) < needed {
		buf = make([]core.StemKeyValue, 0, needed)
	}
	result := buf[:0]
	i, j := 0, 0
	for i < len(existing) && j < len(updates) {
		cmp := bytes.Compare(existing[i].Stem[:], updates[j].Stem[:])
		switch {
		case cmp < 0:
			result = append(result, existing[i])
			i++
		case cmp > 0:
			result = append(result, updates[j])
			j++
		default:
			result = append(result, updates[j])
			i++
			j++
		}
	}
	result = append(result, existing[i:]...)
	result = append(result, updates[j:]...)
	return result
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
	allStems := make([]core.StemKeyValue, len(t.allStems))
	copy(allStems, t.allStems)
	return &NomtTrie{
		nomtDB:   t.nomtDB,
		backend:  t.backend,
		root:     t.root,
		pending:  pending,
		dirty:    t.dirty,
		allStems: allStems,
	}
}

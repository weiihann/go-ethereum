package nomtdb

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/nomt/db"
	"github.com/ethereum/go-ethereum/rlp"
)

// Key prefixes for NOMT flat state in ethdb.
const (
	nomtAccountPrefix byte = 0x01
	nomtStoragePrefix byte = 0x02
)

// NomtAccountKey returns the ethdb key for an account's flat state.
// Format: 0x01 || accountHash (32 bytes)
func NomtAccountKey(accountHash common.Hash) []byte {
	key := make([]byte, 1+common.HashLength)
	key[0] = nomtAccountPrefix
	copy(key[1:], accountHash[:])
	return key
}

// NomtStorageKey returns the ethdb key for a storage slot's flat state.
// Format: 0x02 || accountHash (32 bytes) || slotHash (32 bytes)
func NomtStorageKey(accountHash, slotHash common.Hash) []byte {
	key := make([]byte, 1+2*common.HashLength)
	key[0] = nomtStoragePrefix
	copy(key[1:], accountHash[:])
	copy(key[1+common.HashLength:], slotHash[:])
	return key
}

// nodeReader implements database.NodeReader backed by the NOMT page store.
//
// In NOMT, trie data is stored as pages (126 nodes each), not individual nodes.
// This reader extracts individual nodes from pages for compatibility with
// geth's node-oriented interfaces.
type nodeReader struct {
	nomt *db.DB
}

// Node retrieves a trie node blob. For NOMT, this is a compatibility shim —
// the NomtTrie accesses pages directly rather than going through NodeReader.
func (r *nodeReader) Node(owner common.Hash, path []byte, hash common.Hash) ([]byte, error) {
	// NOMT trie operations go through the page-based engine directly.
	// This reader exists to satisfy the interface; the NomtTrie doesn't
	// use it for normal trie operations.
	return nil, nil
}

// stateReader implements database.StateReader backed by NOMT's flat state
// stored in geth's ethdb.
type stateReader struct {
	diskdb ethdb.Database
}

// Account retrieves an account from NOMT's flat state storage.
func (r *stateReader) Account(hash common.Hash) (*types.SlimAccount, error) {
	data, err := r.diskdb.Get(NomtAccountKey(hash))
	if err != nil {
		// Key not found is not an error — return nil account.
		return nil, nil
	}
	if len(data) == 0 {
		return nil, nil
	}
	account := new(types.SlimAccount)
	if err := rlp.DecodeBytes(data, account); err != nil {
		return nil, err
	}
	return account, nil
}

// Storage retrieves a storage slot from NOMT's flat state storage.
func (r *stateReader) Storage(accountHash, storageHash common.Hash) ([]byte, error) {
	data, err := r.diskdb.Get(NomtStorageKey(accountHash, storageHash))
	if err != nil {
		return nil, nil
	}
	return data, nil
}

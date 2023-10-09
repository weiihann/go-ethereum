// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"bytes"
	"fmt"
	"io"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

type Code []byte

func (c Code) String() string {
	return string(c) //strings.Join(Disassemble(c), " ")
}

type Storage map[common.Hash]common.Hash

func (s Storage) String() (str string) {
	for key, value := range s {
		str += fmt.Sprintf("%X : %X\n", key, value)
	}
	return
}

func (s Storage) Copy() Storage {
	cpy := make(Storage, len(s))
	for key, value := range s {
		cpy[key] = value
	}
	return cpy
}

// stateObject represents an Ethereum account which is being modified.
//
// The usage pattern is as follows:
// - First you need to obtain a state object.
// - Account values as well as storages can be accessed and modified through the object.
// - Finally, call commit to return the changes of storage trie and update account data.
type stateObject struct {
	db       *StateDB
	address  common.Address      // address of ethereum account
	addrHash common.Hash         // hash of ethereum address of the account
	origin   *types.StateAccount // Account original data without any change applied, nil means it was not existent
	data     types.StateAccount  // Account data with all mutations applied in the scope of block

	// Write caches.
	trie Trie // storage trie, which becomes non-nil on first access
	code Code // contract bytecode, which gets set when code is loaded

	originStorage  Storage // Storage cache of original entries to dedup rewrites
	pendingStorage Storage // Storage entries that need to be flushed to disk, at the end of an entire block
	dirtyStorage   Storage // Storage entries that have been modified in the current transaction execution, reset for every transaction

	// revive state
	pendingReviveTrie  Trie
	dirtyReviveTrie    Trie
	pendingReviveState map[string]common.Hash
	dirtyReviveState   map[string]common.Hash
	targetEpoch        types.StateEpoch

	pendingAccessedState map[common.Hash]int
	dirtyAccessedState   map[common.Hash]int

	// Cache flags.
	dirtyCode bool // true if the code was updated

	// Flag whether the account was marked as self-destructed. The self-destructed account
	// is still accessible in the scope of same transaction.
	selfDestructed bool

	// Flag whether the account was marked as deleted. A self-destructed account
	// or an account that is considered as empty will be marked as deleted at
	// the end of transaction and no longer accessible anymore.
	deleted bool

	// Flag whether the object was created in the current transaction
	created bool
}

// empty returns whether the account is considered empty.
func (s *stateObject) empty() bool {
	return s.data.Nonce == 0 && s.data.Balance.Sign() == 0 && bytes.Equal(s.data.CodeHash, types.EmptyCodeHash.Bytes())
}

// newObject creates a state object.
func newObject(db *StateDB, address common.Address, acct *types.StateAccount) *stateObject {
	origin := acct
	if acct == nil {
		acct = types.NewEmptyStateAccount()
	}

	return &stateObject{
		db:                 db,
		address:            address,
		addrHash:           crypto.Keccak256Hash(address[:]),
		origin:             origin,
		data:               *acct,
		originStorage:      make(Storage),
		pendingStorage:     make(Storage),
		dirtyStorage:       make(Storage),
		pendingReviveState: make(map[string]common.Hash),
		dirtyReviveState:   make(map[string]common.Hash),
	}
}

// EncodeRLP implements rlp.Encoder.
func (s *stateObject) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, &s.data)
}

func (s *stateObject) markSelfdestructed() {
	s.selfDestructed = true
}

func (s *stateObject) touch() {
	s.db.journal.append(touchChange{
		account: &s.address,
	})
	if s.address == ripemd {
		// Explicitly put it in the dirty-cache, which is otherwise generated from
		// flattened journals.
		s.db.journal.dirty(s.address)
	}
}

// getTrie returns the associated storage trie. The trie will be opened
// if it's not loaded previously. An error will be returned if trie can't
// be loaded.
func (s *stateObject) getTrie() (Trie, error) {
	if s.trie == nil {
		// Try fetching from prefetcher first
		if s.data.Root != types.EmptyRootHash && s.db.prefetcher != nil {
			// When the miner is creating the pending state, there is no prefetcher
			s.trie = s.db.prefetcher.trie(s.addrHash, s.data.Root)
		}
		if s.trie == nil {
			tr, err := s.db.db.OpenStorageTrie(s.db.originalRoot, s.address, s.data.Root, s.db.trie)
			if err != nil {
				return nil, err
			}
			s.trie = tr
		}
	}
	return s.trie, nil
}

func (s *stateObject) getPendingReviveTrie(db Database) Trie {
	if s.pendingReviveTrie == nil {
		tr, err := s.getTrie()
		if err != nil {
			s.db.setError(err)
			return nil
		}
		s.pendingReviveTrie = s.db.db.CopyTrie(tr)
	}
	return s.pendingReviveTrie
}

func (s *stateObject) getDirtyReviveTrie(db Database) Trie {
	if s.dirtyReviveTrie == nil {
		s.dirtyReviveTrie = s.db.db.CopyTrie(s.getPendingReviveTrie(db))
	}
	return s.dirtyReviveTrie
}

// GetState retrieves a value from the account storage trie.
func (s *stateObject) GetState(key common.Hash) (common.Hash, error) {
	// If we have a dirty value for this state entry, return it
	value, dirty := s.dirtyStorage[key]
	if dirty {
		s.accessState(key)
		return value, nil
	}
	if s.db.enableStateEpoch(true) {
		if reviveVal, revive := s.queryFromReviveState(s.db.db, s.dirtyReviveState, key); revive {
			s.accessState(key)
			return reviveVal, nil
		}
	}
	// Otherwise return the entry's original value
	val, err := s.GetCommittedState(key)
	if err != nil {
		return common.Hash{}, err
	}
	if val != (common.Hash{}) {
		s.accessState(key)
	}
	return val, nil
}

// GetCommittedState retrieves a value from the committed account storage trie.
func (s *stateObject) GetCommittedState(key common.Hash) (common.Hash, error) {
	// If we have a pending write or clean cached, return that
	if value, pending := s.pendingStorage[key]; pending {
		return value, nil
	}
	if value, cached := s.originStorage[key]; cached {
		return value, nil
	}
	if reviveVal, revive := s.queryFromReviveState(s.db.db, s.pendingReviveState, key); revive {
		return reviveVal, nil
	}
	// If the object was destructed in *this* block (and potentially resurrected),
	// the storage has been cleared out, and we should *not* consult the previous
	// database about any storage values. The only possible alternatives are:
	//   1) resurrect happened, and new slot values were set -- those should
	//      have been handles via pendingStorage above.
	//   2) we don't have new values, and can deliver empty response back
	if _, destructed := s.db.stateObjectsDestruct[s.address]; destructed {
		return common.Hash{}, nil
	}
	// If no live objects are available, attempt to use snapshots
	var (
		enc   []byte
		err   error
		value common.Hash
	)
	if s.db.snap != nil {
		start := time.Now()
		enc, err = s.db.snap.Storage(s.addrHash, crypto.Keccak256Hash(key.Bytes()))
		if metrics.EnabledExpensive {
			s.db.SnapshotStorageReads += time.Since(start)
		}
		if len(enc) > 0 {
			_, content, _, err := rlp.Split(enc)
			if err != nil {
				s.db.setError(err)
			}
			value.SetBytes(content)
		}
	}
	// If the snapshot is unavailable or reading from it fails, load from the database.
	if s.db.snap == nil || err != nil {
		start := time.Now()
		tr, err := s.getTrie()
		if err != nil {
			s.db.setError(err)
			return common.Hash{}, err
		}
		val, err := tr.GetStorage(s.address, key.Bytes())
		if metrics.EnabledExpensive {
			s.db.StorageReads += time.Since(start)
		}
		if err != nil {
			if enErr, ok := err.(*trie.ExpiredNodeError); ok {
				// query from dirty revive trie, got the newest expired info
				_, err = s.getDirtyReviveTrie(s.db.db).GetStorage(s.address, key.Bytes())
				if enErr, ok := err.(*trie.ExpiredNodeError); ok {
					return common.Hash{}, NewExpiredStateError(s.address, key, enErr)
				}
				return common.Hash{}, NewExpiredStateError(s.address, key, enErr)
			}
			s.db.setError(err)
			return common.Hash{}, nil
		}
		value.SetBytes(val)
	}

	s.originStorage[key] = value
	return value, nil
}

// SetState updates a value in account storage.
func (s *stateObject) SetState(key, value common.Hash) error {
	// If the new value is the same as old, don't set
	prev, err := s.GetState(key)
	if exErr, ok := err.(*ExpiredStateError); ok {
		exErr.Reason("query from insert")
		return exErr
	}
	if err != nil {
		return err
	}
	if prev == value {
		s.accessState(key)
		return nil
	}
	// when state insert, check if valid to insert new state
	if prev == (common.Hash{}) {
		_, err = s.getDirtyReviveTrie(s.db.db).GetStorage(s.address, key.Bytes())
		if err != nil {
			if enErr, ok := err.(*trie.ExpiredNodeError); ok {
				return NewInsertExpiredStateError(s.address, key, enErr)
			}
			s.db.setError(err)
			return nil
		}
	}
	s.accessState(key)
	// New value is different, update and journal the change
	s.db.journal.append(storageChange{
		account:  &s.address,
		key:      key,
		prevalue: prev,
	})
	s.setState(key, value)
	return nil
}

func (s *stateObject) setState(key, value common.Hash) {
	s.dirtyStorage[key] = value
}

// finalise moves all dirty storage slots into the pending area to be hashed or
// committed later. It is invoked at the end of every transaction.
func (s *stateObject) finalise(prefetch bool) {
	slotsToPrefetch := make([][]byte, 0, len(s.dirtyStorage))
	for key, value := range s.dirtyStorage {
		s.pendingStorage[key] = value
		if value != s.originStorage[key] {
			slotsToPrefetch = append(slotsToPrefetch, common.CopyBytes(key[:])) // Copy needed for closure
		}
	}
	for key, value := range s.dirtyReviveState {
		s.pendingReviveState[key] = value
	}
	for key, value := range s.dirtyAccessedState {
		count := s.pendingAccessedState[key]
		s.pendingAccessedState[key] = count + value
	}
	if s.db.prefetcher != nil && prefetch && len(slotsToPrefetch) > 0 && s.data.Root != types.EmptyRootHash {
		s.db.prefetcher.prefetch(s.addrHash, s.data.Root, s.address, slotsToPrefetch)
	}
	if len(s.dirtyStorage) > 0 {
		s.dirtyStorage = make(Storage)
	}
	if len(s.dirtyReviveState) > 0 {
		s.dirtyReviveState = make(map[string]common.Hash)
	}
	if len(s.dirtyAccessedState) > 0 {
		s.dirtyAccessedState = make(map[common.Hash]int)
	}
	if s.dirtyReviveTrie != nil {
		s.pendingReviveTrie = s.dirtyReviveTrie
		s.dirtyReviveTrie = nil
	}
}

// updateTrie writes cached storage modifications into the object's storage trie.
// It will return nil if the trie has not been loaded and no changes have been
// made. An error will be returned if the trie can't be loaded/updated correctly.
func (s *stateObject) updateTrie() (Trie, error) {
	// Make sure all dirty slots are finalized into the pending storage area
	s.finalise(false) // Don't prefetch anymore, pull directly if need be
	if len(s.pendingStorage) == 0 {
		return s.trie, nil
	}
	// Track the amount of time wasted on updating the storage trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.db.StorageUpdates += time.Since(start) }(time.Now())
	}
	// The snapshot storage map for the object
	var (
		storage map[common.Hash][]byte
		origin  map[common.Hash][]byte
		hasher  = s.db.hasher
	)
	tr := s.getPendingReviveTrie(s.db.db)

	// Insert all the pending updates into the trie
	usedStorage := make([][]byte, 0, len(s.pendingStorage))
	accessStorage := make(map[common.Hash]struct{}, len(s.pendingAccessedState))
	for key, value := range s.pendingStorage {
		// Skip noop changes, persist actual changes
		if value == s.originStorage[key] {
			continue
		}
		prev := s.originStorage[key]
		s.originStorage[key] = value

		// rlp-encoded value to be used by the snapshot
		var snapshotVal []byte
		if (value == common.Hash{}) {
			if err := tr.DeleteStorage(s.address, key[:]); err != nil {
				s.db.setError(err)
				return nil, err
			}
			s.db.StorageDeleted += 1
		} else {
			trimmedVal := common.TrimLeftZeroes(value[:])
			// Encoding []byte cannot fail, ok to ignore the error.
			snapshotVal, _ = rlp.EncodeToBytes(trimmedVal)
			if err := tr.UpdateStorage(s.address, key[:], trimmedVal); err != nil {
				s.db.setError(err)
				return nil, err
			}
			s.db.StorageUpdated += 1
		}
		// Cache the mutated storage slots until commit
		if storage == nil {
			if storage = s.db.storages[s.addrHash]; storage == nil {
				storage = make(map[common.Hash][]byte)
				s.db.storages[s.addrHash] = storage
			}
		}
		khash := crypto.HashData(hasher, key[:])
		storage[khash] = snapshotVal // snapshotVal will be nil if it's deleted

		// Cache the original value of mutated storage slots
		if origin == nil {
			if origin = s.db.storagesOrigin[s.address]; origin == nil {
				origin = make(map[common.Hash][]byte)
				s.db.storagesOrigin[s.address] = origin
			}
		}
		// Track the original value of slot only if it's mutated first time
		if _, ok := origin[khash]; !ok {
			if prev == (common.Hash{}) {
				origin[khash] = nil // nil if it was not present previously
			} else {
				// Encoding []byte cannot fail, ok to ignore the error.
				b, _ := rlp.EncodeToBytes(common.TrimLeftZeroes(prev[:]))
				origin[khash] = b
			}
		}
		// Cache the items for preloading
		usedStorage = append(usedStorage, common.CopyBytes(key[:])) // Copy needed for closure
	}
	if s.db.enableStateEpoch(true) {
		for key, _ := range accessStorage {
			// it must hit in cache
			value, err := s.GetState(key)
			if err != nil {
				return nil, err
			}
			// TODO(w): handle snapshot
			trimmedVal := common.TrimLeftZeroes(value[:])
			snapshotVal, _ := snapshot.EncodeValueToRLPBytes(snapshot.NewValueWithEpoch(s.db.targetEpoch, trimmedVal))
			if err := tr.UpdateStorage(s.address, key[:], trimmedVal); err != nil {
				s.db.setError(err)
				return nil, err
			}
			s.db.StorageUpdated += 1
			// Cache the mutated storage slots until commit
			if storage == nil {
				if storage = s.db.storages[s.addrHash]; storage == nil {
					storage = make(map[common.Hash][]byte)
					s.db.storages[s.addrHash] = storage
				}
			}
			khash := crypto.HashData(hasher, key[:])
			storage[khash] = snapshotVal                                // snapshotVal will be nil if it's deleted
			usedStorage = append(usedStorage, common.CopyBytes(key[:])) // Copy needed for closure
		}
	}
	if s.db.prefetcher != nil {
		s.db.prefetcher.used(s.addrHash, s.data.Root, usedStorage)
	}
	if len(s.pendingStorage) > 0 {
		s.pendingStorage = make(Storage)
	}
	if len(s.pendingReviveState) > 0 {
		s.pendingReviveState = make(map[string]common.Hash)
	}
	if len(s.pendingAccessedState) > 0 {
		s.pendingAccessedState = make(map[common.Hash]int)
	}
	if s.pendingReviveTrie != nil {
		s.pendingReviveTrie = nil
	}
	// reset trie as pending trie, will commit later
	if tr != nil {
		s.trie = s.db.db.CopyTrie(tr)
	}

	return tr, nil
}

// UpdateRoot sets the trie root to the current root hash of. An error
// will be returned if trie root hash is not computed correctly.
func (s *stateObject) updateRoot() {
	tr, err := s.updateTrie()
	if err != nil {
		return
	}
	// If nothing changed, don't bother with hashing anything
	if tr == nil {
		return
	}
	// Track the amount of time wasted on hashing the storage trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.db.StorageHashes += time.Since(start) }(time.Now())
	}
	s.data.Root = tr.Hash()
}

// commit returns the changes made in storage trie and updates the account data.
func (s *stateObject) commit() (*trienode.NodeSet, error) {
	tr, err := s.updateTrie()
	if err != nil {
		return nil, err
	}
	// If nothing changed, don't bother with committing anything
	if tr == nil {
		s.origin = s.data.Copy()
		return nil, nil
	}
	// Track the amount of time wasted on committing the storage trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.db.StorageCommits += time.Since(start) }(time.Now())
	}
	root, nodes, err := tr.Commit(false)
	if err != nil {
		return nil, err
	}
	s.data.Root = root

	// Update original account data after commit
	s.origin = s.data.Copy()
	return nodes, nil
}

// AddBalance adds amount to s's balance.
// It is used to add funds to the destination account of a transfer.
func (s *stateObject) AddBalance(amount *big.Int) {
	// EIP161: We must check emptiness for the objects such that the account
	// clearing (0,0,0 objects) can take effect.
	if amount.Sign() == 0 {
		if s.empty() {
			s.touch()
		}
		return
	}
	s.SetBalance(new(big.Int).Add(s.Balance(), amount))
}

// SubBalance removes amount from s's balance.
// It is used to remove funds from the origin account of a transfer.
func (s *stateObject) SubBalance(amount *big.Int) {
	if amount.Sign() == 0 {
		return
	}
	s.SetBalance(new(big.Int).Sub(s.Balance(), amount))
}

func (s *stateObject) SetBalance(amount *big.Int) {
	s.db.journal.append(balanceChange{
		account: &s.address,
		prev:    new(big.Int).Set(s.data.Balance),
	})
	s.setBalance(amount)
}

func (s *stateObject) setBalance(amount *big.Int) {
	s.data.Balance = amount
}

func (s *stateObject) deepCopy(db *StateDB) *stateObject {
	obj := &stateObject{
		db:       db,
		address:  s.address,
		addrHash: s.addrHash,
		origin:   s.origin,
		data:     s.data,
	}
	if s.trie != nil {
		obj.trie = db.db.CopyTrie(s.trie)
	}
	obj.code = s.code
	obj.dirtyStorage = s.dirtyStorage.Copy()
	obj.originStorage = s.originStorage.Copy()
	obj.pendingStorage = s.pendingStorage.Copy()
	obj.selfDestructed = s.selfDestructed
	obj.dirtyCode = s.dirtyCode
	obj.deleted = s.deleted
	obj.targetEpoch = s.targetEpoch

	if s.dirtyReviveTrie != nil {
		obj.dirtyReviveTrie = db.db.CopyTrie(s.dirtyReviveTrie)
	}
	if s.pendingReviveTrie != nil {
		obj.pendingReviveTrie = db.db.CopyTrie(s.pendingReviveTrie)
	}
	obj.dirtyReviveState = make(map[string]common.Hash, len(s.dirtyReviveState))
	for k, v := range s.dirtyReviveState {
		obj.dirtyReviveState[k] = v
	}
	obj.pendingReviveState = make(map[string]common.Hash, len(s.pendingReviveState))
	for k, v := range s.pendingReviveState {
		obj.pendingReviveState[k] = v
	}
	obj.dirtyAccessedState = make(map[common.Hash]int, len(s.dirtyAccessedState))
	for k, v := range s.dirtyAccessedState {
		obj.dirtyAccessedState[k] = v
	}
	obj.pendingAccessedState = make(map[common.Hash]int, len(s.pendingAccessedState))
	for k, v := range s.pendingAccessedState {
		obj.pendingAccessedState[k] = v
	}
	return obj
}

//
// Attribute accessors
//

// Address returns the address of the contract/account
func (s *stateObject) Address() common.Address {
	return s.address
}

// Code returns the contract code associated with this object, if any.
func (s *stateObject) Code() []byte {
	if s.code != nil {
		return s.code
	}
	if bytes.Equal(s.CodeHash(), types.EmptyCodeHash.Bytes()) {
		return nil
	}
	code, err := s.db.db.ContractCode(s.address, common.BytesToHash(s.CodeHash()))
	if err != nil {
		s.db.setError(fmt.Errorf("can't load code hash %x: %v", s.CodeHash(), err))
	}
	s.code = code
	return code
}

// CodeSize returns the size of the contract code associated with this object,
// or zero if none. This method is an almost mirror of Code, but uses a cache
// inside the database to avoid loading codes seen recently.
func (s *stateObject) CodeSize() int {
	if s.code != nil {
		return len(s.code)
	}
	if bytes.Equal(s.CodeHash(), types.EmptyCodeHash.Bytes()) {
		return 0
	}
	size, err := s.db.db.ContractCodeSize(s.address, common.BytesToHash(s.CodeHash()))
	if err != nil {
		s.db.setError(fmt.Errorf("can't load code size %x: %v", s.CodeHash(), err))
	}
	return size
}

func (s *stateObject) SetCode(codeHash common.Hash, code []byte) {
	prevcode := s.Code()
	s.db.journal.append(codeChange{
		account:  &s.address,
		prevhash: s.CodeHash(),
		prevcode: prevcode,
	})
	s.setCode(codeHash, code)
}

func (s *stateObject) setCode(codeHash common.Hash, code []byte) {
	s.code = code
	s.data.CodeHash = codeHash[:]
	s.dirtyCode = true
}

func (s *stateObject) SetNonce(nonce uint64) {
	s.db.journal.append(nonceChange{
		account: &s.address,
		prev:    s.data.Nonce,
	})
	s.setNonce(nonce)
}

func (s *stateObject) setNonce(nonce uint64) {
	s.data.Nonce = nonce
}

func (s *stateObject) CodeHash() []byte {
	return s.data.CodeHash
}

func (s *stateObject) Balance() *big.Int {
	return s.data.Balance
}

func (s *stateObject) Nonce() uint64 {
	return s.data.Nonce
}

func (s *stateObject) queryFromReviveState(db Database, reviveState map[string]common.Hash, key common.Hash) (common.Hash, bool) {
	tr, err := s.getTrie()
	if err != nil {
		s.db.setError(err)
		return common.Hash{}, false
	}
	hashKey := string(tr.HashKey(key.Bytes()))
	val, ok := reviveState[hashKey]
	return val, ok
}

func (s *stateObject) accessState(key common.Hash) {
	s.db.journal.append(accessedStorageStateChange{
		address: &s.address,
		slot:    &key,
	})
	count := s.dirtyAccessedState[key]
	s.dirtyAccessedState[key] = count + 1
}


func (ch reviveStorageTrieNodeChange) revert(s *StateDB) {
	s.getStateObject(*ch.address).dirtyReviveState = make(map[string]common.Hash)
	s.getStateObject(*ch.address).dirtyReviveTrie = nil
}

func (ch reviveStorageTrieNodeChange) dirtied() *common.Address {
	return ch.address
}

func (ch accessedStorageStateChange) revert(s *StateDB) {
	if count, ok := s.getStateObject(*ch.address).dirtyAccessedState[*ch.slot]; ok {
		if count > 1 {
			s.getStateObject(*ch.address).dirtyAccessedState[*ch.slot] = count - 1
		} else {
			delete(s.getStateObject(*ch.address).dirtyAccessedState, *ch.slot)
		}
	}

}

func (ch accessedStorageStateChange) dirtied() *common.Address {
	return ch.address
}

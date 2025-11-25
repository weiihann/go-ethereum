package krogan

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
)

type reader struct {
	chain *ChainWindow
	db    ethdb.KeyValueReader

	codeCache     *lru.SizeConstrainedCache[common.Hash, []byte]
	codeSizeCache *lru.Cache[common.Hash, int]
}

func newReader(
	chain *ChainWindow,
	db ethdb.KeyValueReader,
	codeCache *lru.SizeConstrainedCache[common.Hash, []byte],
	codeSizeCache *lru.Cache[common.Hash, int],
) *reader {
	return &reader{
		chain:         chain,
		db:            db,
		codeCache:     codeCache,
		codeSizeCache: codeSizeCache,
	}
}

func (r *reader) Code(addr common.Address, codeHash common.Hash) ([]byte, error) {
	// Check cache first
	if code, ok := r.codeCache.Get(codeHash); ok {
		return code, nil
	}

	// Read from disk
	code, err := r.db.Get(codeKey(codeHash))
	if err != nil {
		return nil, nil // Key not found, return empty
	}

	// Update cache
	r.codeCache.Add(codeHash, code)
	r.codeSizeCache.Add(codeHash, len(code))
	return code, nil
}

func (r *reader) CodeSize(addr common.Address, codeHash common.Hash) (int, error) {
	// Check cache first
	if size, ok := r.codeSizeCache.Get(codeHash); ok {
		return size, nil
	}

	// Get code to populate cache
	code, err := r.Code(addr, codeHash)
	if err != nil {
		return 0, err
	}

	return len(code), nil
}

func (r *reader) Account(addr common.Address) (*types.StateAccount, error) {
	// Hash the address to get the account key
	addrHash := crypto.Keccak256Hash(addr.Bytes())

	// Read from disk
	accountRLP, err := r.db.Get(accountKey(addrHash))
	if err != nil {
		return nil, nil // Key not found, return empty
	}

	// Decode SlimAccount
	var slimAccount types.SlimAccount
	if err := rlp.DecodeBytes(accountRLP, &slimAccount); err != nil {
		return nil, fmt.Errorf("failed to decode account %s: %w", addr.Hex(), err)
	}

	// Convert SlimAccount to StateAccount
	account := &types.StateAccount{
		Nonce:    slimAccount.Nonce,
		Balance:  slimAccount.Balance,
		Root:     types.EmptyRootHash, // Krogan doesn't store trie roots
		CodeHash: slimAccount.CodeHash,
	}

	return account, nil
}

func (r *reader) Storage(addr common.Address, slot common.Hash) (common.Hash, error) {
	// Hash the address
	addrHash := crypto.Keccak256Hash(addr.Bytes())

	// Read from disk
	valueRLP, err := r.db.Get(storageKey(addrHash, slot))
	if err != nil {
		return common.Hash{}, nil // Key not found, return empty
	}

	// Decode value
	var value common.Hash
	if err := rlp.DecodeBytes(valueRLP, &value); err != nil {
		return common.Hash{}, fmt.Errorf("failed to decode storage %s[%s]: %w", addr.Hex(), slot.Hex(), err)
	}

	return value, nil
}

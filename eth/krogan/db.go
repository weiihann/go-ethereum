package krogan

import (
	"errors"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie/utils"
	"github.com/ethereum/go-ethereum/triedb"
)

const (
	// Number of codehash->size associations to keep.
	codeSizeCacheSize = 1_000_000 // 4 megabytes in total

	// Cache size granted for caching clean code.
	codeCacheSize = 256 * 1024 * 1024
)

var ErrDBFunctionNotSupported = errors.New("db function not supported by krogan")

type KroganDB struct {
	disk  ethdb.KeyValueStore
	chain *ChainWindow

	codeCache     *lru.SizeConstrainedCache[common.Hash, []byte]
	codeSizeCache *lru.Cache[common.Hash, int]
}

func NewKroganDB(chain *ChainWindow, disk ethdb.KeyValueStore) *KroganDB {
	return &KroganDB{
		disk:          disk,
		chain:         chain,
		codeCache:     lru.NewSizeConstrainedCache[common.Hash, []byte](codeCacheSize),
		codeSizeCache: lru.NewCache[common.Hash, int](codeSizeCacheSize),
	}
}

func (k *KroganDB) Reader(root common.Hash) (state.Reader, error) {
	return newReader(k.chain, k.disk, k.codeCache, k.codeSizeCache), nil
}

func (k *KroganDB) InsertBlock(data *BlockData) error {
	return k.chain.InsertBlock(data)
}

func (k *KroganDB) OpenTrie(root common.Hash) (state.Trie, error) {
	return nil, ErrDBFunctionNotSupported
}

func (k *KroganDB) OpenStorageTrie(stateRoot common.Hash, address common.Address, root common.Hash, trie state.Trie) (state.Trie, error) {
	return nil, ErrDBFunctionNotSupported
}

func (k *KroganDB) PointCache() *utils.PointCache {
	panic(ErrDBFunctionNotSupported)
}

func (k *KroganDB) TrieDB() *triedb.Database {
	panic(ErrDBFunctionNotSupported)
}

func (k *KroganDB) Snapshot() *snapshot.Tree {
	panic(ErrDBFunctionNotSupported)
}

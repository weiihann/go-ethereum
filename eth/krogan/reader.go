package krogan

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
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
	panic("TODO(weiihann): implement")
}

func (r *reader) CodeSize(addr common.Address, codeHash common.Hash) (int, error) {
	panic("TODO(weiihann): implement")
}

func (r *reader) Account(addr common.Address) (*types.StateAccount, error) {
	panic("TODO(weiihann): implement")
}

func (r *reader) Storage(addr common.Address, slot common.Hash) (common.Hash, error) {
	panic("TODO(weiihann): implement")
}

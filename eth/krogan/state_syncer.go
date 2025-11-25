package krogan

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient/kroganclient"
	"github.com/ethereum/go-ethereum/rpc"
)

// StateSyncer is responsible for syncing the state (accounts, storage and codes).
// TODO(weiihann):
// 1. spread workload across multiple master nodes
// 2. deal with the initial state diffs sync
// 3.
type StateSyncer struct {
	httpURLs    []string
	httpMasters []*kroganclient.Client

	// Specifies the range of accounts to sync
	// If not set, it will sync all accounts
	startAccHash common.Hash
	endAccHash   common.Hash

	db *KroganDB
}

func NewStateSyncer(db *KroganDB) *StateSyncer {
	return &StateSyncer{db: db}
}

func (s *StateSyncer) Start() error {
	panic("not implemented")
}

func (s *StateSyncer) Stop() error {
	panic("not implemented")
}

func (s *StateSyncer) RegisterHTTPClient(httpURL string) error {
	client, err := rpc.DialHTTP(httpURL)
	if err != nil {
		return err
	}
	s.httpMasters = append(s.httpMasters, kroganclient.New(client))
	s.httpURLs = append(s.httpURLs, httpURL)
	return nil
}

func (s *StateSyncer) fetchAccounts(startHash common.Hash) error {
	panic("not implemented")
}

func (s *StateSyncer) fetchStorage(addrHash common.Hash, startHash common.Hash) error {
	panic("not implemented")
}

func (s *StateSyncer) fetchBytecodes(codeHashes []common.Hash) error {
	panic("not implemented")
}

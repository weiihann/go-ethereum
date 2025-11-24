package krogan

import (
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/rpc"
)

// TODO(weiihann)
type Krogan struct {
	syncer     *Syncer
	apiBackend *APIBackend
}

func New(stack *node.KroganNode, config *node.KroganConfig) (*Krogan, error) {
	if len(config.WSSMasterNodes) == 0 {
		log.Crit("Must have at least one WebSocket master node")
	}
	if len(config.HTTPMasterNodes) == 0 {
		log.Crit("Must have at least one HTTP master node")
	}

	chain := NewChainWindow(config.ChainSize)
	db := NewKroganDB(chain, nil) // TODO(weiihann): add disk db here
	apiBackend := NewAPIBackend(chain, db)

	syncer := NewSyncer(db)

	err := syncer.RegisterWSClient(config.WSSMasterNodes[0]) // TODO(weiihann): deal with multiple ws nodes
	if err != nil {
		return nil, err
	}

	httpCount := 0
	for _, httpURL := range config.HTTPMasterNodes {
		if err := syncer.RegisterHTTPClient(httpURL); err != nil {
			log.Error("Failed to register HTTP client", "error", err)
		}
		httpCount++
	}

	if httpCount == 0 {
		log.Crit("Must have at least one HTTP master node")
	}

	krogan := &Krogan{
		syncer:     syncer,
		apiBackend: apiBackend,
	}

	stack.RegisterLifecycle(krogan)
	stack.RegisterAPIs(krogan.APIs())

	return krogan, nil
}

func (k *Krogan) APIs() []rpc.API {
	nonceLock := new(ethapi.AddrLocker)
	return []rpc.API{
		{
			Namespace: "eth",
			Service:   ethapi.NewEthereumAPI(k.apiBackend),
		}, {
			Namespace: "eth",
			Service:   ethapi.NewBlockChainAPI(k.apiBackend),
		}, {
			Namespace: "eth",
			Service:   ethapi.NewTransactionAPI(k.apiBackend, nonceLock),
		},
	}
}

func (k *Krogan) Start() error {
	return k.syncer.Start()
}

func (k *Krogan) Stop() error {
	return k.syncer.Stop()
}

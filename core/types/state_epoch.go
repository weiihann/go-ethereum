package types

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/params"
	"github.com/gballet/go-verkle"
	"math/big"
)

var (
	DefaultStateEpochPeriod = uint64(7_008_000)
	StateEpoch0             = verkle.StateEpoch(0)
	StateEpoch1             = verkle.StateEpoch(1)
	StateEpochKeepLiveNum   = verkle.StateEpoch(2)
)

func GetStateEpoch(config *params.ChainConfig, blockNumber *big.Int) verkle.StateEpoch {
	if blockNumber == nil || config == nil {
		return StateEpoch0
	}
	epochPeriod := new(big.Int).SetUint64(DefaultStateEpochPeriod)
	if config.Clique != nil && config.Clique.StateEpochPeriod != 0 {
		epochPeriod = new(big.Int).SetUint64(config.Clique.StateEpochPeriod)
	}

	if config.IsStateExpiryFork2(blockNumber) {
		ret := new(big.Int).Sub(blockNumber, config.StateExpiryBlock2)
		ret.Div(ret, epochPeriod)
		ret.Add(ret, common.Big2)
		return verkle.StateEpoch(ret.Uint64())
	}
	if config.IsStateExpiryFork1(blockNumber) {
		return 1
	}

	return 0
}

// EpochExpired check pre epoch if expired compared to current epoch
func EpochExpired(pre verkle.StateEpoch, cur verkle.StateEpoch) bool {
	return cur > pre && cur-pre >= StateEpochKeepLiveNum
}

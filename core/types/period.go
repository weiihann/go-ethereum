package types

import (
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-verkle"
)

var (
	PeriodLength = uint64(15_778_800) // measured in seconds, about 6 months
	Period0      = verkle.StatePeriod(0)
)

func GetStatePeriod(config *params.ChainConfig, curTime uint64) verkle.StatePeriod {
	if config == nil || config.StateExpiryTime == nil {
		return Period0
	}

	forkTime := *config.StateExpiryTime
	if curTime < forkTime {
		return Period0
	}

	return verkle.StatePeriod((curTime - forkTime) / PeriodLength)
}
package state

import "github.com/ethereum/go-ethereum/common"

type AccessState struct {
	Address map[common.Address]struct{}
	Slots   map[common.Address]map[common.Hash]struct{}
}

func NewAccessState() *AccessState {
	return &AccessState{
		Address: make(map[common.Address]struct{}),
		Slots:   make(map[common.Address]map[common.Hash]struct{}),
	}
}

func (a *AccessState) AddAddress(address common.Address) {
	a.Address[address] = struct{}{}
}

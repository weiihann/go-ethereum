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
	if _, ok := a.Address[address]; ok {
		return
	}
	a.Address[address] = struct{}{}
}

func (a *AccessState) AddSlot(address common.Address, slot common.Hash) {
	if _, ok := a.Slots[address]; !ok {
		a.Slots[address] = make(map[common.Hash]struct{})
	}
	if _, ok := a.Slots[address][slot]; ok {
		return
	}
	a.Slots[address][slot] = struct{}{}
}

func (a *AccessState) Reset() {
	a.Address = make(map[common.Address]struct{})
	a.Slots = make(map[common.Address]map[common.Hash]struct{})
}

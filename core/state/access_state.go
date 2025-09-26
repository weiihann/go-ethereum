package state

import (
	"maps"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
)

type AccessState struct {
	Address map[common.Address]struct{}
	Slots   map[common.Address]map[common.Hash]struct{}
}

func NewAccessState() *AccessState {
	log.Info("Creating AccessState")
	return &AccessState{
		Address: make(map[common.Address]struct{}),
		Slots:   make(map[common.Address]map[common.Hash]struct{}),
	}
}

func (a *AccessState) AddAddress(address common.Address) {
	log.Info("AccessState AddAddress", "address", address)
	if _, ok := a.Address[address]; ok {
		return
	}
	a.Address[address] = struct{}{}
}

func (a *AccessState) AddSlot(address common.Address, slot common.Hash) {
	log.Info("AccessState AddSlot", "address", address, "slot", slot)
	if _, ok := a.Slots[address]; !ok {
		a.Slots[address] = make(map[common.Hash]struct{})
	}
	if _, ok := a.Slots[address][slot]; ok {
		return
	}
	a.Slots[address][slot] = struct{}{}
}

func (a *AccessState) Reset() {
	log.Info("AccessState Reset")
	a.Address = make(map[common.Address]struct{})
	a.Slots = make(map[common.Address]map[common.Hash]struct{})
}

func (a *AccessState) Copy() *AccessState {
	log.Info("AccessState Copy")
	return &AccessState{
		Address: maps.Clone(a.Address),
		Slots:   maps.Clone(a.Slots),
	}
}

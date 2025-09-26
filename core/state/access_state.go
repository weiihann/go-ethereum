package state

import (
	"maps"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

type AccessState struct {
	Address map[common.Hash]struct{}
	Slots   map[common.Hash]map[common.Hash]struct{}
}

func NewAccessState() *AccessState {
	return &AccessState{
		Address: make(map[common.Hash]struct{}),
		Slots:   make(map[common.Hash]map[common.Hash]struct{}),
	}
}

func (a *AccessState) AddAddress(address common.Address) {
	addrHash := crypto.Keccak256Hash(address.Bytes())
	a.Address[addrHash] = struct{}{}
}

func (a *AccessState) AddSlot(address common.Address, slot common.Hash) {
	addrHash := crypto.Keccak256Hash(address.Bytes())
	slotHash := crypto.Keccak256Hash(slot.Bytes())
	if _, ok := a.Slots[addrHash]; !ok {
		a.Slots[addrHash] = make(map[common.Hash]struct{})
	}
	a.Slots[addrHash][slotHash] = struct{}{}
}

func (a *AccessState) Reset() {
	a.Address = make(map[common.Hash]struct{})
	a.Slots = make(map[common.Hash]map[common.Hash]struct{})
}

func (a *AccessState) Copy() *AccessState {
	return &AccessState{
		Address: maps.Clone(a.Address),
		Slots:   maps.Clone(a.Slots),
	}
}

package state

import (
	"maps"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
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
	log.Info("Add slot", "address", address.String(), "slot", slot.String(), "addrHash", addrHash.String(), "slotHash", slotHash.String())
	a.Address[addrHash] = struct{}{} // Add the address to the access state
	if _, ok := a.Slots[addrHash]; !ok {
		a.Slots[addrHash] = make(map[common.Hash]struct{})
	}
	a.Slots[addrHash][slotHash] = struct{}{}
}

func (a *AccessState) Commit(db ethdb.KeyValueStore) {
	batch := db.NewBatch()
	for addr := range a.Address {
		rawdb.WriteAccessAccount(batch, addr)
	}
	for addr, slot := range a.Slots {
		if _, ok := a.Address[addr]; !ok {
			rawdb.WriteAccessAccount(batch, addr)
		}
		for slot := range slot {
			rawdb.WriteAccessSlot(batch, addr, slot)
		}
	}
	if err := batch.Write(); err != nil {
		log.Crit("Failed to write access state", "err", err)
	}
}

func (a *AccessState) Reset() {
	a.Address = make(map[common.Hash]struct{})
	a.Slots = make(map[common.Hash]map[common.Hash]struct{})
}

func (a *AccessState) Copy() *AccessState {
	copied := &AccessState{
		Address: make(map[common.Hash]struct{}),
		Slots:   make(map[common.Hash]map[common.Hash]struct{}),
	}
	copied.Address = maps.Clone(a.Address)
	for addr, slot := range a.Slots {
		copied.Slots[addr] = maps.Clone(slot)
	}
	return copied
}

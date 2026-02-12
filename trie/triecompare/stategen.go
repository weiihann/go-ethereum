// Package triecompare provides realistic Ethereum state generation and
// comparison tests between bintrie and NOMT trie implementations.
//
// The state generation logic is ported from the state-actor repository's
// generator patterns, using PowerLaw/Uniform/Exponential distributions
// to mimic mainnet-like storage slot distributions.
package triecompare

import (
	"bytes"
	"math"
	mrand "math/rand"
	"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/holiman/uint256"
)

// Distribution represents the storage slot distribution strategy.
type Distribution int

const (
	// PowerLaw distribution — most contracts have few slots, few have many.
	// Mimics real Ethereum where contracts like Uniswap have millions of
	// slots while most have very few. Uses Pareto inverse CDF (alpha=1.5).
	PowerLaw Distribution = iota

	// Uniform distribution — all contracts have similar slot counts.
	Uniform

	// Exponential distribution — exponential decay in slot counts.
	Exponential
)

// StateGenConfig configures synthetic state generation.
type StateGenConfig struct {
	NumAccounts  int          // Number of EOA accounts
	NumContracts int          // Number of contract accounts
	MinSlots     int          // Minimum storage slots per contract
	MaxSlots     int          // Maximum storage slots per contract
	CodeSize     int          // Average contract code size in bytes
	Distribution Distribution // Slot distribution strategy
	Seed         int64        // Deterministic random seed
}

// OpKind discriminates between state operation types.
type OpKind int

const (
	OpUpdateAccount OpKind = iota
	OpUpdateStorage
	OpUpdateCode
)

// StateOp represents a single state operation to apply to a trie.
type StateOp struct {
	Kind    OpKind
	Address common.Address
	Account *types.StateAccount // populated for OpUpdateAccount
	CodeLen int                 // code length for OpUpdateAccount
	Code    []byte              // populated for OpUpdateCode
	Slot    []byte              // 32-byte key for OpUpdateStorage
	Value   []byte              // raw value for OpUpdateStorage
}

// GenerateBlocks produces deterministic blocks of state operations.
// Block 0 = initial state creation (all accounts, storage, code).
// Block 1 = incremental mutations (nonce bumps, balance changes, storage mods).
func GenerateBlocks(cfg StateGenConfig) [][]StateOp {
	rng := mrand.New(mrand.NewSource(cfg.Seed))

	// Block 0: initial state.
	block0 := generateInitialState(rng, cfg)

	// Block 1: incremental mutations on a subset of addresses.
	block1 := generateMutations(rng, cfg, block0)

	return [][]StateOp{block0, block1}
}

// generateInitialState creates the full initial state: EOAs, contracts
// with storage and code.
func generateInitialState(rng *mrand.Rand, cfg StateGenConfig) []StateOp {
	estimatedOps := cfg.NumAccounts*1 + cfg.NumContracts*3
	ops := make([]StateOp, 0, estimatedOps)

	emptyCodeHash := common.HexToHash(
		"c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470",
	)

	// EOA accounts.
	for range cfg.NumAccounts {
		var addr common.Address
		rng.Read(addr[:])

		acc := &types.StateAccount{
			Nonce:    uint64(rng.Intn(1000)),
			Balance:  new(uint256.Int).Mul(uint256.NewInt(uint64(rng.Intn(1000))), uint256.NewInt(1e18)),
			CodeHash: emptyCodeHash[:],
		}
		ops = append(ops, StateOp{
			Kind:    OpUpdateAccount,
			Address: addr,
			Account: acc,
			CodeLen: 0,
		})
	}

	// Contract accounts with storage and code.
	slotDist := generateSlotDistribution(rng, cfg)

	for i := range cfg.NumContracts {
		var addr common.Address
		rng.Read(addr[:])

		// Generate code.
		codeSize := cfg.CodeSize + rng.Intn(max(cfg.CodeSize, 1))
		code := make([]byte, codeSize)
		rng.Read(code)

		acc := &types.StateAccount{
			Nonce:    uint64(rng.Intn(1000)),
			Balance:  new(uint256.Int).Mul(uint256.NewInt(uint64(rng.Intn(100))), uint256.NewInt(1e18)),
			CodeHash: emptyCodeHash[:],
		}

		// Account update (with code length for basicData encoding).
		ops = append(ops, StateOp{
			Kind:    OpUpdateAccount,
			Address: addr,
			Account: acc,
			CodeLen: codeSize,
		})

		// Code update.
		ops = append(ops, StateOp{
			Kind:    OpUpdateCode,
			Address: addr,
			Code:    code,
		})

		// Storage slots.
		numSlots := slotDist[i]
		for range numSlots {
			slot := make([]byte, 32)
			rng.Read(slot)
			val := make([]byte, 32)
			rng.Read(val)
			// Ensure non-zero value (matches state-actor behavior).
			if val[0] == 0 && val[31] == 0 {
				val[0] = 0x01
			}
			ops = append(ops, StateOp{
				Kind:    OpUpdateStorage,
				Address: addr,
				Slot:    slot,
				Value:   val,
			})
		}
	}

	return ops
}

// generateMutations creates incremental state changes on a subset of
// addresses from block 0. Modifies ~10% of accounts with nonce bumps,
// balance changes, and new storage slots.
func generateMutations(rng *mrand.Rand, cfg StateGenConfig, block0 []StateOp) []StateOp {
	// Collect unique addresses from block 0.
	addrSet := make(map[common.Address]bool, cfg.NumAccounts+cfg.NumContracts)
	for i := range block0 {
		if block0[i].Kind == OpUpdateAccount {
			addrSet[block0[i].Address] = true
		}
	}
	addrs := make([]common.Address, 0, len(addrSet))
	for addr := range addrSet {
		addrs = append(addrs, addr)
	}
	// Sort for deterministic iteration (map order is random in Go).
	sort.Slice(addrs, func(i, j int) bool {
		return bytes.Compare(addrs[i][:], addrs[j][:]) < 0
	})

	// Mutate ~10% of addresses.
	numMutations := max(len(addrs)/10, 1)
	ops := make([]StateOp, 0, numMutations*2)

	emptyCodeHash := common.HexToHash(
		"c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470",
	)

	for range numMutations {
		addr := addrs[rng.Intn(len(addrs))]

		// Nonce bump + balance change.
		acc := &types.StateAccount{
			Nonce:    uint64(1000 + rng.Intn(1000)),
			Balance:  new(uint256.Int).Mul(uint256.NewInt(uint64(rng.Intn(500))), uint256.NewInt(1e18)),
			CodeHash: emptyCodeHash[:],
		}
		ops = append(ops, StateOp{
			Kind:    OpUpdateAccount,
			Address: addr,
			Account: acc,
			CodeLen: 0,
		})

		// Add a new storage slot.
		slot := make([]byte, 32)
		rng.Read(slot)
		val := make([]byte, 32)
		rng.Read(val)
		if val[0] == 0 && val[31] == 0 {
			val[0] = 0x01
		}
		ops = append(ops, StateOp{
			Kind:    OpUpdateStorage,
			Address: addr,
			Slot:    slot,
			Value:   val,
		})
	}

	return ops
}

// generateSlotDistribution returns the number of storage slots for each
// contract based on the configured distribution strategy.
// Ported from state-actor/generator/generator.go:1056-1092.
func generateSlotDistribution(rng *mrand.Rand, cfg StateGenConfig) []int {
	dist := make([]int, cfg.NumContracts)

	switch cfg.Distribution {
	case PowerLaw:
		alpha := 1.5
		for i := range dist {
			u := rng.Float64()
			slots := float64(cfg.MinSlots) / math.Pow(1-u, 1/alpha)
			if slots > float64(cfg.MaxSlots) {
				slots = float64(cfg.MaxSlots)
			}
			dist[i] = int(slots)
		}

	case Exponential:
		lambda := math.Log(2) / float64(cfg.MaxSlots/4)
		for i := range dist {
			u := rng.Float64()
			slots := -math.Log(1-u) / lambda
			slots = math.Max(float64(cfg.MinSlots), math.Min(slots, float64(cfg.MaxSlots)))
			dist[i] = int(slots)
		}

	case Uniform:
		for i := range dist {
			dist[i] = cfg.MinSlots + rng.Intn(cfg.MaxSlots-cfg.MinSlots+1)
		}
	}

	return dist
}

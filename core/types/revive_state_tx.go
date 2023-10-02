package types

import (
	"bytes"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
)

type ReviveList []ReviveKeyValues

// ReviveStateTx is the transaction for revive state.
type ReviveStateTx struct {
	ChainID    *big.Int
	Nonce      uint64   // nonce of sender account
	GasTipCap  *big.Int // a.k.a. maxPriorityFeePerGas
	GasFeeCap  *big.Int // a.k.a. maxFeePerGas
	AccessList AccessList
	GasPrice   *big.Int        // wei per gas
	Gas        uint64          // gas limit
	To         *common.Address `rlp:"nil"` // nil means contract creation
	Value      *big.Int        // wei amount
	Data       []byte          // contract invocation input data
	ReviveList ReviveList      // for revive

	V, R, S *big.Int // signature values
}

func (tx *ReviveStateTx) txType() byte              { return ReviveStateTxType }
func (tx *ReviveStateTx) chainID() *big.Int         { return tx.ChainID }
func (tx *ReviveStateTx) accessList() AccessList    { return tx.AccessList }
func (tx *ReviveStateTx) data() []byte              { return tx.Data }
func (tx *ReviveStateTx) gas() uint64               { return tx.Gas }
func (tx *ReviveStateTx) gasPrice() *big.Int        { return tx.GasPrice }
func (tx *ReviveStateTx) gasTipCap() *big.Int       { return tx.GasTipCap }
func (tx *ReviveStateTx) gasFeeCap() *big.Int       { return tx.GasFeeCap }
func (tx *ReviveStateTx) value() *big.Int           { return tx.Value }
func (tx *ReviveStateTx) nonce() uint64             { return tx.Nonce }
func (tx *ReviveStateTx) to() *common.Address       { return tx.To }
func (tx *ReviveStateTx) reviveList() ReviveList    { return tx.ReviveList }
func (tx *ReviveStateTx) blobGas() uint64           { return 0 }
func (tx *ReviveStateTx) blobGasFeeCap() *big.Int   { return nil }
func (tx *ReviveStateTx) blobHashes() []common.Hash { return nil }

// copy creates a deep copy of the transaction data and initializes all fields.
func (tx *ReviveStateTx) copy() TxData {
	cpy := &ReviveStateTx{
		Nonce: tx.Nonce,
		To:    copyAddressPtr(tx.To),
		Data:  common.CopyBytes(tx.Data),
		Gas:   tx.Gas,
		// These are copied below.
		AccessList: make(AccessList, len(tx.AccessList)),
		Value:      new(big.Int),
		ChainID:    new(big.Int),
		GasTipCap:  new(big.Int),
		GasFeeCap:  new(big.Int),
		V:          new(big.Int),
		R:          new(big.Int),
		S:          new(big.Int),
	}
	copy(cpy.AccessList, tx.AccessList)
	if tx.Value != nil {
		cpy.Value.Set(tx.Value)
	}
	if tx.ChainID != nil {
		cpy.ChainID.Set(tx.ChainID)
	}
	if tx.GasTipCap != nil {
		cpy.GasTipCap.Set(tx.GasTipCap)
	}
	if tx.GasFeeCap != nil {
		cpy.GasFeeCap.Set(tx.GasFeeCap)
	}
	if tx.V != nil {
		cpy.V.Set(tx.V)
	}
	if tx.R != nil {
		cpy.R.Set(tx.R)
	}
	if tx.S != nil {
		cpy.S.Set(tx.S)
	}

	cpy.ReviveList = make(ReviveList, len(tx.ReviveList))
	copy(cpy.ReviveList, tx.ReviveList)

	return cpy
}

func (tx *ReviveStateTx) effectiveGasPrice(dst *big.Int, baseFee *big.Int) *big.Int {
	if baseFee == nil {
		return dst.Set(tx.GasFeeCap)
	}
	tip := dst.Sub(tx.GasFeeCap, baseFee)
	if tip.Cmp(tx.GasTipCap) > 0 {
		tip.Set(tx.GasTipCap)
	}
	return tip.Add(tip, baseFee)
}

func (tx *ReviveStateTx) rawSignatureValues() (v, r, s *big.Int) {
	return tx.V, tx.R, tx.S
}

func (tx *ReviveStateTx) setSignatureValues(chainID, v, r, s *big.Int) {
	tx.V, tx.R, tx.S = v, r, s
}

func (tx *ReviveStateTx) encode(b *bytes.Buffer) error {
	return rlp.Encode(b, tx)
}

func (tx *ReviveStateTx) decode(input []byte) error {
	return rlp.DecodeBytes(input, tx)
}

func ReviveIntrinsicGas(revives ReviveList) (uint64, error) {
	totalGas := uint64(0)
	for i := 0; i < len(revives); i++ {
		totalGas += revives[i].Size() * params.TxReviveKeyValueGas
	}
	return totalGas, nil
}

type ReviveKeyValues struct {
	key    []byte
	values [][]byte
}

func (r *ReviveKeyValues) Size() uint64 {
	size := uint64(0)
	for i := range r.values {
		size += uint64(len(r.values[i]))
	}
	return size + uint64(len(r.key))
}

func (r *ReviveKeyValues) Copy() ReviveKeyValues {
	copy := ReviveKeyValues{
		key:    common.CopyBytes(r.key),
		values: make([][]byte, len(r.values)),
	}
	for i := range r.values {
		copy.values[i] = common.CopyBytes(r.values[i])
	}
	return copy
}

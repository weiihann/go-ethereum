package types

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/params"
	"github.com/gballet/go-verkle"
	"github.com/stretchr/testify/assert"
)

var epochPeriod = new(big.Int).SetUint64(DefaultStateEpochPeriod)

func TestStateForkConfig(t *testing.T) {
	temp := &params.ChainConfig{}
	assert.NoError(t, temp.CheckConfigForkOrder())

	temp = &params.ChainConfig{
		HomesteadBlock:      big.NewInt(0),
		EIP150Block:         big.NewInt(0),
		EIP155Block:         big.NewInt(0),
		EIP158Block:         big.NewInt(0),
		ByzantiumBlock:      big.NewInt(0),
		ConstantinopleBlock: big.NewInt(0),
		PetersburgBlock:     big.NewInt(0),
		IstanbulBlock:       big.NewInt(0),
		LondonBlock:         big.NewInt(0),
		BerlinBlock:         big.NewInt(0),
		StateExpiryBlock1:   big.NewInt(1),
	}
	assert.NoError(t, temp.CheckConfigForkOrder())

	temp = &params.ChainConfig{
		HomesteadBlock:      big.NewInt(0),
		EIP150Block:         big.NewInt(0),
		EIP155Block:         big.NewInt(0),
		EIP158Block:         big.NewInt(0),
		ByzantiumBlock:      big.NewInt(0),
		ConstantinopleBlock: big.NewInt(0),
		PetersburgBlock:     big.NewInt(0),
		IstanbulBlock:       big.NewInt(0),
		LondonBlock:         big.NewInt(0),
		BerlinBlock:         big.NewInt(0),
		StateExpiryBlock2:   big.NewInt(1),
	}
	assert.Error(t, temp.CheckConfigForkOrder())

	temp = &params.ChainConfig{
		HomesteadBlock:      big.NewInt(0),
		EIP150Block:         big.NewInt(0),
		EIP155Block:         big.NewInt(0),
		EIP158Block:         big.NewInt(0),
		ByzantiumBlock:      big.NewInt(0),
		ConstantinopleBlock: big.NewInt(0),
		PetersburgBlock:     big.NewInt(0),
		IstanbulBlock:       big.NewInt(0),
		LondonBlock:         big.NewInt(0),
		BerlinBlock:         big.NewInt(0),
		StateExpiryBlock1:   big.NewInt(0),
		StateExpiryBlock2:   big.NewInt(0),
	}
	assert.Error(t, temp.CheckConfigForkOrder())

	temp = &params.ChainConfig{
		HomesteadBlock:      big.NewInt(0),
		EIP150Block:         big.NewInt(0),
		EIP155Block:         big.NewInt(0),
		EIP158Block:         big.NewInt(0),
		ByzantiumBlock:      big.NewInt(0),
		ConstantinopleBlock: big.NewInt(0),
		PetersburgBlock:     big.NewInt(0),
		IstanbulBlock:       big.NewInt(0),
		LondonBlock:         big.NewInt(0),
		BerlinBlock:         big.NewInt(0),
		StateExpiryBlock1:   big.NewInt(10000),
		StateExpiryBlock2:   big.NewInt(10000),
	}
	assert.Error(t, temp.CheckConfigForkOrder())

	temp = &params.ChainConfig{
		HomesteadBlock:      big.NewInt(0),
		EIP150Block:         big.NewInt(0),
		EIP155Block:         big.NewInt(0),
		EIP158Block:         big.NewInt(0),
		ByzantiumBlock:      big.NewInt(0),
		ConstantinopleBlock: big.NewInt(0),
		PetersburgBlock:     big.NewInt(0),
		IstanbulBlock:       big.NewInt(0),
		LondonBlock:         big.NewInt(0),
		BerlinBlock:         big.NewInt(0),
		StateExpiryBlock1:   big.NewInt(2),
		StateExpiryBlock2:   big.NewInt(1),
	}
	assert.Error(t, temp.CheckConfigForkOrder())

	temp = &params.ChainConfig{
		HomesteadBlock:      big.NewInt(0),
		EIP150Block:         big.NewInt(0),
		EIP155Block:         big.NewInt(0),
		EIP158Block:         big.NewInt(0),
		ByzantiumBlock:      big.NewInt(0),
		ConstantinopleBlock: big.NewInt(0),
		PetersburgBlock:     big.NewInt(0),
		IstanbulBlock:       big.NewInt(0),
		LondonBlock:         big.NewInt(0),
		BerlinBlock:         big.NewInt(0),
		StateExpiryBlock1:   big.NewInt(0),
		StateExpiryBlock2:   big.NewInt(1),
	}
	assert.Error(t, temp.CheckConfigForkOrder())

	temp = &params.ChainConfig{
		HomesteadBlock:      big.NewInt(0),
		EIP150Block:         big.NewInt(0),
		EIP155Block:         big.NewInt(0),
		EIP158Block:         big.NewInt(0),
		ByzantiumBlock:      big.NewInt(0),
		ConstantinopleBlock: big.NewInt(0),
		PetersburgBlock:     big.NewInt(0),
		IstanbulBlock:       big.NewInt(0),
		LondonBlock:         big.NewInt(0),
		BerlinBlock:         big.NewInt(0),
		StateExpiryBlock1:   big.NewInt(10000),
		StateExpiryBlock2:   big.NewInt(10001),
	}
	assert.NoError(t, temp.CheckConfigForkOrder())
}

func TestSimpleStateEpoch(t *testing.T) {
	temp := &params.ChainConfig{
		HomesteadBlock:      big.NewInt(0),
		EIP150Block:         big.NewInt(0),
		EIP155Block:         big.NewInt(0),
		EIP158Block:         big.NewInt(0),
		ByzantiumBlock:      big.NewInt(0),
		ConstantinopleBlock: big.NewInt(0),
		PetersburgBlock:     big.NewInt(0),
		IstanbulBlock:       big.NewInt(0),
		LondonBlock:         big.NewInt(0),
		BerlinBlock:         big.NewInt(0),
		StateExpiryBlock1:   big.NewInt(10000),
		StateExpiryBlock2:   big.NewInt(20000),
	}
	assert.NoError(t, temp.CheckConfigForkOrder())

	assert.Equal(t, StateEpoch0, GetStateEpoch(temp, big.NewInt(0)))
	assert.Equal(t, verkle.StateEpoch(0), GetStateEpoch(temp, big.NewInt(1000)))
	assert.Equal(t, verkle.StateEpoch(1), GetStateEpoch(temp, big.NewInt(10000)))
	assert.Equal(t, verkle.StateEpoch(1), GetStateEpoch(temp, big.NewInt(19999)))
	assert.Equal(t, verkle.StateEpoch(2), GetStateEpoch(temp, big.NewInt(20000)))
	assert.Equal(t, verkle.StateEpoch(3), GetStateEpoch(temp, new(big.Int).Add(big.NewInt(20000), epochPeriod)))
	assert.Equal(t, verkle.StateEpoch(102), GetStateEpoch(temp, new(big.Int).Add(big.NewInt(20000), new(big.Int).Mul(big.NewInt(100), epochPeriod))))
}

func TestNoZeroStateEpoch(t *testing.T) {
	temp := &params.ChainConfig{
		HomesteadBlock:      big.NewInt(0),
		EIP150Block:         big.NewInt(0),
		EIP155Block:         big.NewInt(0),
		EIP158Block:         big.NewInt(0),
		ByzantiumBlock:      big.NewInt(0),
		ConstantinopleBlock: big.NewInt(0),
		PetersburgBlock:     big.NewInt(0),
		IstanbulBlock:       big.NewInt(0),
		LondonBlock:         big.NewInt(0),
		BerlinBlock:         big.NewInt(0),
		StateExpiryBlock1:   big.NewInt(1),
		StateExpiryBlock2:   big.NewInt(2),
	}
	assert.NoError(t, temp.CheckConfigForkOrder())

	assert.Equal(t, verkle.StateEpoch(0), GetStateEpoch(temp, big.NewInt(0)))
	assert.Equal(t, verkle.StateEpoch(1), GetStateEpoch(temp, big.NewInt(1)))
	assert.Equal(t, verkle.StateEpoch(2), GetStateEpoch(temp, big.NewInt(2)))
	assert.Equal(t, verkle.StateEpoch(2), GetStateEpoch(temp, big.NewInt(10000)))
	assert.Equal(t, verkle.StateEpoch(3), GetStateEpoch(temp, new(big.Int).Add(big.NewInt(2), epochPeriod)))
	assert.Equal(t, verkle.StateEpoch(102), GetStateEpoch(temp, new(big.Int).Add(big.NewInt(2), new(big.Int).Mul(big.NewInt(100), epochPeriod))))
}

func TestNearestStateEpoch(t *testing.T) {
	temp := &params.ChainConfig{
		HomesteadBlock:      big.NewInt(0),
		EIP150Block:         big.NewInt(0),
		EIP155Block:         big.NewInt(0),
		EIP158Block:         big.NewInt(0),
		ByzantiumBlock:      big.NewInt(0),
		ConstantinopleBlock: big.NewInt(0),
		PetersburgBlock:     big.NewInt(0),
		IstanbulBlock:       big.NewInt(0),
		LondonBlock:         big.NewInt(0),
		BerlinBlock:         big.NewInt(0),
		StateExpiryBlock1:   big.NewInt(10000),
		StateExpiryBlock2:   big.NewInt(10001),
	}
	assert.NoError(t, temp.CheckConfigForkOrder())

	assert.Equal(t, verkle.StateEpoch(0), GetStateEpoch(temp, big.NewInt(0)))
	assert.Equal(t, verkle.StateEpoch(1), GetStateEpoch(temp, big.NewInt(10000)))
	assert.Equal(t, verkle.StateEpoch(2), GetStateEpoch(temp, big.NewInt(10001)))
	assert.Equal(t, verkle.StateEpoch(3), GetStateEpoch(temp, new(big.Int).Add(big.NewInt(10001), epochPeriod)))
	assert.Equal(t, verkle.StateEpoch(102), GetStateEpoch(temp, new(big.Int).Add(big.NewInt(10001), new(big.Int).Mul(big.NewInt(100), epochPeriod))))
}

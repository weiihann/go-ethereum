package nomttrie

import (
	"crypto/sha256"
	"testing"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/nomt/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStemValueDBKey(t *testing.T) {
	var stem core.StemPath
	stem[0] = 0xAA
	key := stemValueDBKey(stem, 42)

	assert.Equal(t, byte(nomtStemValuePrefix), key[0])
	assert.Equal(t, byte(0xAA), key[1])
	assert.Equal(t, byte(42), key[1+core.StemSize])
	assert.Len(t, key, 1+core.StemSize+1)
}

func TestLoadStemValuesEmpty(t *testing.T) {
	diskdb := rawdb.NewMemoryDatabase()
	var stem core.StemPath
	values, err := loadStemValues(diskdb, stem)
	require.NoError(t, err)

	for _, v := range values {
		assert.Nil(t, v)
	}
}

func TestLoadStemValuesRoundTrip(t *testing.T) {
	diskdb := rawdb.NewMemoryDatabase()
	var stem core.StemPath
	stem[0] = 0xBB

	// Write two values.
	val0 := make([]byte, 32)
	val0[0] = 0x01
	val5 := make([]byte, 32)
	val5[31] = 0xFF

	require.NoError(t, diskdb.Put(stemValueDBKey(stem, 0), val0))
	require.NoError(t, diskdb.Put(stemValueDBKey(stem, 5), val5))

	// Load and verify.
	values, err := loadStemValues(diskdb, stem)
	require.NoError(t, err)
	assert.Equal(t, val0, values[0])
	assert.Equal(t, val5, values[5])
	assert.Nil(t, values[1])
}

func TestWriteStemValues(t *testing.T) {
	diskdb := rawdb.NewMemoryDatabase()
	var stem core.StemPath
	stem[0] = 0xCC

	// Write a value.
	val := make([]byte, 32)
	val[0] = 0x42

	batch := diskdb.NewBatch()
	updates := map[byte][]byte{3: val}
	require.NoError(t, writeStemValues(batch, stem, updates))
	require.NoError(t, batch.Write())

	// Verify it was written.
	data, err := diskdb.Get(stemValueDBKey(stem, 3))
	require.NoError(t, err)
	assert.Equal(t, val, data)

	// Delete it.
	batch = diskdb.NewBatch()
	deletes := map[byte][]byte{3: nil}
	require.NoError(t, writeStemValues(batch, stem, deletes))
	require.NoError(t, batch.Write())

	has, err := diskdb.Has(stemValueDBKey(stem, 3))
	require.NoError(t, err)
	assert.False(t, has)
}

func TestGroupAndHashStemsEmpty(t *testing.T) {
	diskdb := rawdb.NewMemoryDatabase()
	result, err := groupAndHashStems(nil, diskdb)
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestGroupAndHashStemsSingleValue(t *testing.T) {
	diskdb := rawdb.NewMemoryDatabase()

	var stem core.StemPath
	stem[0] = 0x10
	val := make([]byte, 32)
	val[0] = 0x42

	updates := []stemUpdate{{Stem: stem, Suffix: 0, Value: val}}
	result, err := groupAndHashStems(updates, diskdb)
	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, stem, result[0].Stem)

	// Verify hash matches core.HashStem directly.
	var values [core.StemNodeWidth][]byte
	values[0] = val
	expectedHash := core.HashStem(stem, values)
	assert.Equal(t, expectedHash, result[0].Hash)
}

func TestGroupAndHashStemsMultipleStems(t *testing.T) {
	diskdb := rawdb.NewMemoryDatabase()

	var stemA, stemB core.StemPath
	stemA[0] = 0x10
	stemB[0] = 0x80

	val := make([]byte, 32)
	val[0] = 0x01

	// Updates across two different stems, interleaved order.
	updates := []stemUpdate{
		{Stem: stemB, Suffix: 0, Value: val},
		{Stem: stemA, Suffix: 0, Value: val},
		{Stem: stemA, Suffix: 1, Value: val},
	}

	result, err := groupAndHashStems(updates, diskdb)
	require.NoError(t, err)
	require.Len(t, result, 2)

	// Result should be sorted by stem.
	assert.Equal(t, stemA, result[0].Stem)
	assert.Equal(t, stemB, result[1].Stem)
}

func TestGroupAndHashStemsMergesExistingValues(t *testing.T) {
	diskdb := rawdb.NewMemoryDatabase()

	var stem core.StemPath
	stem[0] = 0x20

	// Pre-populate slot 0 in flat state.
	existing := make([]byte, 32)
	existing[0] = 0xAA
	require.NoError(t, diskdb.Put(stemValueDBKey(stem, 0), existing))

	// Update slot 1 only.
	newVal := make([]byte, 32)
	newVal[0] = 0xBB
	updates := []stemUpdate{{Stem: stem, Suffix: 1, Value: newVal}}

	result, err := groupAndHashStems(updates, diskdb)
	require.NoError(t, err)
	require.Len(t, result, 1)

	// Hash should include both slot 0 (existing) and slot 1 (new).
	var values [core.StemNodeWidth][]byte
	values[0] = existing
	values[1] = newVal
	expectedHash := core.HashStem(stem, values)
	assert.Equal(t, expectedHash, result[0].Hash)
}

func TestGroupAndHashStemsDeletesAreExcluded(t *testing.T) {
	diskdb := rawdb.NewMemoryDatabase()

	var stem core.StemPath
	stem[0] = 0x30

	// Pre-populate slot 0.
	existing := make([]byte, 32)
	existing[0] = 0xDD
	require.NoError(t, diskdb.Put(stemValueDBKey(stem, 0), existing))

	// Delete slot 0 (set to nil).
	updates := []stemUpdate{{Stem: stem, Suffix: 0, Value: nil}}

	result, err := groupAndHashStems(updates, diskdb)
	require.NoError(t, err)

	// No values remain → stem excluded from result.
	assert.Empty(t, result)
}

func TestGroupAndHashStemsFlatStateUpdated(t *testing.T) {
	diskdb := rawdb.NewMemoryDatabase()

	var stem core.StemPath
	stem[0] = 0x40
	val := make([]byte, 32)
	val[0] = 0xEE

	updates := []stemUpdate{{Stem: stem, Suffix: 5, Value: val}}
	_, err := groupAndHashStems(updates, diskdb)
	require.NoError(t, err)

	// Verify flat state was written.
	data, err := diskdb.Get(stemValueDBKey(stem, 5))
	require.NoError(t, err)
	assert.Equal(t, val, data)
}

func TestHashStemMatchesBintrieStemNode(t *testing.T) {
	// Cross-validate core.HashStem against bintrie's StemNode.Hash algorithm
	// using the same manual computation.
	var stem core.StemPath
	for i := range stem {
		stem[i] = byte(i + 1)
	}

	var values [core.StemNodeWidth][]byte
	values[0] = make([]byte, 32)
	values[0][0] = 0x42
	values[1] = make([]byte, 32)
	values[1][31] = 0xFF

	hash := core.HashStem(stem, values)

	// Reproduce bintrie StemNode.Hash manually.
	var data [256][32]byte
	data[0] = sha256.Sum256(values[0])
	data[1] = sha256.Sum256(values[1])

	h := sha256.New()
	for level := 1; level <= 8; level++ {
		for i := range 256 / (1 << level) {
			if data[i*2] == [32]byte{} && data[i*2+1] == [32]byte{} {
				data[i] = [32]byte{}
				continue
			}
			h.Reset()
			h.Write(data[i*2][:])
			h.Write(data[i*2+1][:])
			h.Sum(data[i][:0])
		}
	}

	h.Reset()
	h.Write(stem[:])
	h.Write([]byte{0})
	h.Write(data[0][:])
	expected := h.Sum(nil)

	assert.Equal(t, expected, hash[:])
}

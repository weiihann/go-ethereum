package nomttrie

import (
	"bytes"
	"sort"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/nomt/core"
)

// nomtStemValuePrefix is the ethdb key prefix for stem value flat state.
// Key format: 0x03 || stem[31] || suffix[1] → value[32]
const nomtStemValuePrefix byte = 0x03

// stemValueDBKey returns the ethdb key for a specific (stem, suffix) value slot.
func stemValueDBKey(stem core.StemPath, suffix byte) []byte {
	key := make([]byte, 1+core.StemSize+1)
	key[0] = nomtStemValuePrefix
	copy(key[1:], stem[:])
	key[1+core.StemSize] = suffix
	return key
}

// stemValueDBPrefix returns the ethdb prefix for all values of a stem.
func stemValueDBPrefix(stem core.StemPath) []byte {
	prefix := make([]byte, 1+core.StemSize)
	prefix[0] = nomtStemValuePrefix
	copy(prefix[1:], stem[:])
	return prefix
}

// loadStemValues loads all existing values for a stem from flat state.
// Uses prefix iteration to efficiently find only populated slots.
func loadStemValues(diskdb ethdb.Database, stem core.StemPath) ([core.StemNodeWidth][]byte, error) {
	var values [core.StemNodeWidth][]byte

	prefix := stemValueDBPrefix(stem)
	it := diskdb.NewIterator(prefix, nil)
	defer it.Release()

	for it.Next() {
		key := it.Key()
		if len(key) != 1+core.StemSize+1 {
			continue
		}
		suffix := key[1+core.StemSize]
		value := make([]byte, len(it.Value()))
		copy(value, it.Value())
		values[suffix] = value
	}
	if err := it.Error(); err != nil {
		return values, err
	}
	return values, nil
}

// writeStemValues writes updated stem values to an ethdb batch.
// Only slots marked dirty are written. Nil values delete the key.
func writeStemValues(batch ethdb.Batch, stem core.StemPath, values [core.StemNodeWidth][]byte, dirty [core.StemNodeWidth]bool) error {
	for i, d := range dirty {
		if !d {
			continue
		}
		key := stemValueDBKey(stem, byte(i))
		if values[i] == nil {
			if err := batch.Delete(key); err != nil {
				return err
			}
		} else {
			if err := batch.Put(key, values[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

// groupAndHashStems groups stem updates by stem path, loads existing values
// from flat state, merges updates, computes SHA256 stem hashes, writes updated
// values back to flat state, and returns sorted StemKeyValue pairs for the
// NOMT page tree.
//
// Returns only stems that have at least one non-nil value (empty stems are
// excluded, effectively deleting them from the trie).
func groupAndHashStems(
	updates []stemUpdate,
	diskdb ethdb.Database,
) ([]core.StemKeyValue, error) {
	if len(updates) == 0 {
		return nil, nil
	}

	// Stable sort by stem then suffix to preserve insertion order for
	// duplicate (stem, suffix) pairs — the last queued value must win.
	sort.SliceStable(updates, func(i, j int) bool {
		if updates[i].Stem != updates[j].Stem {
			return stemLess(&updates[i].Stem, &updates[j].Stem)
		}
		return updates[i].Suffix < updates[j].Suffix
	})

	batch := diskdb.NewBatch()
	result := make([]core.StemKeyValue, 0, len(updates)/2+1)

	// Process groups sharing the same stem.
	idx := 0
	for idx < len(updates) {
		stem := updates[idx].Stem

		// Load existing values.
		values, err := loadStemValues(diskdb, stem)
		if err != nil {
			return nil, err
		}

		// Apply updates.
		var dirty [core.StemNodeWidth]bool
		for idx < len(updates) && updates[idx].Stem == stem {
			u := updates[idx]
			values[u.Suffix] = u.Value
			dirty[u.Suffix] = true
			idx++
		}

		// Write to flat state.
		if err := writeStemValues(batch, stem, values, dirty); err != nil {
			return nil, err
		}

		// Only include stems with at least one non-nil value.
		hasValue := false
		for _, v := range values {
			if v != nil {
				hasValue = true
				break
			}
		}
		if hasValue {
			result = append(result, core.StemKeyValue{
				Stem: stem,
				Hash: core.HashStem(stem, values),
			})
		}
	}

	if err := batch.Write(); err != nil {
		return nil, err
	}
	return result, nil
}

// stemLess compares two stem paths lexicographically.
func stemLess(a, b *core.StemPath) bool {
	return bytes.Compare(a[:], b[:]) < 0
}

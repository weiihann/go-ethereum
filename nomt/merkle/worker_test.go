package merkle

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/ethereum/go-ethereum/nomt/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Unit tests for helpers ---

func TestPartitionByChildIndex(t *testing.T) {
	// Key 0x00... → child 0, key 0xFC... → child 63.
	kvs := []core.KeyValue{
		{Key: makeKVKey(0x00), Value: makeKVVal(1)},
		{Key: makeKVKey(0x04), Value: makeKVVal(2)}, // 0x04 >> 2 = 1
		{Key: makeKVKey(0xFC), Value: makeKVVal(3)}, // 0xFC >> 2 = 63
	}
	buckets := partitionByChildIndex(kvs)

	assert.Len(t, buckets[0], 1)
	assert.Len(t, buckets[1], 1)
	assert.Len(t, buckets[63], 1)

	// All other buckets should be empty.
	nonEmpty := 0
	for _, b := range buckets {
		if len(b) > 0 {
			nonEmpty++
		}
	}
	assert.Equal(t, 3, nonEmpty)
}

func TestChildPosition(t *testing.T) {
	// Child 0, left: 7 bits all false → depth 7.
	pos := childPosition(0, false)
	assert.Equal(t, uint16(7), pos.Depth())
	for i := range 7 {
		assert.False(t, pos.Bit(i), "bit %d should be 0", i)
	}

	// Child 0, right: 6 false + 1 true → depth 7.
	pos = childPosition(0, true)
	assert.Equal(t, uint16(7), pos.Depth())
	for i := range 6 {
		assert.False(t, pos.Bit(i), "bit %d should be 0", i)
	}
	assert.True(t, pos.Bit(6))

	// Child 63 (0b111111), left: 6 true + 1 false → depth 7.
	pos = childPosition(63, false)
	assert.Equal(t, uint16(7), pos.Depth())
	for i := range 6 {
		assert.True(t, pos.Bit(i), "bit %d should be 1", i)
	}
	assert.False(t, pos.Bit(6))

	// Child 63 (0b111111), right: 7 true → depth 7.
	pos = childPosition(63, true)
	assert.Equal(t, uint16(7), pos.Depth())
	for i := range 7 {
		assert.True(t, pos.Bit(i), "bit %d should be 1", i)
	}
}

func TestAssignToWorkers(t *testing.T) {
	// 3 non-empty buckets, 2 workers.
	var buckets [64][]core.KeyValue
	buckets[0] = []core.KeyValue{{Key: makeKVKey(0x00), Value: makeKVVal(1)}}
	buckets[10] = []core.KeyValue{{Key: makeKVKey(0x28), Value: makeKVVal(2)}} // 0x28>>2=10
	buckets[63] = []core.KeyValue{{Key: makeKVKey(0xFC), Value: makeKVVal(3)}}

	tasks := assignToWorkers(buckets, 2)
	require.Len(t, tasks, 2)
	// 3 items / 2 workers: first gets 2, second gets 1.
	assert.Len(t, tasks[0].children, 2)
	assert.Len(t, tasks[1].children, 1)
	assert.Equal(t, uint8(0), tasks[0].children[0].childIndex)
	assert.Equal(t, uint8(10), tasks[0].children[1].childIndex)
	assert.Equal(t, uint8(63), tasks[1].children[0].childIndex)
}

func TestAssignToWorkersMoreWorkersThanChildren(t *testing.T) {
	var buckets [64][]core.KeyValue
	buckets[5] = []core.KeyValue{{Key: makeKVKey(0x14), Value: makeKVVal(1)}} // 0x14>>2=5
	buckets[6] = []core.KeyValue{{Key: makeKVKey(0x18), Value: makeKVVal(2)}} // 0x18>>2=6

	tasks := assignToWorkers(buckets, 8)
	// Only 2 non-empty, so cap to 2 workers.
	require.Len(t, tasks, 2)
	assert.Len(t, tasks[0].children, 1)
	assert.Len(t, tasks[1].children, 1)
}

// --- Integration tests ---

// permissivePageSet wraps MemoryPageSet to return fresh pages for missing
// entries (matching bitboxPageSet behavior). This is needed because the
// parallel workers descend into child pages that may not exist yet.
type permissivePageSet struct {
	*MemoryPageSet
}

func (ps *permissivePageSet) Get(pageID core.PageID) (*core.RawPage, PageOrigin, bool) {
	page, origin, ok := ps.MemoryPageSet.Get(pageID)
	if !ok {
		fresh := new(core.RawPage)
		return fresh, PageOrigin{Kind: PageOriginFresh}, true
	}
	return page, origin, true
}

func memoryPageSetFactory() PageSet {
	return &permissivePageSet{NewMemoryPageSet(true)}
}

func TestParallelUpdateEmpty(t *testing.T) {
	out := ParallelUpdate(core.Terminator, nil, 4, memoryPageSetFactory)
	assert.Equal(t, core.Terminator, out.Root)
}

func TestParallelUpdateSingleKey(t *testing.T) {
	kv := core.KeyValue{Key: makeKVKey(0x50), Value: makeKVVal(1)}
	kvs := []core.KeyValue{kv}

	out := ParallelUpdate(core.Terminator, kvs, 4, memoryPageSetFactory)
	expected := expectedRoot(kvs)
	assert.Equal(t, expected, out.Root)
}

func TestParallelUpdateTwoKeysDifferentChildren(t *testing.T) {
	// 0x00 → child 0, 0x80 → child 32.
	kvs := []core.KeyValue{
		{Key: makeKVKey(0x00), Value: makeKVVal(1)},
		{Key: makeKVKey(0x80), Value: makeKVVal(2)},
	}

	out := ParallelUpdate(core.Terminator, kvs, 4, memoryPageSetFactory)
	expected := expectedRoot(kvs)
	assert.Equal(t, expected, out.Root)
}

func TestParallelUpdateSparseChildren(t *testing.T) {
	// Only children 0 and 63 have ops.
	kvs := []core.KeyValue{
		{Key: makeKVKey(0x00), Value: makeKVVal(1)},
		{Key: makeKVKey(0xFC), Value: makeKVVal(2)},
	}

	out := ParallelUpdate(core.Terminator, kvs, 4, memoryPageSetFactory)
	expected := expectedRoot(kvs)
	assert.Equal(t, expected, out.Root)
}

func TestParallelUpdateSingleChild(t *testing.T) {
	// All keys land in child 0 (first 6 bits = 000000).
	kvs := []core.KeyValue{
		{Key: makeKVKey(0x00), Value: makeKVVal(1)},
		{Key: makeKVKey(0x01), Value: makeKVVal(2)},
		{Key: makeKVKey(0x02), Value: makeKVVal(3)},
		{Key: makeKVKey(0x03), Value: makeKVVal(4)},
	}
	sort.Slice(kvs, func(i, j int) bool { return kvLess(&kvs[i], &kvs[j]) })

	out := ParallelUpdate(core.Terminator, kvs, 4, memoryPageSetFactory)
	expected := expectedRoot(kvs)
	assert.Equal(t, expected, out.Root)
}

func TestParallelUpdateFallbackSmallBatch(t *testing.T) {
	// Less than 64 ops → single-threaded fallback.
	kvs := randomKVs(10, 42)
	out := ParallelUpdate(core.Terminator, kvs, 8, memoryPageSetFactory)
	expected := expectedRoot(kvs)
	assert.Equal(t, expected, out.Root)
}

func TestParallelUpdateDeterministic(t *testing.T) {
	kvs := randomKVs(200, 99)

	r1 := ParallelUpdate(core.Terminator, kvs, 4, memoryPageSetFactory).Root
	r2 := ParallelUpdate(core.Terminator, kvs, 4, memoryPageSetFactory).Root
	assert.Equal(t, r1, r2, "same inputs should produce same root")
}

func TestParallelUpdateMatchesSingleThreaded(t *testing.T) {
	tests := []struct {
		name    string
		numKVs  int
		workers int
	}{
		{"1kv_2w", 1, 2},
		{"10kv_2w", 10, 2},
		{"100kv_2w", 100, 2},
		{"100kv_4w", 100, 4},
		{"100kv_8w", 100, 8},
		{"500kv_4w", 500, 4},
		{"1000kv_8w", 1000, 8},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			kvs := randomKVs(tc.numKVs, 12345)

			single := singleThreadedUpdate(
				core.Terminator, kvs, NewMemoryPageSet(true),
			)
			parallel := ParallelUpdate(
				core.Terminator, kvs, tc.workers, memoryPageSetFactory,
			)

			assert.Equal(t, single.Root, parallel.Root,
				"parallel root should match single-threaded root")
		})
	}
}

// --- helpers ---

func randomKVs(n int, seed int64) []core.KeyValue {
	rng := rand.New(rand.NewSource(seed))
	kvs := make([]core.KeyValue, n)
	seen := make(map[core.KeyPath]bool, n)

	for i := range n {
		for {
			var kp core.KeyPath
			rng.Read(kp[:])
			if seen[kp] {
				continue
			}
			seen[kp] = true
			var vh core.ValueHash
			rng.Read(vh[:])
			kvs[i] = core.KeyValue{Key: kp, Value: vh}
			break
		}
	}

	sort.Slice(kvs, func(i, j int) bool { return kvLess(&kvs[i], &kvs[j]) })
	return kvs
}

func kvLess(a, b *core.KeyValue) bool {
	for i := range a.Key {
		if a.Key[i] < b.Key[i] {
			return true
		}
		if a.Key[i] > b.Key[i] {
			return false
		}
	}
	return false
}

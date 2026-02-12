package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSharedBits(t *testing.T) {
	tests := []struct {
		name     string
		a, b     KeyPath
		skip     int
		expected int
	}{
		{"identical", KeyPath{0xFF}, KeyPath{0xFF}, 0, 256},
		{"differ at bit 0", KeyPath{0x80}, KeyPath{0x00}, 0, 0},
		{"share 4 bits", KeyPath{0xF0}, KeyPath{0xF8}, 0, 4},
		{"with skip", KeyPath{0xF0}, KeyPath{0xF8}, 4, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SharedBits(&tt.a, &tt.b, tt.skip)
			assert.Equal(t, tt.expected, got)
		})
	}
}

// makeKV creates a (key, value) pair where both key and value are filled with b.
func makeKV(b byte) KeyValue {
	var key KeyPath
	var val ValueHash
	for i := range key {
		key[i] = b
	}
	for i := range val {
		val[i] = b
	}
	return KeyValue{Key: key, Value: val}
}

func TestBuildTrieEmpty(t *testing.T) {
	var visited []WriteNode
	root := BuildTrie(0, nil, func(wn WriteNode) {
		visited = append(visited, wn)
	})

	require.Len(t, visited, 1)
	assert.Equal(t, WriteNodeTerminator, visited[0].Kind)
	assert.Equal(t, Terminator, root)
}

func TestBuildTrieSingleLeaf(t *testing.T) {
	kv := makeKV(0xFF)
	var visited []WriteNode
	root := BuildTrie(0, []KeyValue{kv}, func(wn WriteNode) {
		visited = append(visited, wn)
	})

	require.Len(t, visited, 1)
	assert.Equal(t, WriteNodeLeaf, visited[0].Kind)
	assert.False(t, visited[0].GoUp)
	assert.True(t, IsLeaf(&root))

	expected := HashLeaf(&LeafData{
		KeyPath:   kv.Key,
		ValueHash: kv.Value,
	})
	assert.Equal(t, expected, root)
}

func TestBuildTrieTwoLeaves(t *testing.T) {
	// Keys: 0x00... and 0xFF... differ at bit 0.
	kv0 := makeKV(0x00)
	kvF := makeKV(0xFF)

	var visited []WriteNode
	root := BuildTrie(0, []KeyValue{kv0, kvF}, func(wn WriteNode) {
		visited = append(visited, wn)
	})

	// Should visit: leaf_0, leaf_F, internal(leaf_0, leaf_F).
	require.Len(t, visited, 3)
	assert.Equal(t, WriteNodeLeaf, visited[0].Kind)
	assert.Equal(t, WriteNodeLeaf, visited[1].Kind)
	assert.Equal(t, WriteNodeInternal, visited[2].Kind)

	leaf0 := HashLeaf(&LeafData{KeyPath: kv0.Key, ValueHash: kv0.Value})
	leafF := HashLeaf(&LeafData{KeyPath: kvF.Key, ValueHash: kvF.Value})
	expected := HashInternal(&InternalData{Left: leaf0, Right: leafF})

	assert.Equal(t, expected, root)
	assert.True(t, IsInternal(&root))
}

func TestBuildTrieThreeLeaves(t *testing.T) {
	// Three keys sharing common prefixes.
	// 0b00010001... = 0x11
	// 0b00010010... = 0x12
	// 0b00010100... = 0x14
	kv1 := makeKV(0x11)
	kv2 := makeKV(0x12)
	kv3 := makeKV(0x14)

	var visited []WriteNode
	root := BuildTrie(0, []KeyValue{kv1, kv2, kv3}, func(wn WriteNode) {
		visited = append(visited, wn)
	})

	assert.True(t, IsInternal(&root) || IsLeaf(&root),
		"root should be non-terminator")

	// Verify determinism.
	var visited2 []WriteNode
	root2 := BuildTrie(0, []KeyValue{kv1, kv2, kv3}, func(wn WriteNode) {
		visited2 = append(visited2, wn)
	})
	assert.Equal(t, root, root2)
	assert.Equal(t, len(visited), len(visited2))
}

func TestBuildTrieWithSkip(t *testing.T) {
	// Keys all share prefix 0001 (4 bits): 0x11, 0x12, 0x14.
	kv1 := makeKV(0x11)
	kv2 := makeKV(0x12)
	kv3 := makeKV(0x14)

	var visited []WriteNode
	root := BuildTrie(4, []KeyValue{kv1, kv2, kv3}, func(wn WriteNode) {
		visited = append(visited, wn)
	})

	// Should produce a non-trivial sub-trie.
	assert.False(t, IsTerminator(&root))
	assert.True(t, len(visited) >= 3)
}

func TestBuildTrieMultiValue(t *testing.T) {
	// Matches the Rust multi_value test pattern.
	// 0b00010000 = 0x10
	// 0b00100000 = 0x20
	// 0b01000000 = 0x40
	// 0b10100000 = 0xA0
	// 0b10110000 = 0xB0
	kvA := makeKV(0x10)
	kvB := makeKV(0x20)
	kvC := makeKV(0x40)
	kvD := makeKV(0xA0)
	kvE := makeKV(0xB0)

	var nodes []Node
	root := BuildTrie(0, []KeyValue{kvA, kvB, kvC, kvD, kvE},
		func(wn WriteNode) {
			nodes = append(nodes, wn.Node)
		})

	// Manually verify the trie structure.
	leafA := HashLeaf(&LeafData{KeyPath: kvA.Key, ValueHash: kvA.Value})
	leafB := HashLeaf(&LeafData{KeyPath: kvB.Key, ValueHash: kvB.Value})
	leafC := HashLeaf(&LeafData{KeyPath: kvC.Key, ValueHash: kvC.Value})
	leafD := HashLeaf(&LeafData{KeyPath: kvD.Key, ValueHash: kvD.Value})
	leafE := HashLeaf(&LeafData{KeyPath: kvE.Key, ValueHash: kvE.Value})

	branchAB := HashInternal(&InternalData{Left: leafA, Right: leafB})
	branchABC := HashInternal(&InternalData{Left: branchAB, Right: leafC})

	branchDE1 := HashInternal(&InternalData{Left: leafD, Right: leafE})
	branchDE2 := HashInternal(&InternalData{Left: Terminator, Right: branchDE1})
	branchDE3 := HashInternal(&InternalData{Left: branchDE2, Right: Terminator})

	expected := HashInternal(&InternalData{Left: branchABC, Right: branchDE3})

	assert.Equal(t, expected, root)
}

func TestLeafOpsSplicedNoExisting(t *testing.T) {
	val := ValueHash{0x01}
	ops := []LeafOp{
		{Key: KeyPath{0x10}, Value: &val},
		{Key: KeyPath{0x20}, Value: &val},
	}

	result := LeafOpsSpliced(nil, ops)
	assert.Len(t, result, 2)
}

func TestLeafOpsSplicedWithExistingLeaf(t *testing.T) {
	val := ValueHash{0x01}
	ops := []LeafOp{
		{Key: KeyPath{0x10}, Value: &val},
		{Key: KeyPath{0x30}, Value: &val},
	}

	existing := &LeafData{
		KeyPath:   KeyPath{0x20},
		ValueHash: ValueHash{0x02},
	}

	result := LeafOpsSpliced(existing, ops)
	assert.Len(t, result, 3)
	assert.Equal(t, KeyPath{0x10}, result[0].Key)
	assert.Equal(t, KeyPath{0x20}, result[1].Key)
	assert.Equal(t, KeyPath{0x30}, result[2].Key)
}

func TestLeafOpsSplicedDeleteFiltered(t *testing.T) {
	val := ValueHash{0x01}
	ops := []LeafOp{
		{Key: KeyPath{0x10}, Value: &val},
		{Key: KeyPath{0x20}, Value: nil}, // delete
		{Key: KeyPath{0x30}, Value: &val},
	}

	result := LeafOpsSpliced(nil, ops)
	assert.Len(t, result, 2)
	assert.Equal(t, KeyPath{0x10}, result[0].Key)
	assert.Equal(t, KeyPath{0x30}, result[1].Key)
}

func TestLeafOpsSplicedExistingKeyInOps(t *testing.T) {
	val := ValueHash{0x01}
	newVal := ValueHash{0x99}
	ops := []LeafOp{
		{Key: KeyPath{0x20}, Value: &newVal},
	}

	existing := &LeafData{
		KeyPath:   KeyPath{0x20},
		ValueHash: val,
	}

	// The existing leaf should NOT be spliced because its key is in ops.
	result := LeafOpsSpliced(existing, ops)
	assert.Len(t, result, 1)
	assert.Equal(t, newVal, result[0].Value)
}

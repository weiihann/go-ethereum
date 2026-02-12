package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStemSharedBits(t *testing.T) {
	tests := []struct {
		name     string
		a, b     StemPath
		skip     int
		expected int
	}{
		{"identical", StemPath{0xFF}, StemPath{0xFF}, 0, 248},
		{"differ at bit 0", StemPath{0x80}, StemPath{0x00}, 0, 0},
		{"share 4 bits", StemPath{0xF0}, StemPath{0xF8}, 0, 4},
		{"with skip", StemPath{0xF0}, StemPath{0xF8}, 4, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := StemSharedBits(&tt.a, &tt.b, tt.skip)
			assert.Equal(t, tt.expected, got)
		})
	}
}

// makeSKV creates a StemKeyValue where the stem is filled with b and
// the hash is a deterministic non-zero value derived from b.
func makeSKV(b byte) StemKeyValue {
	var stem StemPath
	for i := range stem {
		stem[i] = b
	}
	// Use a simple deterministic hash for testing.
	var hash Node
	for i := range hash {
		hash[i] = b ^ byte(i)
	}
	return StemKeyValue{Stem: stem, Hash: hash}
}

func TestBuildInternalTreeEmpty(t *testing.T) {
	var visited []WriteNode
	root := BuildInternalTree(0, nil, func(wn WriteNode) {
		visited = append(visited, wn)
	})

	require.Len(t, visited, 1)
	assert.Equal(t, WriteNodeTerminator, visited[0].Kind)
	assert.Equal(t, Terminator, root)
}

func TestBuildInternalTreeSingleStem(t *testing.T) {
	skv := makeSKV(0xFF)
	var visited []WriteNode
	root := BuildInternalTree(0, []StemKeyValue{skv}, func(wn WriteNode) {
		visited = append(visited, wn)
	})

	require.Len(t, visited, 1)
	assert.Equal(t, WriteNodeStem, visited[0].Kind)
	assert.False(t, visited[0].GoUp)
	assert.False(t, IsTerminator(&root))
	assert.Equal(t, skv.Hash, root)
}

func TestBuildInternalTreeTwoStems(t *testing.T) {
	// Stems: 0x00... and 0xFF... differ at bit 0.
	skv0 := makeSKV(0x00)
	skvF := makeSKV(0xFF)

	var visited []WriteNode
	root := BuildInternalTree(0, []StemKeyValue{skv0, skvF}, func(wn WriteNode) {
		visited = append(visited, wn)
	})

	// Should visit: stem_0, stem_F, internal(stem_0, stem_F).
	require.Len(t, visited, 3)
	assert.Equal(t, WriteNodeStem, visited[0].Kind)
	assert.Equal(t, WriteNodeStem, visited[1].Kind)
	assert.Equal(t, WriteNodeInternal, visited[2].Kind)

	expected := HashInternal(&InternalData{Left: skv0.Hash, Right: skvF.Hash})
	assert.Equal(t, expected, root)
}

func TestBuildInternalTreeThreeStems(t *testing.T) {
	skv1 := makeSKV(0x11)
	skv2 := makeSKV(0x12)
	skv3 := makeSKV(0x14)

	var visited []WriteNode
	root := BuildInternalTree(0, []StemKeyValue{skv1, skv2, skv3}, func(wn WriteNode) {
		visited = append(visited, wn)
	})

	assert.False(t, IsTerminator(&root))

	// Verify determinism.
	var visited2 []WriteNode
	root2 := BuildInternalTree(0, []StemKeyValue{skv1, skv2, skv3}, func(wn WriteNode) {
		visited2 = append(visited2, wn)
	})
	assert.Equal(t, root, root2)
	assert.Equal(t, len(visited), len(visited2))
}

func TestBuildInternalTreeWithSkip(t *testing.T) {
	skv1 := makeSKV(0x11)
	skv2 := makeSKV(0x12)
	skv3 := makeSKV(0x14)

	var visited []WriteNode
	root := BuildInternalTree(4, []StemKeyValue{skv1, skv2, skv3}, func(wn WriteNode) {
		visited = append(visited, wn)
	})

	assert.False(t, IsTerminator(&root))
	assert.True(t, len(visited) >= 3)
}

func TestBuildInternalTreeMultiValue(t *testing.T) {
	skvA := makeSKV(0x10)
	skvB := makeSKV(0x20)
	skvC := makeSKV(0x40)
	skvD := makeSKV(0xA0)
	skvE := makeSKV(0xB0)

	var nodes []Node
	root := BuildInternalTree(0, []StemKeyValue{skvA, skvB, skvC, skvD, skvE},
		func(wn WriteNode) {
			nodes = append(nodes, wn.Node)
		})

	// Manually verify the trie structure.
	// A=0x10 (0001...), B=0x20 (0010...), C=0x40 (0100...)
	// D=0xA0 (1010...), E=0xB0 (1011...)
	branchAB := HashInternal(&InternalData{Left: skvA.Hash, Right: skvB.Hash})
	branchABC := HashInternal(&InternalData{Left: branchAB, Right: skvC.Hash})

	branchDE1 := HashInternal(&InternalData{Left: skvD.Hash, Right: skvE.Hash})
	branchDE2 := HashInternal(&InternalData{Left: Terminator, Right: branchDE1})
	branchDE3 := HashInternal(&InternalData{Left: branchDE2, Right: Terminator})

	expected := HashInternal(&InternalData{Left: branchABC, Right: branchDE3})

	assert.Equal(t, expected, root)
}

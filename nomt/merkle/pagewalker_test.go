package merkle

import (
	"testing"

	"github.com/ethereum/go-ethereum/nomt/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper: build a TriePosition by descending along the given bits.
func triePos(bits ...bool) core.TriePosition {
	p := core.NewTriePosition()
	for _, b := range bits {
		p.Down(b)
	}
	return p
}

// Helper: create a KeyPath with the given bits set at the MSB positions.
func keyPath(bits ...bool) core.KeyPath {
	var kp core.KeyPath
	for i, b := range bits {
		if b {
			kp[i/8] |= 1 << (7 - i%8)
		}
	}
	return kp
}

// Helper: create a ValueHash filled with a single byte.
func val(v byte) core.ValueHash {
	var vh core.ValueHash
	for i := range vh {
		vh[i] = v
	}
	return vh
}

// Helper: compute the expected root from a set of key-value pairs using
// BuildTrie directly (the "oracle").
func expectedRoot(kvs []core.KeyValue) core.Node {
	return core.BuildTrie(0, kvs, func(_ core.WriteNode) {})
}

func TestPageWalkerEmptyTrie(t *testing.T) {
	ps := NewMemoryPageSet(true)
	walker := NewPageWalker(core.Terminator, nil)
	out := walker.Conclude()
	assert.Equal(t, core.Terminator, out.Root)
	assert.Empty(t, out.Pages)
	_ = ps
}

func TestPageWalkerSingleInsert(t *testing.T) {
	ps := NewMemoryPageSet(true)
	walker := NewPageWalker(core.Terminator, nil)

	kp := keyPath(false, false)
	v := val(1)
	pos := triePos(false, false)

	walker.AdvanceAndReplace(ps, pos, []core.KeyValue{{Key: kp, Value: v}})
	out := walker.Conclude()

	expected := expectedRoot([]core.KeyValue{{Key: kp, Value: v}})
	assert.Equal(t, expected, out.Root)
	assert.True(t, core.IsLeaf(&out.Root))
}

func TestPageWalkerTwoInsertsSameAdvance(t *testing.T) {
	// Two keys that share a common prefix at position [0,0], then diverge.
	ps := NewMemoryPageSet(true)
	walker := NewPageWalker(core.Terminator, nil)

	kp1 := keyPath(false, false, true, false) // 0010...
	kp2 := keyPath(false, false, true, true)  // 0011...
	v1, v2 := val(1), val(2)

	pos := triePos(false, false)
	walker.AdvanceAndReplace(ps, pos, []core.KeyValue{
		{Key: kp1, Value: v1},
		{Key: kp2, Value: v2},
	})
	out := walker.Conclude()

	expected := expectedRoot([]core.KeyValue{
		{Key: kp1, Value: v1},
		{Key: kp2, Value: v2},
	})
	assert.Equal(t, expected, out.Root)
	assert.True(t, core.IsInternal(&out.Root))
}

func TestPageWalkerTwoAdvances(t *testing.T) {
	// First advance inserts at [0], second at [1].
	ps := NewMemoryPageSet(true)
	walker := NewPageWalker(core.Terminator, nil)

	kp0 := keyPath(false, false) // 00...
	kp1 := keyPath(true, false)  // 10...
	v0, v1 := val(1), val(2)

	walker.AdvanceAndReplace(ps, triePos(false), []core.KeyValue{
		{Key: kp0, Value: v0},
	})
	walker.AdvanceAndReplace(ps, triePos(true), []core.KeyValue{
		{Key: kp1, Value: v1},
	})
	out := walker.Conclude()

	expected := expectedRoot([]core.KeyValue{
		{Key: kp0, Value: v0},
		{Key: kp1, Value: v1},
	})
	assert.Equal(t, expected, out.Root)
}

func TestPageWalkerMultipleAdvances(t *testing.T) {
	// Match the Rust multi_value test pattern:
	// 0b00010000 = 0x10, 0b00100000 = 0x20, 0b01000000 = 0x40,
	// 0b10100000 = 0xA0, 0b10110000 = 0xB0
	ps := NewMemoryPageSet(true)
	walker := NewPageWalker(core.Terminator, nil)

	kvA := core.KeyValue{Key: makeKVKey(0x10), Value: makeKVVal(0x10)}
	kvB := core.KeyValue{Key: makeKVKey(0x20), Value: makeKVVal(0x20)}
	kvC := core.KeyValue{Key: makeKVKey(0x40), Value: makeKVVal(0x40)}
	kvD := core.KeyValue{Key: makeKVKey(0xA0), Value: makeKVVal(0xA0)}
	kvE := core.KeyValue{Key: makeKVKey(0xB0), Value: makeKVVal(0xB0)}

	allOps := []core.KeyValue{kvA, kvB, kvC, kvD, kvE}
	expected := expectedRoot(allOps)

	// Group by terminal: A and B share prefix 00, C is at 01, D and E
	// share prefix 101.
	// Terminal at [0,0]: A, B
	walker.AdvanceAndReplace(ps, triePos(false, false),
		[]core.KeyValue{kvA, kvB})
	// Terminal at [0,1]: C
	walker.AdvanceAndReplace(ps, triePos(false, true),
		[]core.KeyValue{kvC})
	// Terminal at [1]: D, E
	walker.AdvanceAndReplace(ps, triePos(true),
		[]core.KeyValue{kvD, kvE})

	out := walker.Conclude()
	assert.Equal(t, expected, out.Root)
}

func TestPageWalkerDeleteToTerminator(t *testing.T) {
	// Insert a leaf, then delete it in a second walker pass.
	ps := NewMemoryPageSet(true)

	kp := keyPath(false)
	v := val(1)

	// First: insert a leaf.
	walker1 := NewPageWalker(core.Terminator, nil)
	walker1.AdvanceAndReplace(ps, triePos(false), []core.KeyValue{
		{Key: kp, Value: v},
	})
	out1 := walker1.Conclude()
	require.True(t, core.IsLeaf(&out1.Root))
	ps.Apply(out1.Pages)

	// Second: delete it (empty ops = terminator replacement).
	walker2 := NewPageWalker(out1.Root, nil)
	walker2.AdvanceAndReplace(ps, triePos(false), nil)
	out2 := walker2.Conclude()
	assert.Equal(t, core.Terminator, out2.Root)
}

func TestPageWalkerCompactionLeafUp(t *testing.T) {
	// When one sibling becomes a terminator and the other is a leaf,
	// the leaf should be compacted upward.
	ps := NewMemoryPageSet(true)

	kp0 := keyPath(false)
	kp1 := keyPath(true)
	v := val(1)

	// Insert two leaves.
	walker1 := NewPageWalker(core.Terminator, nil)
	walker1.AdvanceAndReplace(ps, triePos(false), []core.KeyValue{
		{Key: kp0, Value: v},
	})
	walker1.AdvanceAndReplace(ps, triePos(true), []core.KeyValue{
		{Key: kp1, Value: v},
	})
	out1 := walker1.Conclude()
	ps.Apply(out1.Pages)

	// Delete the right leaf — left leaf should compact up to root.
	walker2 := NewPageWalker(out1.Root, nil)
	walker2.AdvanceAndReplace(ps, triePos(true), nil)
	out2 := walker2.Conclude()

	expectedLeaf := core.HashLeaf(&core.LeafData{KeyPath: kp0, ValueHash: v})
	assert.Equal(t, expectedLeaf, out2.Root)
}

func TestPageWalkerOutputPages(t *testing.T) {
	ps := NewMemoryPageSet(true)
	walker := NewPageWalker(core.Terminator, nil)

	kp := keyPath(false)
	v := val(1)

	walker.AdvanceAndReplace(ps, triePos(false), []core.KeyValue{
		{Key: kp, Value: v},
	})
	out := walker.Conclude()

	// Should output at least the root page.
	require.NotEmpty(t, out.Pages)
	assert.True(t, out.Pages[0].PageID.IsRoot())
}

func TestPageWalkerAdvanceBackwardsPanics(t *testing.T) {
	ps := NewMemoryPageSet(true)
	walker := NewPageWalker(core.Terminator, nil)

	walker.AdvanceAndReplace(ps, triePos(true), []core.KeyValue{
		{Key: keyPath(true), Value: val(1)},
	})

	assert.Panics(t, func() {
		walker.AdvanceAndReplace(ps, triePos(false), []core.KeyValue{
			{Key: keyPath(false), Value: val(2)},
		})
	})
}

func TestPageWalkerDeterministic(t *testing.T) {
	// Same inputs should produce the same root.
	kvs := []core.KeyValue{
		{Key: makeKVKey(0x10), Value: makeKVVal(0x10)},
		{Key: makeKVKey(0x50), Value: makeKVVal(0x50)},
		{Key: makeKVKey(0xA0), Value: makeKVVal(0xA0)},
	}

	run := func() core.Node {
		ps := NewMemoryPageSet(true)
		w := NewPageWalker(core.Terminator, nil)
		// All at root terminal since trie is empty.
		w.AdvanceAndReplace(ps, triePos(false), kvs[:2])
		w.AdvanceAndReplace(ps, triePos(true), kvs[2:])
		return w.Conclude().Root
	}

	r1 := run()
	r2 := run()
	assert.Equal(t, r1, r2)
}

func TestPageWalkerIncrementalUpdates(t *testing.T) {
	// Build a trie, apply updates, verify the resulting root matches
	// building from scratch.
	ps := NewMemoryPageSet(true)

	kp0 := makeKVKey(0x10)
	kp1 := makeKVKey(0x80)
	v1, v2 := makeKVVal(0x01), makeKVVal(0x02)

	// Pass 1: insert two keys.
	w1 := NewPageWalker(core.Terminator, nil)
	w1.AdvanceAndReplace(ps, triePos(false), []core.KeyValue{
		{Key: kp0, Value: v1},
	})
	w1.AdvanceAndReplace(ps, triePos(true), []core.KeyValue{
		{Key: kp1, Value: v1},
	})
	out1 := w1.Conclude()
	ps.Apply(out1.Pages)

	// Pass 2: update the second key's value.
	w2 := NewPageWalker(out1.Root, nil)
	w2.AdvanceAndReplace(ps, triePos(true), []core.KeyValue{
		{Key: kp1, Value: v2},
	})
	out2 := w2.Conclude()

	// The expected root should match building the whole trie from scratch
	// with the updated value.
	expected := expectedRoot([]core.KeyValue{
		{Key: kp0, Value: v1},
		{Key: kp1, Value: v2},
	})
	assert.Equal(t, expected, out2.Root)
}

func TestPageWalkerAdvanceWithoutModify(t *testing.T) {
	// Test the Advance (read-only) method.
	ps := NewMemoryPageSet(true)
	walker := NewPageWalker(core.Terminator, nil)

	kp0 := keyPath(false, false, true, false) // 0010...
	kp1 := keyPath(false, false, true, true)  // 0011...
	kp2 := keyPath(true)                      // 1...
	v := val(1)

	walker.AdvanceAndReplace(ps, triePos(false, false), []core.KeyValue{
		{Key: kp0, Value: v},
		{Key: kp1, Value: v},
	})

	// Advance past [0,1] without modifying — the walker should still
	// compact correctly.
	walker.Advance(triePos(false, true))

	walker.AdvanceAndReplace(ps, triePos(true), []core.KeyValue{
		{Key: kp2, Value: v},
	})

	out := walker.Conclude()

	expected := expectedRoot([]core.KeyValue{
		{Key: kp0, Value: v},
		{Key: kp1, Value: v},
		{Key: kp2, Value: v},
	})
	assert.Equal(t, expected, out.Root)
}

func TestPageWalkerPageDiffs(t *testing.T) {
	// Verify that output pages have non-empty diffs.
	ps := NewMemoryPageSet(true)
	walker := NewPageWalker(core.Terminator, nil)

	kp := keyPath(false, false)
	v := val(1)
	walker.AdvanceAndReplace(ps, triePos(false, false), []core.KeyValue{
		{Key: kp, Value: v},
	})
	out := walker.Conclude()

	require.NotEmpty(t, out.Pages)
	// The root page should have at least one changed node.
	assert.True(t, out.Pages[0].Diff.Count() > 0,
		"page diff should track changed nodes")
}

// --- helpers ---

func makeKVKey(b byte) core.KeyPath {
	var kp core.KeyPath
	for i := range kp {
		kp[i] = b
	}
	return kp
}

func makeKVVal(b byte) core.ValueHash {
	var vh core.ValueHash
	for i := range vh {
		vh[i] = b
	}
	return vh
}

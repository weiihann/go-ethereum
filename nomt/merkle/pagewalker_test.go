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

// makeStemPath creates a StemPath filled with a single byte.
func makeStemPath(b byte) core.StemPath {
	var sp core.StemPath
	for i := range sp {
		sp[i] = b
	}
	return sp
}

// makeSKV creates a StemKeyValue where the stem is filled with b
// and the hash is a deterministic non-zero value derived from b.
func makeSKV(b byte) core.StemKeyValue {
	stem := makeStemPath(b)
	var hash core.Node
	for i := range hash {
		hash[i] = b ^ byte(i)
	}
	return core.StemKeyValue{Stem: stem, Hash: hash}
}

// expectedRoot computes the expected root hash for a set of stem key-values
// using BuildInternalTree as the oracle. This splits at bit 0 (left/right)
// and hashes up, matching the PageWalker's singleThreadedUpdate approach.
func expectedRoot(skvs []core.StemKeyValue) core.Node {
	if len(skvs) == 0 {
		return core.Terminator
	}
	var left, right []core.StemKeyValue
	for _, skv := range skvs {
		if skv.Stem[0]&0x80 == 0 {
			left = append(left, skv)
		} else {
			right = append(right, skv)
		}
	}
	leftHash := core.BuildInternalTree(1, left, func(_ core.WriteNode) {})
	rightHash := core.BuildInternalTree(1, right, func(_ core.WriteNode) {})
	if core.IsTerminator(&leftHash) && core.IsTerminator(&rightHash) {
		return core.Terminator
	}
	return core.HashInternal(&core.InternalData{Left: leftHash, Right: rightHash})
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

	skv := makeSKV(0x00) // stem 0x00..., first bit = 0
	pos := triePos(false)

	walker.AdvanceAndReplace(ps, pos, []core.StemKeyValue{skv})
	out := walker.Conclude()

	expected := expectedRoot([]core.StemKeyValue{skv})
	assert.Equal(t, expected, out.Root)
	assert.False(t, core.IsTerminator(&out.Root))
}

func TestPageWalkerTwoInsertsSameAdvance(t *testing.T) {
	// Two stems that share a common prefix, then diverge.
	ps := NewMemoryPageSet(true)
	walker := NewPageWalker(core.Terminator, nil)

	skv1 := makeSKV(0x00) // 0000 0000...
	skv2 := makeSKV(0x01) // 0000 0001... (differ at bit 7)

	pos := triePos(false) // advance to left subtree
	walker.AdvanceAndReplace(ps, pos, []core.StemKeyValue{skv1, skv2})
	out := walker.Conclude()

	expected := expectedRoot([]core.StemKeyValue{skv1, skv2})
	assert.Equal(t, expected, out.Root)
}

func TestPageWalkerTwoAdvances(t *testing.T) {
	// First advance inserts at [0], second at [1].
	ps := NewMemoryPageSet(true)
	walker := NewPageWalker(core.Terminator, nil)

	skv0 := makeSKV(0x00) // first bit 0
	skv1 := makeSKV(0x80) // first bit 1

	walker.AdvanceAndReplace(ps, triePos(false), []core.StemKeyValue{skv0})
	walker.AdvanceAndReplace(ps, triePos(true), []core.StemKeyValue{skv1})
	out := walker.Conclude()

	expected := expectedRoot([]core.StemKeyValue{skv0, skv1})
	assert.Equal(t, expected, out.Root)
}

func TestPageWalkerMultipleAdvances(t *testing.T) {
	// Match the multi_value test pattern.
	ps := NewMemoryPageSet(true)
	walker := NewPageWalker(core.Terminator, nil)

	skvA := makeSKV(0x10) // 0001...
	skvB := makeSKV(0x20) // 0010...
	skvC := makeSKV(0x40) // 0100...
	skvD := makeSKV(0xA0) // 1010...
	skvE := makeSKV(0xB0) // 1011...

	allOps := []core.StemKeyValue{skvA, skvB, skvC, skvD, skvE}
	expected := expectedRoot(allOps)

	// Group by terminal: A and B share prefix 00, C is at 01, D and E
	// share prefix 101.
	walker.AdvanceAndReplace(ps, triePos(false, false),
		[]core.StemKeyValue{skvA, skvB})
	walker.AdvanceAndReplace(ps, triePos(false, true),
		[]core.StemKeyValue{skvC})
	walker.AdvanceAndReplace(ps, triePos(true),
		[]core.StemKeyValue{skvD, skvE})

	out := walker.Conclude()
	assert.Equal(t, expected, out.Root)
}

func TestPageWalkerDeleteToTerminator(t *testing.T) {
	// Insert a stem, then delete it in a second walker pass.
	ps := NewMemoryPageSet(true)

	skv := makeSKV(0x00) // left child

	// First: insert a stem.
	walker1 := NewPageWalker(core.Terminator, nil)
	walker1.AdvanceAndReplace(ps, triePos(false), []core.StemKeyValue{skv})
	out1 := walker1.Conclude()
	require.False(t, core.IsTerminator(&out1.Root))
	ps.Apply(out1.Pages)

	// Second: delete it (empty ops = terminator replacement).
	walker2 := NewPageWalker(out1.Root, nil)
	walker2.AdvanceAndReplace(ps, triePos(false), nil)
	out2 := walker2.Conclude()
	assert.Equal(t, core.Terminator, out2.Root)
}

func TestPageWalkerNoLeafCompaction(t *testing.T) {
	// In EIP-7864, when one sibling becomes a terminator and the other is
	// a stem hash, they form an internal node (no compaction upward).
	ps := NewMemoryPageSet(true)

	skv0 := makeSKV(0x00) // left
	skv1 := makeSKV(0x80) // right

	// Insert two stems.
	walker1 := NewPageWalker(core.Terminator, nil)
	walker1.AdvanceAndReplace(ps, triePos(false), []core.StemKeyValue{skv0})
	walker1.AdvanceAndReplace(ps, triePos(true), []core.StemKeyValue{skv1})
	out1 := walker1.Conclude()
	ps.Apply(out1.Pages)

	// Delete the right stem — root should be HashInternal(stemHash, Terminator),
	// NOT just stemHash (no compaction).
	walker2 := NewPageWalker(out1.Root, nil)
	walker2.AdvanceAndReplace(ps, triePos(true), nil)
	out2 := walker2.Conclude()

	expected := core.HashInternal(&core.InternalData{Left: skv0.Hash, Right: core.Terminator})
	assert.Equal(t, expected, out2.Root)
}

func TestPageWalkerOutputPages(t *testing.T) {
	ps := NewMemoryPageSet(true)
	walker := NewPageWalker(core.Terminator, nil)

	skv := makeSKV(0x00)
	walker.AdvanceAndReplace(ps, triePos(false), []core.StemKeyValue{skv})
	out := walker.Conclude()

	// Should output at least the root page.
	require.NotEmpty(t, out.Pages)
	assert.True(t, out.Pages[0].PageID.IsRoot())
}

func TestPageWalkerAdvanceBackwardsPanics(t *testing.T) {
	ps := NewMemoryPageSet(true)
	walker := NewPageWalker(core.Terminator, nil)

	walker.AdvanceAndReplace(ps, triePos(true), []core.StemKeyValue{makeSKV(0x80)})

	assert.Panics(t, func() {
		walker.AdvanceAndReplace(ps, triePos(false), []core.StemKeyValue{makeSKV(0x00)})
	})
}

func TestPageWalkerDeterministic(t *testing.T) {
	skvs := []core.StemKeyValue{
		makeSKV(0x10),
		makeSKV(0x50),
		makeSKV(0xA0),
	}

	run := func() core.Node {
		ps := NewMemoryPageSet(true)
		w := NewPageWalker(core.Terminator, nil)
		w.AdvanceAndReplace(ps, triePos(false), skvs[:2])
		w.AdvanceAndReplace(ps, triePos(true), skvs[2:])
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

	skv0 := makeSKV(0x10) // left
	skv1 := makeSKV(0x80) // right
	skv1Updated := core.StemKeyValue{
		Stem: skv1.Stem,
		Hash: core.Node{0xFF, 0xFE, 0xFD}, // different hash
	}

	// Pass 1: insert two stems.
	w1 := NewPageWalker(core.Terminator, nil)
	w1.AdvanceAndReplace(ps, triePos(false), []core.StemKeyValue{skv0})
	w1.AdvanceAndReplace(ps, triePos(true), []core.StemKeyValue{skv1})
	out1 := w1.Conclude()
	ps.Apply(out1.Pages)

	// Pass 2: update the second stem's hash.
	w2 := NewPageWalker(out1.Root, nil)
	w2.AdvanceAndReplace(ps, triePos(true), []core.StemKeyValue{skv1Updated})
	out2 := w2.Conclude()

	// The expected root should match building from scratch with updated hash.
	expected := expectedRoot([]core.StemKeyValue{skv0, skv1Updated})
	assert.Equal(t, expected, out2.Root)
}

func TestPageWalkerAdvanceWithoutModify(t *testing.T) {
	ps := NewMemoryPageSet(true)
	walker := NewPageWalker(core.Terminator, nil)

	skvA := makeSKV(0x00) // 0000...
	skvB := makeSKV(0x01) // 0000 0001... (share prefix with A)
	skvC := makeSKV(0x80) // 1000...

	walker.AdvanceAndReplace(ps, triePos(false), []core.StemKeyValue{skvA, skvB})

	// Advance past [0,1] without modifying.
	walker.Advance(triePos(false, true))

	walker.AdvanceAndReplace(ps, triePos(true), []core.StemKeyValue{skvC})

	out := walker.Conclude()

	expected := expectedRoot([]core.StemKeyValue{skvA, skvB, skvC})
	assert.Equal(t, expected, out.Root)
}

func TestPageWalkerPageDiffs(t *testing.T) {
	ps := NewMemoryPageSet(true)
	walker := NewPageWalker(core.Terminator, nil)

	skv := makeSKV(0x00)
	walker.AdvanceAndReplace(ps, triePos(false), []core.StemKeyValue{skv})
	out := walker.Conclude()

	require.NotEmpty(t, out.Pages)
	assert.True(t, out.Pages[0].Diff.Count() > 0,
		"page diff should track changed nodes")
}

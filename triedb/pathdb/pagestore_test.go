// Copyright 2025 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package pathdb

import (
	"bytes"
	"crypto/sha256"
	"math/rand"
	"testing"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
)

// makeNodeBlob creates a deterministic 65-byte node blob for testing.
func makeNodeBlob(seed byte) []byte {
	blob := make([]byte, NodeSize)
	blob[0] = nodeTypeInternal
	for i := 1; i < NodeSize; i++ {
		blob[i] = seed + byte(i)
	}
	return blob
}

func TestPageSerializeDeserializeRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		slots []int
	}{
		{"empty page", nil},
		{"single slot", []int{0}},
		{"sparse page", []int{0, 5, 20, 62}},
		{"full page", allSlots()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewPage()
			for _, s := range tt.slots {
				p.SetNode(s, makeNodeBlob(byte(s)))
			}

			data := p.Serialize()
			p2, err := DeserializePage(data)
			if err != nil {
				t.Fatalf("deserialize failed: %v", err)
			}
			if p2.version != PageVersion {
				t.Errorf("version: got %d, want %d", p2.version, PageVersion)
			}
			if p2.bitmap != p.bitmap {
				t.Errorf("bitmap: got %064b, want %064b", p2.bitmap, p.bitmap)
			}
			for s := 0; s < PageSlots; s++ {
				a := p.GetNode(s)
				b := p2.GetNode(s)
				if !bytes.Equal(a, b) {
					t.Errorf("slot %d mismatch", s)
				}
			}
		})
	}
}

func TestPageSetGetDelete(t *testing.T) {
	p := NewPage()

	if !p.IsEmpty() {
		t.Fatal("new page should be empty")
	}

	blob := makeNodeBlob(42)
	p.SetNode(10, blob)

	if p.IsEmpty() {
		t.Fatal("page with one node should not be empty")
	}
	got := p.GetNode(10)
	if !bytes.Equal(got, blob) {
		t.Fatal("GetNode returned wrong data")
	}

	p.DeleteNode(10)
	if !p.IsEmpty() {
		t.Fatal("page should be empty after delete")
	}
	if p.GetNode(10) != nil {
		t.Fatal("deleted slot should return nil")
	}
}

func TestPageBoundarySlots(t *testing.T) {
	p := NewPage()

	// Out-of-range operations should not panic.
	p.SetNode(-1, makeNodeBlob(0))
	p.SetNode(PageSlots, makeNodeBlob(0))
	p.DeleteNode(-1)
	p.DeleteNode(PageSlots)

	if p.GetNode(-1) != nil {
		t.Error("negative slot should return nil")
	}
	if p.GetNode(PageSlots) != nil {
		t.Error("out-of-range slot should return nil")
	}
}

func TestPathToSlot(t *testing.T) {
	// Slot 0: root of page subtree (local depth 0)
	if got := PathToSlot([]byte{0, 0, 0, 0, 0, 0}); got != 0 {
		t.Errorf("depth-0 root: got %d, want 0", got)
	}

	// Depth 1: slots 1, 2
	// Path ending in [0] -> slot 1
	path := make([]byte, PageDepth+1)
	path[PageDepth] = 0
	if got := PathToSlot(path); got != 1 {
		t.Errorf("depth-1 left: got %d, want 1", got)
	}
	// Path ending in [1] -> slot 2
	path[PageDepth] = 1
	if got := PathToSlot(path); got != 2 {
		t.Errorf("depth-1 right: got %d, want 2", got)
	}

	// Depth 2: slots 3..6
	path2 := make([]byte, PageDepth+2)
	// [0,0] -> slot 3
	if got := PathToSlot(path2); got != 3 {
		t.Errorf("depth-2 [0,0]: got %d, want 3", got)
	}
	// [0,1] -> slot 4
	path2[PageDepth+1] = 1
	if got := PathToSlot(path2); got != 4 {
		t.Errorf("depth-2 [0,1]: got %d, want 4", got)
	}
	// [1,0] -> slot 5
	path2[PageDepth] = 1
	path2[PageDepth+1] = 0
	if got := PathToSlot(path2); got != 5 {
		t.Errorf("depth-2 [1,0]: got %d, want 5", got)
	}
	// [1,1] -> slot 6
	path2[PageDepth+1] = 1
	if got := PathToSlot(path2); got != 6 {
		t.Errorf("depth-2 [1,1]: got %d, want 6", got)
	}

	// Verify all 63 slots are reachable
	seen := make(map[int]bool, PageSlots)
	for d := 0; d < PageDepth; d++ {
		count := 1 << uint(d)
		for v := 0; v < count; v++ {
			p := make([]byte, PageDepth+d)
			for i := 0; i < d; i++ {
				if v&(1<<uint(d-1-i)) != 0 {
					p[PageDepth+i] = 1
				}
			}
			slot := PathToSlot(p)
			if slot < 0 || slot >= PageSlots {
				t.Fatalf("slot %d out of range for depth %d val %d", slot, d, v)
			}
			if seen[slot] {
				t.Fatalf("duplicate slot %d", slot)
			}
			seen[slot] = true
		}
	}
	if len(seen) != PageSlots {
		t.Errorf("expected %d unique slots, got %d", PageSlots, len(seen))
	}
}

func TestSlotToLocalPathRoundTrip(t *testing.T) {
	for slot := 0; slot < PageSlots; slot++ {
		local := SlotToLocalPath(slot)
		// Reconstruct the full path with a page-aligned prefix
		fullPath := make([]byte, PageDepth+len(local))
		copy(fullPath[PageDepth:], local)
		gotSlot := PathToSlot(fullPath)
		if gotSlot != slot {
			t.Errorf(
				"round-trip failed: slot %d -> local %v -> slot %d",
				slot, local, gotSlot,
			)
		}
	}
}

func TestPathToPageKey(t *testing.T) {
	// Account trie: prefix "P"
	path := make([]byte, 12) // 2 page levels
	path[0] = 1
	path[6] = 1
	key := PathToPageKey(common.Hash{}, path)
	if key[0] != 'P' {
		t.Errorf("account key prefix: got %c, want P", key[0])
	}

	// Storage trie: prefix "Q" + owner hash
	owner := common.HexToHash("0x1234")
	key = PathToPageKey(owner, path)
	if key[0] != 'Q' {
		t.Errorf("storage key prefix: got %c, want Q", key[0])
	}
	gotOwner := common.BytesToHash(key[1 : 1+common.HashLength])
	if gotOwner != owner {
		t.Errorf("owner hash mismatch")
	}

	// Short path (within first page) should have empty packed portion.
	shortPath := make([]byte, 3)
	key = PathToPageKey(common.Hash{}, shortPath)
	if len(key) != 1 {
		t.Errorf(
			"short path key length: got %d, want 1 (prefix only)",
			len(key),
		)
	}
}

func TestBitPack(t *testing.T) {
	tests := []struct {
		name   string
		input  []byte
		expect []byte
	}{
		{"empty", nil, nil},
		{
			"8 bits",
			[]byte{1, 0, 1, 0, 1, 0, 1, 0},
			[]byte{0xAA},
		},
		{
			"6 bits",
			[]byte{1, 1, 0, 0, 1, 1},
			[]byte{0xCC},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BitPack(tt.input)
			if !bytes.Equal(got, tt.expect) {
				t.Errorf("BitPack: got %x, want %x", got, tt.expect)
			}
		})
	}
}

func TestBitPackUnpackRoundTrip(t *testing.T) {
	original := []byte{1, 0, 1, 1, 0, 0, 1, 0, 1, 1, 0, 1}
	packed := BitPack(original)
	unpacked := BitUnpack(packed, len(original))
	if !bytes.Equal(original, unpacked) {
		t.Errorf("round-trip failed: got %v, want %v", unpacked, original)
	}
}

func TestDeserializePageErrors(t *testing.T) {
	// Too short
	if _, err := DeserializePage([]byte{1}); err == nil {
		t.Error("expected error for short data")
	}

	// Wrong length (bitmap says 1 node, but no node data)
	data := make([]byte, 9)
	data[0] = PageVersion
	data[1] = 1 // bitmap bit 0 set
	if _, err := DeserializePage(data); err == nil {
		t.Error("expected error for length mismatch")
	}
}

func TestPopcount64(t *testing.T) {
	tests := []struct {
		val  uint64
		want int
	}{
		{0, 0},
		{1, 1},
		{0xFF, 8},
		{0xFFFFFFFFFFFFFFFF, 64},
		{0x8000000000000001, 2},
	}
	for _, tt := range tests {
		if got := popcount64(tt.val); got != tt.want {
			t.Errorf(
				"popcount64(%x): got %d, want %d",
				tt.val, got, tt.want,
			)
		}
	}
}

func BenchmarkPageSerialize(b *testing.B) {
	p := NewPage()
	for i := 0; i < PageSlots; i++ {
		p.SetNode(i, makeNodeBlob(byte(i)))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Serialize()
	}
}

func BenchmarkPageDeserialize(b *testing.B) {
	p := NewPage()
	for i := 0; i < PageSlots; i++ {
		p.SetNode(i, makeNodeBlob(byte(i)))
	}
	data := p.Serialize()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DeserializePage(data)
	}
}

// allSlots returns indices 0..62 for a fully-populated page.
func allSlots() []int {
	s := make([]int, PageSlots)
	for i := range s {
		s[i] = i
	}
	return s
}

// makeInternalNodeBlob creates a realistic 65-byte InternalNode blob
// from two child hashes.
func makeInternalNodeBlob(left, right common.Hash) []byte {
	blob := make([]byte, NodeSize)
	blob[0] = nodeTypeInternal
	copy(blob[1:33], left[:])
	copy(blob[33:65], right[:])
	return blob
}

// generateTriePaths generates N random bit-paths of the given depth.
func generateTriePaths(n, depth int, seed int64) [][]byte {
	rng := rand.New(rand.NewSource(seed))
	paths := make([][]byte, n)
	for i := range paths {
		p := make([]byte, depth)
		for j := range p {
			p[j] = byte(rng.Intn(2))
		}
		paths[i] = p
	}
	return paths
}

// collectAllPrefixNodes generates deterministic InternalNode blobs for
// every prefix of every path.
func collectAllPrefixNodes(paths [][]byte) map[string][]byte {
	blobs := make(map[string][]byte)
	for _, path := range paths {
		for depth := 0; depth < len(path); depth++ {
			prefix := string(path[:depth])
			if _, exists := blobs[prefix]; exists {
				continue
			}
			h := sha256.Sum256([]byte(prefix))
			var h2 common.Hash
			h2[0] = h[0] ^ 0xFF
			blobs[prefix] = makeInternalNodeBlob(
				common.Hash(h), h2,
			)
		}
	}
	return blobs
}

// countingDB wraps a database to count Get operations.
type countingDB struct {
	ethdb.Database
	gets uint64
}

func (c *countingDB) Get(key []byte) ([]byte, error) {
	c.gets++
	return c.Database.Get(key)
}

func (c *countingDB) Has(key []byte) (bool, error) {
	return c.Database.Has(key)
}

// readNodeCached checks the clean cache first, then falls back to
// ReadNodeFromPage, mimicking diskLayer.node()'s actual read path.
func readNodeCached(
	db ethdb.KeyValueReader,
	owner common.Hash,
	path []byte,
	cache *fastcache.Cache,
) []byte {
	if cache != nil {
		key := nodeCacheKey(owner, path)
		if blob := cache.Get(nil, key); len(blob) > 0 {
			return blob
		}
	}
	blob, _ := ReadNodeFromPage(db, owner, path, cache)
	return blob
}

// writeIndividualNodes writes blobs as individual DB entries.
func writeIndividualNodes(db ethdb.Database, blobs map[string][]byte) {
	batch := db.NewBatch()
	for pathStr, blob := range blobs {
		rawdb.WriteAccountTrieNode(batch, []byte(pathStr), blob)
	}
	batch.Write()
}

// writePagePackedNodes groups blobs into pages and writes pages.
func writePagePackedNodes(db ethdb.Database, blobs map[string][]byte) {
	pages := make(map[string]*Page)
	for pathStr, blob := range blobs {
		pathBytes := []byte(pathStr)
		pk := string(PathToPageKey(common.Hash{}, pathBytes))
		if pages[pk] == nil {
			pages[pk] = NewPage()
		}
		slot := PathToSlot(pathBytes)
		pages[pk].SetNode(slot, blob)
	}
	batch := db.NewBatch()
	for pk, page := range pages {
		writeTriePage(batch, common.Hash{}, pk, page.Serialize())
	}
	batch.Write()
}

// TestPageReadReduction verifies that page-packed storage reduces
// actual DB read count by ~6x. This is the core optimization metric.
func TestPageReadReduction(t *testing.T) {
	const depth = 48
	for _, numPaths := range []int{10, 100} {
		paths := generateTriePaths(numPaths, depth, 42)
		allNodes := collectAllPrefixNodes(paths)

		// Individual reads: each node requires its own DB Get.
		indivDB := &countingDB{Database: rawdb.NewMemoryDatabase()}
		writeIndividualNodes(indivDB.Database, allNodes)
		for _, path := range paths {
			for d := 0; d < depth; d++ {
				rawdb.ReadAccountTrieNode(indivDB, path[:d])
			}
		}
		indivGets := indivDB.gets

		// Page-packed reads: page read covers 6 levels + cache.
		pageDB := &countingDB{Database: rawdb.NewMemoryDatabase()}
		writePagePackedNodes(pageDB.Database, allNodes)
		cache := fastcache.New(16 * 1024 * 1024)
		for _, path := range paths {
			for d := 0; d < depth; d++ {
				readNodeCached(pageDB, common.Hash{}, path[:d], cache)
			}
		}
		pageGets := pageDB.gets

		ratio := float64(indivGets) / float64(pageGets)
		t.Logf(
			"paths=%d nodes=%d | Individual Gets=%d Page Gets=%d "+
				"Ratio=%.1fx reduction",
			numPaths, len(allNodes), indivGets, pageGets, ratio,
		)

		if ratio < 3.0 {
			t.Errorf(
				"expected at least 3x read reduction, got %.1fx "+
					"(individual=%d page=%d)",
				ratio, indivGets, pageGets,
			)
		}
	}
}

// BenchmarkPageReadVsIndividual measures read throughput with PebbleDB
// to capture real disk IO characteristics. The key metrics are:
// - Total time (with disk IO overhead)
// - DB Get operations (reported via b.ReportMetric)
func BenchmarkPageReadVsIndividual(b *testing.B) {
	const depth = 48

	for _, numPaths := range []int{10, 100} {
		paths := generateTriePaths(numPaths, depth, 42)
		allNodes := collectAllPrefixNodes(paths)
		totalReads := numPaths * depth

		b.Run(labelFor("Individual", numPaths), func(b *testing.B) {
			db := rawdb.NewMemoryDatabase()
			writeIndividualNodes(db, allNodes)
			cdb := &countingDB{Database: db}
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				cdb.gets = 0
				for _, path := range paths {
					for d := 0; d < depth; d++ {
						rawdb.ReadAccountTrieNode(cdb, path[:d])
					}
				}
			}
			b.ReportMetric(
				float64(cdb.gets)/float64(totalReads),
				"gets/read",
			)
		})

		b.Run(labelFor("PageCold", numPaths), func(b *testing.B) {
			db := rawdb.NewMemoryDatabase()
			writePagePackedNodes(db, allNodes)
			cdb := &countingDB{Database: db}
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				cdb.gets = 0
				cache := fastcache.New(4 * 1024 * 1024)
				for _, path := range paths {
					for d := 0; d < depth; d++ {
						readNodeCached(
							cdb, common.Hash{}, path[:d], cache,
						)
					}
				}
			}
			b.ReportMetric(
				float64(cdb.gets)/float64(totalReads),
				"gets/read",
			)
		})

		b.Run(labelFor("PageWarm", numPaths), func(b *testing.B) {
			db := rawdb.NewMemoryDatabase()
			writePagePackedNodes(db, allNodes)
			cache := fastcache.New(16 * 1024 * 1024)
			// Pre-warm: populate cache once.
			for _, path := range paths {
				for d := 0; d < depth; d++ {
					readNodeCached(
						db, common.Hash{}, path[:d], cache,
					)
				}
			}
			cdb := &countingDB{Database: db}
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				cdb.gets = 0
				for _, path := range paths {
					for d := 0; d < depth; d++ {
						readNodeCached(
							cdb, common.Hash{}, path[:d], cache,
						)
					}
				}
			}
			b.ReportMetric(
				float64(cdb.gets)/float64(totalReads),
				"gets/read",
			)
		})
	}
}

// BenchmarkWritePageVsIndividual compares write throughput.
func BenchmarkWritePageVsIndividual(b *testing.B) {
	const depth = 48
	for _, numPaths := range []int{10, 100} {
		paths := generateTriePaths(numPaths, depth, 42)
		allNodes := collectAllPrefixNodes(paths)

		b.Run(labelFor("Individual", numPaths), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				db := rawdb.NewMemoryDatabase()
				writeIndividualNodes(db, allNodes)
			}
		})

		b.Run(labelFor("PagePacked", numPaths), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				db := rawdb.NewMemoryDatabase()
				writePagePackedNodes(db, allNodes)
			}
		})
	}
}

func labelFor(method string, n int) string {
	switch {
	case n >= 1000:
		return method + "/1K"
	case n >= 100:
		return method + "/100"
	default:
		return method + "/10"
	}
}

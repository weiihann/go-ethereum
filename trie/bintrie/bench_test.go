// Copyright 2025 go-ethereum Authors
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

package bintrie

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/trie"
)

// generateKeys produces n deterministic 32-byte keys from a seeded PRNG.
func generateKeys(n int, seed int64) [][]byte {
	rng := rand.New(rand.NewSource(seed))
	keys := make([][]byte, n)
	for i := range keys {
		k := make([]byte, HashSize)
		rng.Read(k)
		keys[i] = k
	}
	return keys
}

// generateValues produces n deterministic 32-byte values from a seeded PRNG.
func generateValues(n int, seed int64) [][]byte {
	rng := rand.New(rand.NewSource(seed))
	vals := make([][]byte, n)
	for i := range vals {
		v := make([]byte, HashSize)
		rng.Read(v)
		vals[i] = v
	}
	return vals
}

// buildTrie inserts n key-value pairs into a fresh BinaryNode tree.
func buildTrie(n int) (BinaryNode, [][]byte, [][]byte) {
	keys := generateKeys(n, 42)
	vals := generateValues(n, 99)
	var root BinaryNode = Empty{}
	for i := range n {
		var err error
		root, err = root.Insert(keys[i], vals[i], nil, 0)
		if err != nil {
			panic(err)
		}
	}
	return root, keys, vals
}

// buildBinaryTrie creates a BinaryTrie with n accounts inserted.
func buildBinaryTrie(n int) *BinaryTrie {
	t := &BinaryTrie{
		root:   NewBinaryNode(),
		tracer: trie.NewPrevalueTracer(),
	}
	rng := rand.New(rand.NewSource(42))
	for range n {
		var addr common.Address
		rng.Read(addr[:])
		var values [StemNodeWidth][]byte
		var basicData [HashSize]byte
		binary.BigEndian.PutUint64(basicData[BasicDataNonceOffset:], uint64(rng.Int63()))
		values[BasicDataLeafKey] = basicData[:]
		var codeHash [HashSize]byte
		rng.Read(codeHash[:])
		values[CodeHashLeafKey] = codeHash[:]
		stem := GetBinaryTreeKey(addr, zero[:])
		var err error
		t.root, err = t.root.InsertValuesAtStem(stem, values[:], nil, 0)
		if err != nil {
			panic(err)
		}
	}
	return t
}

// --- Insert benchmarks ---

func BenchmarkBinaryTrieInsert(b *testing.B) {
	for _, size := range []int{1_000, 100_000} {
		b.Run(sizeLabel(size), func(b *testing.B) {
			keys := generateKeys(size, 42)
			vals := generateValues(size, 99)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				var root BinaryNode = Empty{}
				for i := range size {
					root, _ = root.Insert(keys[i], vals[i], nil, 0)
				}
			}
		})
	}
	if !testing.Short() {
		b.Run("1M", func(b *testing.B) {
			keys := generateKeys(1_000_000, 42)
			vals := generateValues(1_000_000, 99)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				var root BinaryNode = Empty{}
				for i := range 1_000_000 {
					root, _ = root.Insert(keys[i], vals[i], nil, 0)
				}
			}
		})
	}
}

// --- Get benchmarks ---

func BenchmarkBinaryTrieGet(b *testing.B) {
	for _, size := range []int{1_000, 100_000} {
		b.Run(sizeLabel(size), func(b *testing.B) {
			root, keys, _ := buildTrie(size)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				for _, k := range keys {
					root.Get(k, nil)
				}
			}
		})
	}
}

// --- Hash benchmarks ---

func BenchmarkBinaryTrieHash(b *testing.B) {
	for _, size := range []int{100, 1_000, 10_000, 100_000} {
		b.Run(sizeLabel(size), func(b *testing.B) {
			keys := generateKeys(size, 42)
			vals := generateValues(size, 99)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				b.StopTimer()
				var root BinaryNode = Empty{}
				for i := range size {
					root, _ = root.Insert(keys[i], vals[i], nil, 0)
				}
				b.StartTimer()
				root.Hash()
			}
		})
	}
}

// --- Incremental hash benchmarks ---

func BenchmarkBinaryTrieHashIncremental(b *testing.B) {
	for _, size := range []int{1_000, 10_000, 100_000} {
		b.Run(sizeLabel(size), func(b *testing.B) {
			root, keys, _ := buildTrie(size)
			root.Hash()
			rng := rand.New(rand.NewSource(77))
			modifyCount := size / 10
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				b.StopTimer()
				for range modifyCount {
					idx := rng.Intn(size)
					var newVal [HashSize]byte
					rng.Read(newVal[:])
					root, _ = root.Insert(keys[idx], newVal[:], nil, 0)
				}
				b.StartTimer()
				root.Hash()
			}
		})
	}
}

// --- Commit benchmarks ---

func BenchmarkBinaryTrieCommit(b *testing.B) {
	for _, size := range []int{100, 1_000, 10_000} {
		b.Run(sizeLabel(size), func(b *testing.B) {
			keys := generateKeys(size, 42)
			vals := generateValues(size, 99)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				b.StopTimer()
				t := &BinaryTrie{
					root:   NewBinaryNode(),
					tracer: trie.NewPrevalueTracer(),
				}
				for i := range size {
					t.root, _ = t.root.Insert(keys[i], vals[i], nil, 0)
				}
				b.StartTimer()
				t.commitBench()
			}
		})
	}
}

// commitBench mirrors Commit but avoids requiring a real trie.Reader.
func (t *BinaryTrie) commitBench() common.Hash {
	rootHash := t.root.Hash()
	t.root.CollectNodes(nil, func(path []byte, node BinaryNode) {
		SerializeNode(node)
	})
	return rootHash
}

// --- Update benchmarks ---

func BenchmarkBinaryTrieUpdate(b *testing.B) {
	for _, size := range []int{1_000, 100_000} {
		b.Run(sizeLabel(size), func(b *testing.B) {
			root, keys, _ := buildTrie(size)
			newVals := generateValues(size, 77)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				r := root
				for i := range size {
					r, _ = r.Insert(keys[i], newVals[i], nil, 0)
				}
			}
		})
	}
}

// --- Delete benchmarks ---

func BenchmarkBinaryTrieDelete(b *testing.B) {
	for _, size := range []int{1_000, 100_000} {
		b.Run(sizeLabel(size), func(b *testing.B) {
			root, keys, _ := buildTrie(size)
			var zeroVal [HashSize]byte
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				r := root
				for _, k := range keys {
					r, _ = r.Insert(k, zeroVal[:], nil, 0)
				}
			}
		})
	}
}

// --- Serialization benchmarks ---

func BenchmarkSerializeStemNode(b *testing.B) {
	for _, fill := range []int{1, 10, 128, 256} {
		b.Run(sizeLabel(fill), func(b *testing.B) {
			stem := make([]byte, StemSize)
			var values [StemNodeWidth][]byte
			rng := rand.New(rand.NewSource(42))
			for i := range fill {
				v := make([]byte, HashSize)
				rng.Read(v)
				values[i] = v
			}
			node := &StemNode{
				Stem:          stem,
				Values:        values[:],
				depth:         0,
				mustRecompute: true,
			}
			node.Hash()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				SerializeNode(node)
			}
		})
	}
}

func BenchmarkDeserializeStemNode(b *testing.B) {
	for _, fill := range []int{1, 10, 128, 256} {
		b.Run(sizeLabel(fill), func(b *testing.B) {
			stem := make([]byte, StemSize)
			var values [StemNodeWidth][]byte
			rng := rand.New(rand.NewSource(42))
			for i := range fill {
				v := make([]byte, HashSize)
				rng.Read(v)
				values[i] = v
			}
			node := &StemNode{
				Stem:          stem,
				Values:        values[:],
				depth:         0,
				mustRecompute: true,
			}
			node.Hash()
			serialized := SerializeNode(node)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				DeserializeNode(serialized, 0)
			}
		})
	}
}

// --- Key encoding benchmarks ---

func BenchmarkGetBinaryTreeKey(b *testing.B) {
	addr := common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678")
	var offset [HashSize]byte
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		GetBinaryTreeKey(addr, offset[:])
	}
}

func BenchmarkGetBinaryTreeKeyStorageSlot(b *testing.B) {
	addr := common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678")
	slot := common.HexToHash("0x00000000000000000000000000000000000000000000000000000000000000FF")
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		GetBinaryTreeKeyStorageSlot(addr, slot[:])
	}
}

func sizeLabel(n int) string {
	switch {
	case n >= 1_000_000:
		return "1M"
	case n >= 100_000:
		return "100K"
	case n >= 10_000:
		return "10K"
	case n >= 1_000:
		return "1K"
	default:
		return fmt.Sprintf("%d", n)
	}
}

// Copyright 2026 The go-ethereum Authors
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

package trie

import (
	"bytes"
	"testing"
)

// gballetHeight computes structural height the way gballet's archiver does:
// a leaf shortNode (shortNode→valueNode) is height 1; an extension is child+1;
// a fullNode is max(children)+1; a bare valueNode is 0.
func gballetHeight(n node) int {
	switch n := n.(type) {
	case nil:
		return 0
	case valueNode:
		return 0
	case *shortNode:
		if _, ok := n.Val.(valueNode); ok {
			return 1 // leaf shortNode
		}
		return gballetHeight(n.Val) + 1 // extension
	case *fullNode:
		m := 0
		for _, c := range n.Children[:16] {
			if c != nil {
				if h := gballetHeight(c); h > m {
					m = h
				}
			}
		}
		return m + 1
	}
	return 0
}

// identifierRootHeight replays the eip8188 identifier's frame/leaf height
// aggregation over the real NodeIterator and returns the height it assigns to
// the trie root. leafBump mirrors the (suspected-buggy) line that sets a leaf's
// contribution to 1; with leafBump=false the leaf valueNode contributes 0.
func identifierRootHeight(t *testing.T, tr *Trie, leafBump bool) int {
	t.Helper()
	it, err := tr.NodeIterator(nil)
	if err != nil {
		t.Fatalf("NodeIterator: %v", err)
	}
	type frame struct {
		path []byte
		maxc int
	}
	var stack []*frame
	rootHeight := 0
	pop := func() {
		p := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		h := p.maxc + 1
		if len(stack) == 0 {
			rootHeight = h
			return
		}
		par := stack[len(stack)-1]
		if h > par.maxc {
			par.maxc = h
		}
	}
	isPrefix := func(p, q []byte) bool { return len(p) <= len(q) && bytes.Equal(p, q[:len(p)]) }
	for it.Next(true) {
		cur := it.Path()
		for len(stack) > 0 && !isPrefix(stack[len(stack)-1].path, cur) {
			pop()
		}
		if it.Leaf() {
			if leafBump && len(stack) > 0 {
				top := stack[len(stack)-1]
				if top.maxc < 1 {
					top.maxc = 1
				}
			}
		} else {
			stack = append(stack, &frame{path: append([]byte{}, cur...)})
		}
	}
	for len(stack) > 0 {
		pop()
	}
	return rootHeight
}

func TestIdentifierHeightOffByOne(t *testing.T) {
	cases := []struct {
		name string
		keys [][]byte // raw keys (non-secure trie uses them as-is)
	}{
		// Root fullNode directly above leaf shortNodes → gballet height 2.
		{"gballet-h2", [][]byte{{0x1a}, {0x2b}, {0x3c}}},
		// Root fullNode → inner fullNode → leaves → gballet height 3.
		{"gballet-h3", [][]byte{{0x11}, {0x12}, {0x20}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := NewEmpty(nil)
			for _, k := range tc.keys {
				tr.MustUpdate(k, []byte("v"))
			}
			want := gballetHeight(tr.root)
			cur := identifierRootHeight(t, tr, true)    // current code (leaf bump)
			fixed := identifierRootHeight(t, tr, false) // proposed fix (no bump)
			t.Logf("%s: gballetHeight=%d  current(--subtree-height matches)=%d  fixed=%d",
				tc.name, want, cur, fixed)
			if cur != want+1 {
				t.Errorf("current height = %d, expected gballet+1 = %d", cur, want+1)
			}
			if fixed != want {
				t.Errorf("fixed height = %d, expected gballet = %d", fixed, want)
			}
		})
	}
}

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

import "sync"

// nodeCache is a thread-safe cache for prefetched node blobs,
// keyed by node hash.
type nodeCache struct {
	entries sync.Map // common.Hash -> []byte
}

func newNodeCache() *nodeCache {
	return &nodeCache{}
}

func (c *nodeCache) get(hash [32]byte) ([]byte, bool) {
	v, ok := c.entries.Load(hash)
	if !ok {
		return nil, false
	}
	return v.([]byte), true
}

func (c *nodeCache) put(hash [32]byte, blob []byte) {
	c.entries.Store(hash, blob)
}

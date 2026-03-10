// Copyright 2024 The go-ethereum Authors
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

	"github.com/VictoriaMetrics/fastcache"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

// nodeCacheKey constructs the unique key of clean cache. The assumption is held
// that zero address does not have any associated storage slots.
func nodeCacheKey(owner common.Hash, path []byte) []byte {
	if owner == (common.Hash{}) {
		return path
	}
	return append(owner.Bytes(), path...)
}

// writeNodes writes the trie nodes into the provided database batch.
// InternalNodes (binary trie type byte == 2) are grouped into pages
// for packed storage; all other nodes are written individually.
// Note this function will also inject all the newly written nodes
// into clean cache.
func writeNodes(db ethdb.KeyValueReader, batch ethdb.Batch, nodes map[common.Hash]map[string]*trienode.Node, clean *fastcache.Cache) (total int) {
	for owner, subset := range nodes {
		// Separate InternalNodes (packed into pages) from others.
		pages := make(map[string]*Page)
		pageLoaded := make(map[string]bool)

		for path, n := range subset {
			pathBytes := []byte(path)
			if !n.IsDeleted() && isInternalNode(n.Blob) {
				pk := string(PathToPageKey(owner, pathBytes))
				if !pageLoaded[pk] {
					pageLoaded[pk] = true
					pages[pk] = readOrCreatePage(db, owner, pathBytes)
				}
				slot := PathToSlot(pathBytes)
				pages[pk].SetNode(slot, n.Blob)
			} else if n.IsDeleted() && shouldDeleteFromPage(db, owner, pathBytes) {
				pk := string(PathToPageKey(owner, pathBytes))
				if !pageLoaded[pk] {
					pageLoaded[pk] = true
					pages[pk] = readOrCreatePage(db, owner, pathBytes)
				}
				slot := PathToSlot(pathBytes)
				pages[pk].DeleteNode(slot)
			} else {
				// Non-internal node: write individually.
				if n.IsDeleted() {
					if owner == (common.Hash{}) {
						rawdb.DeleteAccountTrieNode(batch, pathBytes)
					} else {
						rawdb.DeleteStorageTrieNode(batch, owner, pathBytes)
					}
					if clean != nil {
						clean.Del(nodeCacheKey(owner, pathBytes))
					}
				} else {
					if owner == (common.Hash{}) {
						rawdb.WriteAccountTrieNode(batch, pathBytes, n.Blob)
					} else {
						rawdb.WriteStorageTrieNode(batch, owner, pathBytes, n.Blob)
					}
					if clean != nil {
						clean.Set(nodeCacheKey(owner, pathBytes), n.Blob)
					}
				}
			}
		}

		// Flush accumulated pages.
		for pk, page := range pages {
			if page.IsEmpty() {
				deleteTriePage(batch, owner, pk)
			} else {
				writeTriePage(batch, owner, pk, page.Serialize())
			}
			if clean != nil {
				prefix := pageKeyPrefix(owner, pk)
				populateCleanCacheFromPage(page, owner, prefix, clean)
			}
		}
		total += len(subset)
	}
	return total
}

// shouldDeleteFromPage checks whether a node being deleted was
// previously stored as an InternalNode in a page. If a page exists
// for the node's location, the deletion targets the page slot.
func shouldDeleteFromPage(db ethdb.KeyValueReader, owner common.Hash, path []byte) bool {
	pageKey := PathToPageKey(owner, path)
	var pageBlob []byte
	if owner == (common.Hash{}) {
		pageBlob = rawdb.ReadAccountTriePage(db, pageKey[1:])
	} else {
		pageBlob = rawdb.ReadStorageTriePage(
			db, owner, pageKey[1+common.HashLength:],
		)
	}
	return pageBlob != nil
}

// writeStates flushes state mutations into the provided database batch as a whole.
//
// This function assumes the background generator is already terminated and states
// before the supplied marker has been correctly generated.
//
// TODO(rjl493456442) do we really need this generation marker? The state updates
// after the marker can also be written and will be fixed by generator later if
// it's outdated.
func writeStates(batch ethdb.Batch, genMarker []byte, accountData map[common.Hash][]byte, storageData map[common.Hash]map[common.Hash][]byte, clean *fastcache.Cache) (int, int) {
	var (
		accounts int
		slots    int
	)
	for addrHash, blob := range accountData {
		// Skip any account not yet covered by the snapshot. The account
		// at the generation marker position (addrHash == genMarker[:common.HashLength])
		// should still be updated, as it would be skipped in the next
		// generation cycle.
		if genMarker != nil && bytes.Compare(addrHash[:], genMarker) > 0 {
			continue
		}
		accounts += 1
		if len(blob) == 0 {
			rawdb.DeleteAccountSnapshot(batch, addrHash)
			if clean != nil {
				clean.Set(addrHash[:], nil)
			}
		} else {
			rawdb.WriteAccountSnapshot(batch, addrHash, blob)
			if clean != nil {
				clean.Set(addrHash[:], blob)
			}
		}
	}
	for addrHash, storages := range storageData {
		// Skip any account not covered yet by the snapshot
		if genMarker != nil && bytes.Compare(addrHash[:], genMarker) > 0 {
			continue
		}
		midAccount := genMarker != nil && bytes.Equal(addrHash[:], genMarker[:common.HashLength])

		for storageHash, blob := range storages {
			// Skip any storage slot not yet covered by the snapshot. The storage slot
			// at the generation marker position (addrHash == genMarker[:common.HashLength]
			// and storageHash == genMarker[common.HashLength:]) should still be updated,
			// as it would be skipped in the next generation cycle.
			if midAccount && bytes.Compare(storageHash[:], genMarker[common.HashLength:]) > 0 {
				continue
			}
			slots += 1
			key := storageKeySlice(addrHash, storageHash)
			if len(blob) == 0 {
				rawdb.DeleteStorageSnapshot(batch, addrHash, storageHash)
				if clean != nil {
					clean.Set(key, nil)
				}
			} else {
				rawdb.WriteStorageSnapshot(batch, addrHash, storageHash, blob)
				if clean != nil {
					clean.Set(key, blob)
				}
			}
		}
	}
	return accounts, slots
}

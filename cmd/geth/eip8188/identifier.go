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

package eip8188

// Inactive subtree identification.
//
// The algorithm walks a trie in pre-order (geth's NodeIterator) while
// advancing a parallel snapshot iterator (also in keccak-hash order). At each
// leaf we look up the period from the snapshot, decide active/inactive, and
// propagate the status up the trie via a stack of in-progress internal-node
// frames.
//
// The candidate-deferral trick: when an internal node finishes with all its
// children inactive, we don't yet know if its parent will also be fully
// inactive. We defer emission by storing the inactive child as a candidate
// on its parent's frame. The parent decides at finalization:
//   - parent fully inactive  → discard candidates (parent subsumes them)
//   - parent mixed           → emit candidates (they're maximal subtree roots)
//
// Embedded nodes (size < 32 bytes, no standalone hash) are never emitted —
// they have no separate identity to move to alternative storage.

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
)

// InactiveSubtree describes a maximal inactive subtree root identified by a
// walk. It can be moved to alternative storage as a unit (out of scope here).
type InactiveSubtree struct {
	Trie      string      `json:"trie"`       // "account" or "storage"
	Owner     common.Hash `json:"owner"`      // For storage tries: address hash. Zero for account trie.
	Path      string      `json:"path"`       // Hex-nibble path (lowercase ascii) from the trie root.
	Hash      common.Hash `json:"hash"`       // Hash of the subtree root (always non-zero — embedded skipped).
	LeafCount uint64      `json:"leaf_count"` // Number of leaves under this subtree.
}

// Scope selects which tries the identifier walks.
type Scope uint8

const (
	ScopeBoth    Scope = iota // both account and per-contract storage tries (default)
	ScopeAccount              // only the account trie; storage tries are skipped
	ScopeStorage              // walk account leaves to discover storage roots, but only emit storage subtrees
)

// String returns the canonical CLI form for a Scope value.
func (s Scope) String() string {
	switch s {
	case ScopeBoth:
		return "both"
	case ScopeAccount:
		return "account"
	case ScopeStorage:
		return "storage"
	default:
		return fmt.Sprintf("scope(%d)", uint8(s))
	}
}

// ParseScope converts a CLI string into a Scope value. Empty defaults to
// ScopeBoth. Unknown values return an error.
func ParseScope(s string) (Scope, error) {
	switch s {
	case "", "both":
		return ScopeBoth, nil
	case "account":
		return ScopeAccount, nil
	case "storage":
		return ScopeStorage, nil
	default:
		return 0, fmt.Errorf("eip8188: invalid scope %q (want account|storage|both)", s)
	}
}

// IdentifyConfig parameterizes a single identification run.
type IdentifyConfig struct {
	// CurrentPeriod is the period at the head block.
	CurrentPeriod uint32

	// InactiveMinAge is the threshold: a leaf is inactive iff
	// CurrentPeriod - leafPeriod >= InactiveMinAge.
	InactiveMinAge uint32

	// Scope selects which tries to walk.
	Scope Scope

	// SubtreeHeight selects the granularity of emitted subtrees.
	//   0 → maximal inactive subtrees (largest fully-inactive subtree; default).
	//   N → emit only subtrees of height exactly N (from leaves: a leaf node is
	//       height 1) whose leaves are all inactive. Mirrors gballet's height-N
	//       archival, gated by inactivity. Taller fully-cold regions are tiled
	//       into height-N pieces; cold regions shorter than N are left in place.
	SubtreeHeight uint8
}

// IdentifyStats summarises an identification run.
type IdentifyStats struct {
	AccountsScanned      uint64 `json:"accounts_scanned"`
	StorageSlotsScanned  uint64 `json:"storage_slots_scanned"`
	StorageTriesWalked   uint64 `json:"storage_tries_walked"`
	InactiveAccountTrees uint64 `json:"inactive_account_subtrees"`
	InactiveStorageTrees uint64 `json:"inactive_storage_subtrees"`
	SnapshotMismatches   uint64 `json:"snapshot_mismatches"`
	MaxSubtreeLeaves     uint64 `json:"max_subtree_leaves"` // largest emitted subtree (sanity: height-3 ⇒ ≤256)
}

// EmitFunc receives each inactive subtree root as it's identified. Callers
// must not retain references to the InactiveSubtree's path/hash beyond the
// call — the algorithm reuses backing buffers on the next iteration.
type EmitFunc func(InactiveSubtree)

// Identify walks the account trie at stateRoot and emits inactive subtree
// roots via emit. For each contract account encountered with a non-empty
// storage root, the storage trie is also walked when cfg.Scope permits.
//
// The walk is read-only: it never mutates the database.
func Identify(ctx context.Context, tdb *triedb.Database, stateRoot common.Hash, cfg IdentifyConfig, emit EmitFunc) (IdentifyStats, error) {
	if emit == nil {
		return IdentifyStats{}, fmt.Errorf("eip8188: emit callback is required")
	}
	id := &identifier{
		ctx:        ctx,
		tdb:        tdb,
		cfg:        cfg,
		emit:       emit,
		phaseStart: time.Now(),
	}
	id.nextLog = id.phaseStart.Add(progressInterval)
	log.Info("eip8188 identify: starting",
		"state-root", stateRoot,
		"current-period", cfg.CurrentPeriod,
		"inactive-min-age", cfg.InactiveMinAge,
		"scope", cfg.Scope)
	if err := id.walkAccountTrie(stateRoot); err != nil {
		return id.stats, err
	}
	return id.stats, nil
}

// identifier holds per-run state. Not exported — driven via Identify().
type identifier struct {
	ctx   context.Context
	tdb   *triedb.Database
	cfg   IdentifyConfig
	emit  EmitFunc
	stats IdentifyStats

	// Shared heartbeat clock — runCoreLoop is called once for the account
	// trie and once per discovered storage trie; all share this timer so
	// we get one log line every ~progressInterval regardless of which
	// inner walk triggered it.
	phaseStart time.Time
	nextLog    time.Time
}

// isInactive returns true iff a leaf with the given period is inactive given
// the configured current period and threshold.
func (id *identifier) isInactive(leafPeriod uint32) bool {
	if id.cfg.CurrentPeriod < leafPeriod {
		return false // future period — treat as active (defensive)
	}
	return id.cfg.CurrentPeriod-leafPeriod >= id.cfg.InactiveMinAge
}

// nodeFrame is one entry in the bottom-up aggregation stack.
type nodeFrame struct {
	path           []byte // Hex-nibble path to this node from the trie root.
	hash           common.Hash
	allInactive    bool
	leafCount      uint64
	maxChildHeight uint8             // tallest child seen so far; node height = maxChildHeight+1
	candidates     []InactiveSubtree // deferred inactive children waiting for parent decision (maximal mode)
}

// leafLookup returns (period, ok) for a leaf identified by leafKey. It also
// triggers any side effects required for that leaf — for the account walker
// this is where we descend into the storage trie.
//
// For state tries (account and per-contract storage), leafKey is exactly
// 32 bytes — the keccak256 hash of the original address or slot key.
// `Trie.NodeIterator.LeafKey()` reconstructs this from the full hex-nibble
// path (which has 64 nibbles + terminator at every leaf because state-trie
// keys are fixed-length keccak hashes). It matches `snapIt.Hash()` exactly,
// which is what makes the parallel-iteration synchronization sound.
//
// If the algorithm is ever applied to a non-fixed-key trie (e.g. a raw Trie
// with variable-length keys), this assumption breaks and the lookup would
// need a different matching strategy.
type leafLookup func(leafKey []byte) (period uint32, ok bool)

// stateTrieKeyLen is the fixed length of a state-trie leaf key — keccak256
// always produces 32 bytes, and snapshot iterators key on a common.Hash.
const stateTrieKeyLen = common.HashLength

// walkAccountTrie opens the account trie and snapshot iterator and runs the
// core loop. It walks per-contract storage tries inline whenever an account
// leaf has a non-empty storage root.
func (id *identifier) walkAccountTrie(stateRoot common.Hash) error {
	tr, err := trie.NewStateTrie(trie.StateTrieID(stateRoot), id.tdb)
	if err != nil {
		return fmt.Errorf("open account trie: %w", err)
	}
	trieIt, err := tr.NodeIterator(nil)
	if err != nil {
		return fmt.Errorf("create account node iterator: %w", err)
	}
	snapIt, err := id.tdb.AccountIterator(stateRoot, common.Hash{})
	if err != nil {
		return fmt.Errorf("create account snapshot iterator: %w", err)
	}
	defer snapIt.Release()

	advanced := false
	lookup := func(leafKey []byte) (uint32, bool) {
		// Invariant: state-trie leaf keys are 32-byte keccak hashes.
		// Bail loudly if the iterator violates this (e.g. someone wires a
		// non-state-trie iterator into this code path).
		if len(leafKey) != stateTrieKeyLen {
			log.Warn("identify: unexpected leaf key length",
				"got", len(leafKey), "want", stateTrieKeyLen, "key", common.Bytes2Hex(leafKey))
			return 0, false
		}
		target := common.BytesToHash(leafKey)
		for {
			if !advanced {
				if !snapIt.Next() {
					return 0, false
				}
				advanced = true
			}
			cmp := bytes.Compare(snapIt.Hash().Bytes(), target.Bytes())
			if cmp < 0 {
				advanced = false
				continue
			}
			if cmp > 0 {
				// trie has a leaf the snapshot doesn't — inconsistency
				return 0, false
			}
			// Match. Decode SlimAccount directly so we get LastWrittenPeriod
			// (FullAccount discards it). Same blob also gives us the storage
			// root for the inline storage walk.
			var slim types.SlimAccount
			if err := rlp.DecodeBytes(snapIt.Account(), &slim); err != nil {
				log.Warn("identify: failed to decode snapshot account", "err", err)
				return 0, false
			}
			advanced = false // consume this entry; advance on next call

			// Walk this account's storage trie if it has one and scope permits.
			if id.cfg.Scope != ScopeAccount && len(slim.Root) != 0 {
				addrHash := common.BytesToHash(leafKey)
				storageRoot := common.BytesToHash(slim.Root)
				if err := id.walkStorageTrie(stateRoot, addrHash, storageRoot); err != nil {
					log.Warn("identify: storage trie walk failed",
						"addr", addrHash, "err", err)
				}
			}
			id.stats.AccountsScanned++
			return slim.LastWrittenPeriod, true
		}
	}

	if id.cfg.Scope == ScopeStorage {
		// Storage-only mode: still need the trie iterator to advance through
		// account leaves to discover storage roots, but we don't emit account
		// subtrees.
		return id.runCoreLoop(trieIt, lookup, "account", common.Hash{}, false)
	}
	return id.runCoreLoop(trieIt, lookup, "account", common.Hash{}, true)
}

// walkStorageTrie walks a single contract's storage trie.
func (id *identifier) walkStorageTrie(stateRoot, owner, storageRoot common.Hash) error {
	id.stats.StorageTriesWalked++

	tr, err := trie.NewStateTrie(trie.StorageTrieID(stateRoot, owner, storageRoot), id.tdb)
	if err != nil {
		return fmt.Errorf("open storage trie: %w", err)
	}
	trieIt, err := tr.NodeIterator(nil)
	if err != nil {
		return fmt.Errorf("create storage node iterator: %w", err)
	}
	snapIt, err := id.tdb.StorageIterator(stateRoot, owner, common.Hash{})
	if err != nil {
		return fmt.Errorf("create storage snapshot iterator: %w", err)
	}
	defer snapIt.Release()

	advanced := false
	lookup := func(leafKey []byte) (uint32, bool) {
		if len(leafKey) != stateTrieKeyLen {
			log.Warn("identify: unexpected storage leaf key length",
				"got", len(leafKey), "want", stateTrieKeyLen,
				"owner", owner, "key", common.Bytes2Hex(leafKey))
			return 0, false
		}
		target := common.BytesToHash(leafKey)
		for {
			if !advanced {
				if !snapIt.Next() {
					return 0, false
				}
				advanced = true
			}
			cmp := bytes.Compare(snapIt.Hash().Bytes(), target.Bytes())
			if cmp < 0 {
				advanced = false
				continue
			}
			if cmp > 0 {
				return 0, false
			}
			_, period, err := types.DecodeStorageSnapshotValue(snapIt.Slot())
			if err != nil {
				log.Warn("identify: failed to decode storage slot", "err", err)
				return 0, false
			}
			advanced = false
			id.stats.StorageSlotsScanned++
			return period, true
		}
	}

	return id.runCoreLoop(trieIt, lookup, "storage", owner, true)
}

// runCoreLoop is the shared stack-based aggregation loop, parameterized over
// the leaf-lookup function and the trie label. When emitOutput is false, the
// loop performs the walk for its side effects (e.g., storage trie discovery)
// without emitting account subtree roots — used in storage-only scope.
func (id *identifier) runCoreLoop(trieIt trie.NodeIterator, lookup leafLookup, trieLabel string, owner common.Hash, emitOutput bool) error {
	stack := make([]*nodeFrame, 0, 64) // typical trie depth << 64

	for trieIt.Next(true) {
		if id.ctx != nil && id.ctx.Err() != nil {
			return id.ctx.Err()
		}
		if now := time.Now(); !now.Before(id.nextLog) {
			elapsed := now.Sub(id.phaseStart)
			rate := float64(id.stats.AccountsScanned+id.stats.StorageSlotsScanned) / elapsed.Seconds()
			log.Info("eip8188 identify: progress",
				"accounts-scanned", id.stats.AccountsScanned,
				"storage-tries", id.stats.StorageTriesWalked,
				"storage-slots", id.stats.StorageSlotsScanned,
				"inactive-account-trees", id.stats.InactiveAccountTrees,
				"inactive-storage-trees", id.stats.InactiveStorageTrees,
				"max-subtree-leaves", id.stats.MaxSubtreeLeaves,
				"snapshot-mismatches", id.stats.SnapshotMismatches,
				"leaves-per-sec", uint64(rate),
				"elapsed", common.PrettyDuration(elapsed),
			)
			id.nextLog = now.Add(progressInterval)
		}
		curPath := trieIt.Path()

		// Backtracking: pop frames whose path is no longer a prefix of curPath.
		for len(stack) > 0 && !isPathPrefix(stack[len(stack)-1].path, curPath) {
			popped := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			id.finalize(popped, stack, trieLabel, owner, emitOutput)
		}

		if trieIt.Leaf() {
			leafKey := trieIt.LeafKey()
			period, ok := lookup(leafKey)
			if !ok {
				id.stats.SnapshotMismatches++
				log.Warn("identify: snapshot mismatch — treating leaf as active",
					"trie", trieLabel, "owner", owner,
					"key", common.Bytes2Hex(leafKey))
				if len(stack) > 0 {
					f := stack[len(stack)-1]
					f.allInactive = false
					f.leafCount++
				}
				continue
			}

			if len(stack) == 0 {
				// Degenerate single-leaf trie. Nothing to emit (no internal
				// node hash).
				continue
			}
			top := stack[len(stack)-1]
			top.leafCount++
			if !id.isInactive(period) {
				top.allInactive = false
			}
			// We do NOT emit individual leaves as subtree roots — leaves
			// have no standalone trie-node identity. Their inactivity just
			// contributes to their parent's allInactive aggregation.
		} else {
			// Internal node — push a frame.
			stack = append(stack, &nodeFrame{
				path:        slices.Clone(curPath),
				hash:        trieIt.Hash(),
				allInactive: true,
			})
		}
	}
	if err := trieIt.Error(); err != nil {
		return err
	}

	// Drain remaining frames.
	for len(stack) > 0 {
		popped := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		id.finalize(popped, stack, trieLabel, owner, emitOutput)
	}
	return nil
}

// finalize processes a popped frame: either propagate its status to the
// parent, or emit candidates if the parent is mixed.
func (id *identifier) finalize(popped *nodeFrame, stack []*nodeFrame, trieLabel string, owner common.Hash, emitOutput bool) {
	// Height-N gated mode: emit a subtree iff it is exactly the target height
	// (from leaves) AND all its leaves are inactive. No maximal roll-up — taller
	// fully-cold regions are tiled into height-N pieces (their height-N
	// descendants were already emitted), and regions shorter than N are skipped.
	if id.cfg.SubtreeHeight != 0 {
		poppedHeight := popped.maxChildHeight + 1
		if emitOutput && poppedHeight == id.cfg.SubtreeHeight && popped.allInactive && popped.hash != (common.Hash{}) {
			id.emitSubtree(InactiveSubtree{
				Trie:      trieLabel,
				Owner:     owner,
				Path:      common.Bytes2Hex(popped.path),
				Hash:      popped.hash,
				LeafCount: popped.leafCount,
			})
		}
		if len(stack) > 0 {
			parent := stack[len(stack)-1]
			parent.leafCount += popped.leafCount
			if poppedHeight > parent.maxChildHeight {
				parent.maxChildHeight = poppedHeight
			}
			if !popped.allInactive {
				parent.allInactive = false
			}
		}
		return
	}

	if len(stack) == 0 {
		// popped is the trie root.
		if !emitOutput {
			return
		}
		if popped.allInactive && popped.hash != (common.Hash{}) {
			// Whole trie is inactive — emit it and discard nested candidates
			// (they're subsumed by the root).
			id.emitSubtree(InactiveSubtree{
				Trie:      trieLabel,
				Owner:     owner,
				Path:      common.Bytes2Hex(popped.path),
				Hash:      popped.hash,
				LeafCount: popped.leafCount,
			})
			return
		}
		// Root is mixed (or unhashable): emit its deferred inactive
		// candidates as maximal subtree roots.
		for _, c := range popped.candidates {
			id.emitSubtree(c)
		}
		return
	}

	parent := stack[len(stack)-1]
	parent.leafCount += popped.leafCount

	if popped.allInactive {
		// Subsume into parent — parent may also be fully inactive. If popped
		// has a standalone hash, it becomes a candidate the parent will
		// either keep (if itself inactive) or emit (if mixed).
		if popped.hash != (common.Hash{}) {
			parent.candidates = append(parent.candidates, InactiveSubtree{
				Trie:      trieLabel,
				Owner:     owner,
				Path:      common.Bytes2Hex(popped.path),
				Hash:      popped.hash,
				LeafCount: popped.leafCount,
			})
			// popped is itself emittable; its deferred inner candidates are
			// PROPER SUBSETS of popped's subtree. If we inherited them, the
			// parent would later emit popped AND all the inner ones — the
			// double-counting bug. Inner candidates are subsumed by popped.
			//
			// On mainnet scale that overcounting led to the converter
			// processing a parent first (deleting its interior) then trying
			// to convert each child (now reading deleted nodes), producing
			// millions of "Unexpected trie node" pathdb errors and
			// eventually crashing pebble under the log volume.
			popped.candidates = nil
		} else {
			// popped is embedded (no standalone hash); it can't be emitted
			// as a subtree root itself, so bubble its inner candidates up.
			parent.candidates = append(parent.candidates, popped.candidates...)
		}
		// allInactive unchanged — parent stays "all inactive so far" if it was.
	} else {
		// popped has at least one active leaf → parent is mixed.
		parent.allInactive = false
		// popped's deferred candidates are now maximal inactive subtree roots
		// (their parent — popped — is mixed).
		if emitOutput {
			for _, c := range popped.candidates {
				id.emitSubtree(c)
			}
		}
		popped.candidates = nil
	}
}

// emitSubtree calls the user emit callback and updates stats.
func (id *identifier) emitSubtree(s InactiveSubtree) {
	id.emit(s)
	if s.LeafCount > id.stats.MaxSubtreeLeaves {
		id.stats.MaxSubtreeLeaves = s.LeafCount
	}
	if s.Trie == "account" {
		id.stats.InactiveAccountTrees++
	} else {
		id.stats.InactiveStorageTrees++
	}
}

// isPathPrefix reports whether prefix is a prefix of path. Used to detect when
// the iterator has backtracked out of a frame's subtree.
func isPathPrefix(prefix, path []byte) bool {
	if len(prefix) > len(path) {
		return false
	}
	return bytes.Equal(prefix, path[:len(prefix)])
}

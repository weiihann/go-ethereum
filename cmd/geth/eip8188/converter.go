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

// EIP-8188 inactive-subtree converter. For each subtree emitted by Identify,
// the converter materialises it from the live chaindb, encodes it as a
// frozen-trie blob, appends the blob to the inactive file, and rewrites the
// chaindb so the subtree's root key holds a 17-byte stub pointing to the
// blob (with the original interior trie nodes deleted).
//
// Crash safety:
//   1. inactive.File.Append() fsyncs the blob bytes BEFORE returning the
//      offset. Any stub later written to chaindb references durable bytes.
//   2. Pebble batches are atomic. A crash mid-conversion either rolls back
//      the entire batch or commits it whole; the chaindb never references
//      a partially-deleted subtree.
//   3. An orphaned blob may remain in inactive.bin after an aborted run.
//      That's fine — it's just unreferenced bytes; no readers will hit it.

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/database"
	"github.com/ethereum/go-ethereum/triedb/inactive"
)

// DefaultConvertBatchSize is the default size in bytes at which the
// chaindb pebble batch is flushed during conversion.
const DefaultConvertBatchSize = 4 * 1024 * 1024 // 4 MiB

// ConvertConfig parameterises a single Convert run.
type ConvertConfig struct {
	// IdentifyConfig drives the identifier that emits inactive subtree roots.
	IdentifyConfig IdentifyConfig

	// StateRoot is the root of the state at which to walk the tries. Usually
	// the chaindb head's state root (caller derives via rawdb.ReadHeadBlock).
	StateRoot common.Hash

	// InactiveFile is the file that receives the appended frozen-trie blobs.
	// Must be opened by the caller; closed by the caller after Convert returns.
	InactiveFile *inactive.File

	// BatchSize bounds the chaindb pebble batch size in bytes. Zero falls
	// back to DefaultConvertBatchSize.
	BatchSize int

	// DryRun, when true, runs identification + materialisation + encoding
	// without appending to the inactive file or mutating chaindb. Useful
	// for sizing experiments.
	DryRun bool
}

// ConvertStats summarises the outcome of a Convert run.
type ConvertStats struct {
	IdentifyStats     IdentifyStats
	SubtreesConverted uint64 // successfully converted (or counted, in dry run)
	AccountSubtrees   uint64
	StorageSubtrees   uint64
	NodesDeleted      uint64 // interior trie node keys removed from chaindb
	BytesAppended     uint64 // total bytes written to inactive.bin
	ConversionErrors  uint64 // subtrees skipped due to per-subtree errors

	// StaleStubsDeleted counts chaindb entries removed by the pre-run sweep
	// because their values were stale stubs/hybrids from earlier conversions.
	StaleStubsDeleted uint64
}

// Convert runs the full pipeline: identify → materialise → encode → append →
// write stubs → delete originals. The state root and trie database must be
// open. The chainDB is the underlying ethdb.Database (for staging the pebble
// batch with stubs and deletes).
//
// Convert returns a non-nil error only on fatal pipeline failures (identify
// errors, batch write failures). Per-subtree errors are logged and counted
// in ConversionErrors but do not stop the run — partial conversions are
// safe because each subtree's stub+deletes are committed atomically.
func Convert(ctx context.Context, chainDB ethdb.Database, tdb *triedb.Database, cfg ConvertConfig) (ConvertStats, error) {
	var stats ConvertStats
	if cfg.InactiveFile == nil && !cfg.DryRun {
		return stats, fmt.Errorf("eip8188 convert: InactiveFile is required (unless DryRun)")
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = DefaultConvertBatchSize
	}

	// Clean-slate prep: a fresh conversion starts with an empty inactive
	// file and a chaindb whose trie-node keyspace contains no stale stub
	// or hybrid values. Without this, two consecutive conversions would
	// leak orphan stubs (no referenced blob) and orphan hybrid entries
	// (referencing stale offsets) — both of which would decode-fail at
	// read time.
	if !cfg.DryRun {
		if err := prepareCleanSlate(ctx, chainDB, cfg.InactiveFile, &stats); err != nil {
			return stats, fmt.Errorf("eip8188 convert: clean-slate prep: %w", err)
		}
	}

	reader, err := tdb.NodeReader(cfg.StateRoot)
	if err != nil {
		return stats, fmt.Errorf("eip8188 convert: NodeReader: %w", err)
	}

	batch := chainDB.NewBatch()
	defer batch.Reset()

	log.Info("eip8188 convert: identify+convert phase starting",
		"state-root", cfg.StateRoot,
		"current-period", cfg.IdentifyConfig.CurrentPeriod,
		"inactive-min-age", cfg.IdentifyConfig.InactiveMinAge,
		"scope", cfg.IdentifyConfig.Scope,
		"dry-run", cfg.DryRun)
	phaseStart := time.Now()
	nextLog := phaseStart.Add(progressInterval)

	emit := func(s InactiveSubtree) {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := convertOne(reader, cfg.InactiveFile, batch, s, &stats, cfg.DryRun); err != nil {
			log.Warn("eip8188 convert: subtree failed",
				"trie", s.Trie, "owner", s.Owner, "path", s.Path, "err", err)
			stats.ConversionErrors++
			return
		}
		stats.SubtreesConverted++
		if s.Trie == "account" {
			stats.AccountSubtrees++
		} else {
			stats.StorageSubtrees++
		}
		// Periodic batch flush. Pebble batches buffer in memory; flushing
		// keeps memory bounded and persists progress incrementally.
		if !cfg.DryRun && batch.ValueSize() >= cfg.BatchSize {
			if err := batch.Write(); err != nil {
				log.Crit("eip8188 convert: batch flush failed", "err", err)
			}
			batch.Reset()
		}
		// Heartbeat. Identifier-side counters live on stats.IdentifyStats and
		// are mutated synchronously inside Identify(); reading them here is
		// safe because emit and Identify share the same goroutine.
		if now := time.Now(); !now.Before(nextLog) {
			elapsed := now.Sub(phaseStart)
			rate := float64(stats.SubtreesConverted) / elapsed.Seconds()
			log.Info("eip8188 convert: progress",
				"accounts-scanned", stats.IdentifyStats.AccountsScanned,
				"storage-tries", stats.IdentifyStats.StorageTriesWalked,
				"storage-slots", stats.IdentifyStats.StorageSlotsScanned,
				"subtrees-converted", stats.SubtreesConverted,
				"account-subtrees", stats.AccountSubtrees,
				"storage-subtrees", stats.StorageSubtrees,
				"nodes-deleted", stats.NodesDeleted,
				"bytes-appended", stats.BytesAppended,
				"errors", stats.ConversionErrors,
				"snapshot-mismatches", stats.IdentifyStats.SnapshotMismatches,
				"subtrees-per-sec", uint64(rate),
				"elapsed", common.PrettyDuration(elapsed),
			)
			nextLog = now.Add(progressInterval)
		}
	}

	idStats, err := Identify(ctx, tdb, cfg.StateRoot, cfg.IdentifyConfig, emit)
	stats.IdentifyStats = idStats
	if err != nil {
		return stats, fmt.Errorf("eip8188 convert: identify: %w", err)
	}

	// Final batch flush.
	if !cfg.DryRun {
		if err := batch.Write(); err != nil {
			return stats, fmt.Errorf("eip8188 convert: final batch write: %w", err)
		}
	}
	return stats, nil
}

// prepareCleanSlate truncates the inactive file and sweeps the chaindb's
// trie-node keyspace, deleting every value whose first byte marks it as a
// primary stub (0x00) or hybrid node (0x01). Run once at the start of every
// non-dry conversion so the run begins from a known-clean state.
//
// The sweep covers BOTH prefixes: "A" (account trie) and "O" (storage
// trie). Iteration is bounded by the prefix scope, so this is an O(N) walk
// over only trie-node keys — not the entire chaindb.
func prepareCleanSlate(ctx context.Context, chainDB ethdb.Database, file *inactive.File, stats *ConvertStats) error {
	if file != nil {
		if err := file.Truncate(); err != nil {
			return fmt.Errorf("truncate inactive file: %w", err)
		}
	}
	prefixes := [][]byte{rawdb.TrieNodeAccountPrefix, rawdb.TrieNodeStoragePrefix}
	for _, prefix := range prefixes {
		removed, err := sweepStubs(ctx, chainDB, prefix)
		if err != nil {
			return fmt.Errorf("sweep prefix %q: %w", prefix, err)
		}
		stats.StaleStubsDeleted += removed
	}
	return nil
}

// sweepStubs iterates every key in chaindb that begins with `prefix` and
// deletes those whose value is a stub (0x00) or hybrid (0x01). Returns the
// number of entries removed.
func sweepStubs(ctx context.Context, chainDB ethdb.Database, prefix []byte) (uint64, error) {
	log.Info("eip8188 convert: clean-slate sweep starting", "prefix", string(prefix))
	batch := chainDB.NewBatch()
	defer batch.Reset()

	var (
		removed    uint64
		scanned    uint64
		phaseStart = time.Now()
		nextLog    = phaseStart.Add(progressInterval)
	)
	it := chainDB.NewIterator(prefix, nil)
	defer it.Release()
	for it.Next() {
		select {
		case <-ctx.Done():
			return removed, ctx.Err()
		default:
		}
		scanned++
		if now := time.Now(); !now.Before(nextLog) {
			elapsed := now.Sub(phaseStart)
			rate := float64(scanned) / elapsed.Seconds()
			log.Info("eip8188 convert: clean-slate sweep progress",
				"prefix", string(prefix),
				"scanned", scanned,
				"removed", removed,
				"keys-per-sec", uint64(rate),
				"elapsed", common.PrettyDuration(elapsed),
			)
			nextLog = now.Add(progressInterval)
		}
		val := it.Value()
		if !inactive.IsStubOrHybrid(val) {
			continue
		}
		// Copy the key — Iterator reuses its internal buffer across Next() calls.
		k := make([]byte, len(it.Key()))
		copy(k, it.Key())
		if err := batch.Delete(k); err != nil {
			return removed, fmt.Errorf("batch delete: %w", err)
		}
		removed++
		if batch.ValueSize() >= DefaultConvertBatchSize {
			if err := batch.Write(); err != nil {
				return removed, fmt.Errorf("batch write: %w", err)
			}
			batch.Reset()
		}
	}
	if err := it.Error(); err != nil {
		return removed, fmt.Errorf("iterator: %w", err)
	}
	if err := batch.Write(); err != nil {
		return removed, fmt.Errorf("final batch write: %w", err)
	}
	log.Info("eip8188 convert: clean-slate sweep finished",
		"prefix", string(prefix), "scanned", scanned, "removed", removed,
		"elapsed", common.PrettyDuration(time.Since(phaseStart)))
	return removed, nil
}

// convertOne handles a single emitted subtree.
func convertOne(reader database.NodeReader, file *inactive.File, batch ethdb.Batch, s InactiveSubtree, stats *ConvertStats, dryRun bool) error {
	rootPath := common.Hex2Bytes(s.Path)

	// 1. Materialise the subtree from the live chaindb.
	root, paths, err := trie.MaterialiseLiveSubtree(reader, s.Owner, rootPath, s.Hash)
	if err != nil {
		return fmt.Errorf("materialise: %w", err)
	}

	// 2. Encode as a frozen-trie blob.
	blob, err := trie.EncodeInactiveBlob(root)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}

	if dryRun {
		stats.BytesAppended += uint64(len(blob))
		stats.NodesDeleted += uint64(len(paths)) // approximate
		return nil
	}

	// 3. Append the blob to the inactive file. fsync happens inside Append.
	offset, err := file.Append(blob)
	if err != nil {
		return fmt.Errorf("append: %w", err)
	}
	stats.BytesAppended += uint64(len(blob))

	// 4. Stage stub + deletes. The stub records the blob's file offset plus
	// the (offset, size) of the blob's root node — pre-resolved here so the
	// trie's decodeStub can produce a normalised *expiredNode without an
	// extra header read at runtime.
	hdr, err := inactive.ParseHeader(blob)
	if err != nil {
		return fmt.Errorf("parse header of just-encoded blob: %w", err)
	}
	stub := trie.EncodeStub(offset, hdr.RootOffset, hdr.RootSize)

	switch s.Trie {
	case "account":
		// Stub at the subtree root path.
		rawdb.WriteAccountTrieNode(batch, rootPath, stub)
		// Delete every interior node key (skip root — we just wrote there).
		for _, p := range paths {
			path := []byte(p)
			if bytes.Equal(path, rootPath) {
				continue
			}
			rawdb.DeleteAccountTrieNode(batch, path)
			stats.NodesDeleted++
		}
	case "storage":
		rawdb.WriteStorageTrieNode(batch, s.Owner, rootPath, stub)
		for _, p := range paths {
			path := []byte(p)
			if bytes.Equal(path, rootPath) {
				continue
			}
			rawdb.DeleteStorageTrieNode(batch, s.Owner, path)
			stats.NodesDeleted++
		}
	default:
		return fmt.Errorf("unknown trie label %q", s.Trie)
	}

	return nil
}

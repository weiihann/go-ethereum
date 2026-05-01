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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/cmd/geth/eip8188"
	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/triedb/inactive"
	"github.com/urfave/cli/v2"
)

var (
	eip8188ForkBlockFlag = &cli.Uint64Flag{
		Name:     "fork-block",
		Usage:    "Block number at which EIP-8188 period tracking begins",
		Required: true,
	}
	eip8188PeriodLengthFlag = &cli.Uint64Flag{
		Name:  "blocks-per-period",
		Usage: "Number of blocks in each EIP-8188 period (~6 months at 12s slot time)",
		Value: eip8188.DefaultBlocksPerPeriod,
	}
	eip8188SourceFlag = &cli.StringFlag{
		Name:  "source",
		Usage: "Source URI: 'clickhouse' (default) or 'file://path/to/fixtures.jsonl'",
		Value: "clickhouse",
	}
	eip8188ClickHouseHostFlag = &cli.StringFlag{
		Name:  "clickhouse-host",
		Usage: "ClickHouse host",
		Value: eip8188.DefaultClickHouseHost,
	}
	eip8188ClickHousePortFlag = &cli.IntFlag{
		Name:  "clickhouse-port",
		Usage: "ClickHouse HTTP port",
		Value: eip8188.DefaultClickHousePort,
	}
	eip8188ClickHouseUserFlag = &cli.StringFlag{
		Name:  "clickhouse-user",
		Usage: "ClickHouse username",
		Value: eip8188.DefaultClickHouseUser,
	}
	eip8188ClickHousePasswordFlag = &cli.StringFlag{
		Name:  "clickhouse-password",
		Usage: "ClickHouse password",
		Value: eip8188.DefaultClickHousePassword,
	}
	eip8188ClickHouseDatabaseFlag = &cli.StringFlag{
		Name:  "clickhouse-database",
		Usage: "ClickHouse database",
		Value: eip8188.DefaultClickHouseDatabase,
	}
	eip8188ClickHouseQueryBatchFlag = &cli.Uint64Flag{
		Name:  "clickhouse-query-batch",
		Usage: "Number of blocks per ClickHouse ARGMAX query (split full range into chunks)",
		Value: eip8188.DefaultClickHouseQueryBatch,
	}
	eip8188BatchSizeFlag = &cli.IntFlag{
		Name:  "batch-size",
		Usage: "Snapshot records per pebble batch flush",
		Value: eip8188.DefaultBatchSize,
	}
	eip8188DryRunFlag = &cli.BoolFlag{
		Name:  "dry-run",
		Usage: "Read and count diffs without writing to disk",
	}
	eip8188JSONFlag = &cli.BoolFlag{
		Name:  "json",
		Usage: "Emit the period report as JSON (default: pretty text)",
	}

	dbInjectPeriodsCmd = &cli.Command{
		Action:    dbInjectPeriods,
		Name:      "inject-periods",
		Usage:     "Backfill EIP-8188 last-written-period metadata into the snapshot layer (prototype)",
		ArgsUsage: "",
		Flags: slices.Concat([]cli.Flag{
			eip8188ForkBlockFlag,
			eip8188PeriodLengthFlag,
			eip8188SourceFlag,
			eip8188ClickHouseHostFlag,
			eip8188ClickHousePortFlag,
			eip8188ClickHouseUserFlag,
			eip8188ClickHousePasswordFlag,
			eip8188ClickHouseDatabaseFlag,
			eip8188ClickHouseQueryBatchFlag,
			eip8188BatchSizeFlag,
			eip8188DryRunFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Description: `Reads post-fork account and storage diffs from a Source and rewrites the
corresponding account / storage snapshot records with their most-recent
EIP-8188 period. The state root is preserved — only the flat snapshot
keyspace is touched. Do not regenerate the snapshot after injection or the
periods will be lost (prototype limitation).

The node must be stopped. The default source is the xatu-cbt ClickHouse at
the user's tailnet; use --source=file:///path/to/fixtures.jsonl to inject
from a JSONL fixture (one diff per line, fields: kind/block/address/slot).`,
	}

	dbInspectPeriodsCmd = &cli.Command{
		Action: dbInspectPeriods,
		Name:   "inspect-periods",
		Usage:  "Report EIP-8188 period statistics across the snapshot",
		Flags: slices.Concat([]cli.Flag{eip8188JSONFlag},
			utils.NetworkFlags, utils.DatabaseFlags),
		Description: `Iterates the account and storage snapshot keyspaces, decoding each record
and tallying how many carry a non-zero EIP-8188 last_written_period.
Output is pretty text by default; pass --json for machine-readable output.`,
	}

	eip8188InactiveMinAgeFlag = &cli.Uint64Flag{
		Name:  "inactive-min-age",
		Usage: "Minimum age (current_period - leaf_period) for a leaf to be considered inactive",
		Value: 1,
	}
	eip8188CurrentPeriodFlag = &cli.Uint64Flag{
		Name:  "current-period",
		Usage: "Override the current period (default: derived from chaindb head and --fork-block)",
	}
	eip8188ScopeFlag = &cli.StringFlag{
		Name:  "scope",
		Usage: "Which tries to walk: account, storage, or both",
		Value: "both",
	}
	eip8188OutputFlag = &cli.StringFlag{
		Name:  "output",
		Usage: "Path to JSONL output file (default: stdout)",
	}

	dbIdentifyInactiveCmd = &cli.Command{
		Action: dbIdentifyInactive,
		Name:   "identify-inactive",
		Usage:  "Identify maximal inactive trie subtree roots based on EIP-8188 periods",
		Flags: slices.Concat([]cli.Flag{
			eip8188ForkBlockFlag,
			eip8188PeriodLengthFlag,
			eip8188InactiveMinAgeFlag,
			eip8188CurrentPeriodFlag,
			eip8188ScopeFlag,
			eip8188OutputFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Description: `Walks the account trie (and per-contract storage tries) at the chaindb head
state root, looking up each leaf's period from the snapshot layer and
emitting maximal inactive subtree roots as JSON-lines records. A subtree is
"inactive" iff every leaf under it has period age >= --inactive-min-age.

The output stream is one JSON object per line:
  {"trie":"account","owner":"0x..","path":"abcd","hash":"0x..","leaf_count":42}

Run after 'inject-periods' (or after a node sync that records periods at
commit time). The triedb diff layers are flushed to disk at startup so the
walk reads a consistent on-disk view.`,
	}

	eip8188InactiveFileFlag = &cli.StringFlag{
		Name:  "inactive-file",
		Usage: "Path to the inactive trie file (default: <chaindata>/inactive.bin)",
	}
	eip8188ConvertBatchSizeFlag = &cli.IntFlag{
		Name:  "convert-batch-size",
		Usage: "Chaindb pebble batch size (bytes) for stub writes and original-node deletes",
		Value: eip8188.DefaultConvertBatchSize,
	}
	eip8188ConvertDryRunFlag = &cli.BoolFlag{
		Name:  "dry-run",
		Usage: "Identify and count inactive subtrees without mutating the chaindb or inactive file",
	}
	eip8188SkipCleanSlateFlag = &cli.BoolFlag{
		Name:  "skip-clean-slate",
		Usage: "Skip the pre-run sweep that removes existing stubs/hybrids from chaindb. Use only when chaindata is known clean (e.g. fresh post-inject snapshot).",
	}

	dbCountTrieNodeKindsCmd = &cli.Command{
		Action: dbCountTrieNodeKinds,
		Name:   "count-trienode-kinds",
		Usage:  "Count chaindb trie-node entries by kind (RLP / EIP-8188 stub / EIP-8188 hybrid)",
		Flags:  slices.Concat([]cli.Flag{eip8188JSONFlag}, utils.NetworkFlags, utils.DatabaseFlags),
		Description: `Iterates the chaindb's trie-node keyspace ('A'-prefix and 'O'-prefix entries)
and classifies each value by its first byte:
  - 0x00:    EIP-8188 primary stub (full subtree replacement)
  - 0x01:    EIP-8188 hybrid node (partially-materialised parent)
  - 0xc0+:   standard MPT RLP node
  - other:   anomaly (counted under 'other')

Useful for verifying the on-disk effect of EIP-8188 conversion and lazy
materialisation: a fresh conversion produces only stubs (no hybrids); a
post-modification chaindb produces hybrids along the modified path.`,
	}

	dbConvertInactiveCmd = &cli.Command{
		Action: dbConvertInactive,
		Name:   "convert-inactive",
		Usage:  "Move inactive trie subtrees out of chaindb into a separate file",
		Flags: slices.Concat([]cli.Flag{
			eip8188ForkBlockFlag,
			eip8188PeriodLengthFlag,
			eip8188InactiveMinAgeFlag,
			eip8188CurrentPeriodFlag,
			eip8188ScopeFlag,
			eip8188InactiveFileFlag,
			eip8188ConvertBatchSizeFlag,
			eip8188ConvertDryRunFlag,
			eip8188SkipCleanSlateFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Description: `Walks the account trie (and per-contract storage tries) at the chaindb head,
identifies maximal inactive subtree roots (same algorithm as identify-inactive),
serialises each as a frozen-trie blob into the inactive file, writes a 17-byte
stub at the original chaindb key, and deletes the interior trie nodes that
made up the original subtree.

The state root is preserved — readers transparently follow stubs into the
inactive file via an archive resolver attached at pathdb open time. The
node must be stopped during this command.

Subsequent 'geth' invocations auto-attach the inactive file when present at
<chaindata>/inactive.bin (or whatever --inactive-file points at).`,
	}
)

func dbInjectPeriods(ctx *cli.Context) error {
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, false)
	defer db.Close()

	head := rawdb.ReadHeadHeader(db)
	if head == nil {
		return fmt.Errorf("chaindb has no head header; is this a fresh datadir?")
	}
	endBlock := head.Number.Uint64()
	forkBlock := ctx.Uint64(eip8188ForkBlockFlag.Name)
	if forkBlock > endBlock {
		return fmt.Errorf("fork block %d is ahead of chaindb head %d", forkBlock, endBlock)
	}

	// Flush pathdb diff layers (loaded from the on-shutdown journal) into the
	// disk snapshot keyspace before iterating. The injector reads raw "a"+/"o"+
	// keys; if recent state is still buffered in diff layers, those accounts
	// would be invisible and the inject would silently skip them. This open +
	// Commit pair forces a merge.
	headBlock := rawdb.ReadHeadBlock(db)
	if headBlock != nil {
		if err := flushDiffLayers(ctx, stack, db, headBlock.Root()); err != nil {
			log.Warn("inject-periods: failed to flush diff layers", "err", err)
		}
	}

	source, err := openSource(ctx)
	if err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer source.Close()

	log.Info("EIP-8188 injector starting",
		"fork-block", forkBlock,
		"end-block", endBlock,
		"blocks-per-period", ctx.Uint64(eip8188PeriodLengthFlag.Name),
		"dry-run", ctx.Bool(eip8188DryRunFlag.Name),
	)

	stats, err := eip8188.Inject(ctx.Context, db, eip8188.Config{
		Source:          source,
		ForkBlock:       forkBlock,
		BlocksPerPeriod: ctx.Uint64(eip8188PeriodLengthFlag.Name),
		EndBlock:        endBlock,
		BatchSize:       ctx.Int(eip8188BatchSizeFlag.Name),
		DryRun:          ctx.Bool(eip8188DryRunFlag.Name),
	})
	log.Info("EIP-8188 injector finished",
		"account-diffs-seen", stats.AccountDiffsSeen,
		"account-snapshots-updated", stats.AccountSnapshotsUpdated,
		"account-snapshots-missing", stats.AccountSnapshotsMissing,
		"storage-diffs-seen", stats.StorageDiffsSeen,
		"storage-snapshots-updated", stats.StorageSnapshotsUpdated,
		"storage-snapshots-missing", stats.StorageSnapshotsMissing,
	)
	return err
}

func dbInspectPeriods(ctx *cli.Context) error {
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, true)
	defer db.Close()

	report, err := eip8188.Inspect(ctx.Context, db)
	if err != nil {
		return err
	}
	if ctx.Bool(eip8188JSONFlag.Name) {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	fmt.Printf("EIP-8188 period report:\n")
	fmt.Printf("  accounts:       total=%d  with_period=%d  max_period=%d  decode_errors=%d  key_bytes=%d  value_bytes=%d\n",
		report.TotalAccounts, report.AccountsWithPeriod, report.MaxAccountPeriod,
		report.AccountDecodeErrors, report.AccountSnapshotKeyBytes, report.AccountSnapshotValueBytes)
	fmt.Printf("  storage slots:  total=%d  with_period=%d  max_period=%d  decode_errors=%d  key_bytes=%d  value_bytes=%d\n",
		report.TotalStorageSlots, report.StorageWithPeriod, report.MaxStoragePeriod,
		report.StorageDecodeErrors, report.StorageSnapshotKeyBytes, report.StorageSnapshotValueBytes)
	return nil
}

func dbIdentifyInactive(ctx *cli.Context) error {
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	chaindb := utils.MakeChainDatabase(ctx, stack, false)
	defer chaindb.Close()

	head := rawdb.ReadHeadBlock(chaindb)
	if head == nil {
		return fmt.Errorf("chaindb has no head block; is this a fresh datadir?")
	}
	stateRoot := head.Root()
	headBlock := head.NumberU64()

	// If inject-periods was run earlier, it already flushed the diff layers
	// to disk. Re-flush defensively in case it wasn't (e.g. periods were
	// stamped at commit time, or the user is running identify alone).
	if err := flushDiffLayers(ctx, stack, chaindb, stateRoot); err != nil {
		log.Warn("failed to flush diff layers; results may be incomplete", "err", err)
	}

	// Re-open the triedb for the actual walk. After flushing above, the
	// disk view and triedb merged view should be identical.
	tdb := utils.MakeTrieDatabase(ctx, stack, chaindb, false, false, false)
	defer tdb.Close()

	// Resolve current period.
	currentPeriod := uint32(0)
	if ctx.IsSet(eip8188CurrentPeriodFlag.Name) {
		v := ctx.Uint64(eip8188CurrentPeriodFlag.Name)
		currentPeriod = clampPeriod(v)
	} else {
		forkBlock := ctx.Uint64(eip8188ForkBlockFlag.Name)
		if headBlock < forkBlock {
			return fmt.Errorf("head block %d is before fork block %d", headBlock, forkBlock)
		}
		currentPeriod = eip8188.ComputePeriod(headBlock, forkBlock,
			ctx.Uint64(eip8188PeriodLengthFlag.Name))
	}

	threshold := clampPeriod(ctx.Uint64(eip8188InactiveMinAgeFlag.Name))
	scope, err := eip8188.ParseScope(ctx.String(eip8188ScopeFlag.Name))
	if err != nil {
		return err
	}

	// Output sink.
	out := os.Stdout
	if path := ctx.String(eip8188OutputFlag.Name); path != "" {
		f, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("create output: %w", err)
		}
		defer f.Close()
		out = f
	}
	enc := json.NewEncoder(out)

	log.Info("EIP-8188 identify-inactive starting",
		"state-root", stateRoot,
		"head-block", headBlock,
		"current-period", currentPeriod,
		"inactive-min-age", threshold,
		"scope", scope,
	)

	emit := func(s eip8188.InactiveSubtree) {
		if err := enc.Encode(s); err != nil {
			log.Warn("encode subtree", "err", err)
		}
	}

	stats, err := eip8188.Identify(ctx.Context, tdb, stateRoot, eip8188.IdentifyConfig{
		CurrentPeriod:  currentPeriod,
		InactiveMinAge: threshold,
		Scope:          scope,
	}, emit)
	log.Info("EIP-8188 identify-inactive finished",
		"accounts-scanned", stats.AccountsScanned,
		"storage-tries-walked", stats.StorageTriesWalked,
		"storage-slots-scanned", stats.StorageSlotsScanned,
		"inactive-account-subtrees", stats.InactiveAccountTrees,
		"inactive-storage-subtrees", stats.InactiveStorageTrees,
		"snapshot-mismatches", stats.SnapshotMismatches,
	)
	return err
}

// dbConvertInactive runs the offline inactive-subtree conversion: identify +
// serialise + append + write stubs + delete originals. After it completes,
// subsequent geth invocations transparently follow stubs into the inactive
// file via the auto-attached archive resolver.
func dbConvertInactive(ctx *cli.Context) error {
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	chaindb := utils.MakeChainDatabase(ctx, stack, false)
	defer chaindb.Close()

	head := rawdb.ReadHeadBlock(chaindb)
	if head == nil {
		return fmt.Errorf("chaindb has no head block; is this a fresh datadir?")
	}
	stateRoot := head.Root()
	headBlock := head.NumberU64()

	// Flush any pathdb diff layers loaded from the on-shutdown journal so
	// the converter sees a fully-merged disk view of the trie.
	if err := flushDiffLayers(ctx, stack, chaindb, stateRoot); err != nil {
		log.Warn("convert-inactive: failed to flush diff layers", "err", err)
	}

	tdb := utils.MakeTrieDatabase(ctx, stack, chaindb, false, false, false)
	defer tdb.Close()

	// Resolve current period.
	currentPeriod := uint32(0)
	if ctx.IsSet(eip8188CurrentPeriodFlag.Name) {
		currentPeriod = clampPeriod(ctx.Uint64(eip8188CurrentPeriodFlag.Name))
	} else {
		forkBlock := ctx.Uint64(eip8188ForkBlockFlag.Name)
		if headBlock < forkBlock {
			return fmt.Errorf("head block %d is before fork block %d", headBlock, forkBlock)
		}
		currentPeriod = eip8188.ComputePeriod(headBlock, forkBlock,
			ctx.Uint64(eip8188PeriodLengthFlag.Name))
	}
	threshold := clampPeriod(ctx.Uint64(eip8188InactiveMinAgeFlag.Name))
	scope, err := eip8188.ParseScope(ctx.String(eip8188ScopeFlag.Name))
	if err != nil {
		return err
	}
	dryRun := ctx.Bool(eip8188ConvertDryRunFlag.Name)

	// Resolve inactive file path. Default to <chaindata>/inactive.bin.
	inactivePath := ctx.String(eip8188InactiveFileFlag.Name)
	if inactivePath == "" {
		inactivePath = filepath.Join(stack.ResolvePath("chaindata"), "inactive.bin")
	}

	var file *inactive.File
	if !dryRun {
		f, err := inactive.Open(inactivePath, true) // create=true; appends if exists
		if err != nil {
			return fmt.Errorf("open inactive file %q: %w", inactivePath, err)
		}
		file = f
		defer f.Close()
	}

	log.Info("EIP-8188 convert-inactive starting",
		"state-root", stateRoot,
		"head-block", headBlock,
		"current-period", currentPeriod,
		"inactive-min-age", threshold,
		"scope", scope,
		"inactive-file", inactivePath,
		"dry-run", dryRun,
	)

	stats, err := eip8188.Convert(ctx.Context, chaindb, tdb, eip8188.ConvertConfig{
		IdentifyConfig: eip8188.IdentifyConfig{
			CurrentPeriod:  currentPeriod,
			InactiveMinAge: threshold,
			Scope:          scope,
		},
		StateRoot:      stateRoot,
		InactiveFile:   file,
		BatchSize:      ctx.Int(eip8188ConvertBatchSizeFlag.Name),
		DryRun:         dryRun,
		SkipCleanSlate: ctx.Bool(eip8188SkipCleanSlateFlag.Name),
	})
	log.Info("EIP-8188 convert-inactive finished",
		"subtrees-converted", stats.SubtreesConverted,
		"account-subtrees", stats.AccountSubtrees,
		"storage-subtrees", stats.StorageSubtrees,
		"nodes-deleted", stats.NodesDeleted,
		"bytes-appended", stats.BytesAppended,
		"errors", stats.ConversionErrors,
		"snapshot-mismatches", stats.IdentifyStats.SnapshotMismatches,
	)
	return err
}

// dbCountTrieNodeKinds iterates the chaindb's trie-node keyspace and counts
// entries by their first-byte classification. Flushes pathdb diff layers
// first so the count reflects all committed state, including modifications
// still buffered in the journal at last shutdown.
func dbCountTrieNodeKinds(ctx *cli.Context) error {
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, false)
	defer db.Close()

	headBlock := rawdb.ReadHeadBlock(db)
	if headBlock != nil {
		if err := flushDiffLayers(ctx, stack, db, headBlock.Root()); err != nil {
			log.Warn("count-trienode-kinds: failed to flush diff layers", "err", err)
		}
	}

	type counts struct {
		Stubs       uint64 `json:"stubs"`
		Hybrids     uint64 `json:"hybrids"`
		RLP         uint64 `json:"standard_rlp"`
		Other       uint64 `json:"other"`
		Empty       uint64 `json:"empty"`
		TotalKey    uint64 `json:"total_keys"`
		StubBytes   uint64 `json:"stub_bytes"`
		HybridBytes uint64 `json:"hybrid_bytes"`
		RLPBytes    uint64 `json:"standard_rlp_bytes"`
		OtherBytes  uint64 `json:"other_bytes"`
		TotalBytes  uint64 `json:"total_bytes"`
	}
	type report struct {
		AccountTrie counts `json:"account_trie"`
		StorageTrie counts `json:"storage_trie"`
	}
	classify := func(prefix []byte) counts {
		log.Info("count-trienode-kinds: phase starting", "prefix", string(prefix))
		var c counts
		it := db.NewIterator(prefix, nil)
		defer it.Release()
		phaseStart := time.Now()
		nextLog := phaseStart.Add(30 * time.Second)
		for it.Next() {
			c.TotalKey++
			if now := time.Now(); !now.Before(nextLog) {
				elapsed := now.Sub(phaseStart)
				rate := float64(c.TotalKey) / elapsed.Seconds()
				log.Info("count-trienode-kinds: progress",
					"prefix", string(prefix),
					"scanned", c.TotalKey,
					"stubs", c.Stubs,
					"hybrids", c.Hybrids,
					"rlp", c.RLP,
					"other", c.Other,
					"empty", c.Empty,
					"total-bytes", c.TotalBytes,
					"keys-per-sec", uint64(rate),
					"elapsed", common.PrettyDuration(elapsed),
				)
				nextLog = now.Add(30 * time.Second)
			}
			val := it.Value()
			n := uint64(len(val))
			c.TotalBytes += n
			switch {
			case n == 0:
				c.Empty++
			case val[0] == 0x00:
				c.Stubs++
				c.StubBytes += n
			case val[0] == 0x01:
				c.Hybrids++
				c.HybridBytes += n
			case val[0] >= 0xc0:
				c.RLP++
				c.RLPBytes += n
			default:
				c.Other++
				c.OtherBytes += n
			}
		}
		if err := it.Error(); err != nil {
			log.Warn("count-trienode-kinds: iterator error", "prefix", string(prefix), "err", err)
		}
		log.Info("count-trienode-kinds: phase finished",
			"prefix", string(prefix),
			"scanned", c.TotalKey,
			"stubs", c.Stubs,
			"hybrids", c.Hybrids,
			"rlp", c.RLP,
			"other", c.Other,
			"empty", c.Empty,
			"total-bytes", c.TotalBytes,
			"elapsed", common.PrettyDuration(time.Since(phaseStart)))
		return c
	}

	rep := report{
		AccountTrie: classify(rawdb.TrieNodeAccountPrefix),
		StorageTrie: classify(rawdb.TrieNodeStoragePrefix),
	}

	if ctx.Bool(eip8188JSONFlag.Name) {
		out, err := json.Marshal(rep)
		if err != nil {
			return fmt.Errorf("marshal: %w", err)
		}
		fmt.Println(string(out))
		return nil
	}
	fmt.Printf("Account trie counts:  rlp=%d stubs=%d hybrids=%d other=%d empty=%d (total=%d)\n",
		rep.AccountTrie.RLP, rep.AccountTrie.Stubs, rep.AccountTrie.Hybrids,
		rep.AccountTrie.Other, rep.AccountTrie.Empty, rep.AccountTrie.TotalKey)
	fmt.Printf("Account trie bytes:   rlp=%d stubs=%d hybrids=%d other=%d (total_bytes=%d)\n",
		rep.AccountTrie.RLPBytes, rep.AccountTrie.StubBytes, rep.AccountTrie.HybridBytes,
		rep.AccountTrie.OtherBytes, rep.AccountTrie.TotalBytes)
	fmt.Printf("Storage trie counts:  rlp=%d stubs=%d hybrids=%d other=%d empty=%d (total=%d)\n",
		rep.StorageTrie.RLP, rep.StorageTrie.Stubs, rep.StorageTrie.Hybrids,
		rep.StorageTrie.Other, rep.StorageTrie.Empty, rep.StorageTrie.TotalKey)
	fmt.Printf("Storage trie bytes:   rlp=%d stubs=%d hybrids=%d other=%d (total_bytes=%d)\n",
		rep.StorageTrie.RLPBytes, rep.StorageTrie.StubBytes, rep.StorageTrie.HybridBytes,
		rep.StorageTrie.OtherBytes, rep.StorageTrie.TotalBytes)
	return nil
}

// clampPeriod clamps a uint64 period value to uint32 (the EIP-8188 prototype's
// on-disk type). Values that overflow are treated as MaxUint32.
func clampPeriod(v uint64) uint32 {
	if v > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(v)
}

// flushDiffLayers opens the trie database, merges any pathdb diff layers
// loaded from the on-shutdown journal into the disk snapshot keyspace, and
// closes the trie database again. This is required before inject-periods
// (so it sees all snapshot records on disk) and before identify-inactive
// (so the trie iterator and the snapshot iterator see a consistent view).
//
// The triedb is opened with read-write access. On Close, pathdb will write
// any remaining diff layers back to the journal — but after a successful
// Commit there should be no remaining layers, so the journal becomes empty.
func flushDiffLayers(ctx *cli.Context, stack *node.Node, chaindb ethdb.Database, stateRoot common.Hash) error {
	tdb := utils.MakeTrieDatabase(ctx, stack, chaindb, false, false, false)
	defer tdb.Close()
	return tdb.Commit(stateRoot, false)
}

// openSource constructs a Source from the --source flag value. "clickhouse"
// (or empty) uses the CLI's clickhouse-* flags; "file://path" opens a JSONL
// fixture.
func openSource(ctx *cli.Context) (eip8188.Source, error) {
	src := ctx.String(eip8188SourceFlag.Name)
	if src == "" || src == "clickhouse" {
		return eip8188.NewClickHouseSource(ctx.Context, eip8188.ClickHouseConfig{
			Host:       ctx.String(eip8188ClickHouseHostFlag.Name),
			Port:       ctx.Int(eip8188ClickHousePortFlag.Name),
			User:       ctx.String(eip8188ClickHouseUserFlag.Name),
			Password:   ctx.String(eip8188ClickHousePasswordFlag.Name),
			Database:   ctx.String(eip8188ClickHouseDatabaseFlag.Name),
			QueryBatch: ctx.Uint64(eip8188ClickHouseQueryBatchFlag.Name),
		})
	}
	if path, ok := strings.CutPrefix(src, "file://"); ok {
		return eip8188.NewFileSource(path), nil
	}
	return nil, fmt.Errorf("unrecognised --source %q (expected 'clickhouse' or 'file://path')", src)
}

// Satisfy the linter in case context.Context is ever passed explicitly.
var _ = context.Background

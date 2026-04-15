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
	"os"
	"slices"
	"strings"

	"github.com/ethereum/go-ethereum/cmd/geth/eip8188"
	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/log"
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
	fmt.Printf("  accounts:       total=%d  with_period=%d  max_period=%d  decode_errors=%d\n",
		report.TotalAccounts, report.AccountsWithPeriod, report.MaxAccountPeriod, report.AccountDecodeErrors)
	fmt.Printf("  storage slots:  total=%d  with_period=%d  max_period=%d  decode_errors=%d\n",
		report.TotalStorageSlots, report.StorageWithPeriod, report.MaxStoragePeriod, report.StorageDecodeErrors)
	return nil
}

// openSource constructs a Source from the --source flag value. "clickhouse"
// (or empty) uses the CLI's clickhouse-* flags; "file://path" opens a JSONL
// fixture.
func openSource(ctx *cli.Context) (eip8188.Source, error) {
	src := ctx.String(eip8188SourceFlag.Name)
	if src == "" || src == "clickhouse" {
		return eip8188.NewClickHouseSource(ctx.Context, eip8188.ClickHouseConfig{
			Host:     ctx.String(eip8188ClickHouseHostFlag.Name),
			Port:     ctx.Int(eip8188ClickHousePortFlag.Name),
			User:     ctx.String(eip8188ClickHouseUserFlag.Name),
			Password: ctx.String(eip8188ClickHousePasswordFlag.Name),
			Database: ctx.String(eip8188ClickHouseDatabaseFlag.Name),
		})
	}
	if path, ok := strings.CutPrefix(src, "file://"); ok {
		return eip8188.NewFileSource(path), nil
	}
	return nil, fmt.Errorf("unrecognised --source %q (expected 'clickhouse' or 'file://path')", src)
}

// Satisfy the linter in case context.Context is ever passed explicitly.
var _ = context.Background

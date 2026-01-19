// Copyright 2021 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/console/prompt"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/urfave/cli/v2"
)

var (
	removeStateDataFlag = &cli.BoolFlag{
		Name:  "remove.state",
		Usage: "If set, selects the state data for removal",
	}
	removeChainDataFlag = &cli.BoolFlag{
		Name:  "remove.chain",
		Usage: "If set, selects the state data for removal",
	}

	removedbCommand = &cli.Command{
		Action:    removeDB,
		Name:      "removedb",
		Usage:     "Remove blockchain and state databases",
		ArgsUsage: "",
		Flags: slices.Concat(utils.DatabaseFlags,
			[]cli.Flag{removeStateDataFlag, removeChainDataFlag}),
		Description: `
Remove blockchain and state databases`,
	}
	dbCommand = &cli.Command{
		Name:      "db",
		Usage:     "Low level database operations",
		ArgsUsage: "",
		Subcommands: []*cli.Command{
			dbInspectCmd,
			dbStatCmd,
			dbCompactCmd,
			dbGetCmd,
			dbDeleteCmd,
			dbPutCmd,
			dbGetSlotsCmd,
			dbDumpFreezerIndex,
			dbImportCmd,
			dbExportCmd,
			dbMetadataCmd,
			dbCheckStateContentCmd,
			dbInspectHistoryCmd,
			dbAddressStorageCmd,
			dbContractSlotsCmd,
		},
	}
	dbInspectCmd = &cli.Command{
		Action:      inspect,
		Name:        "inspect",
		ArgsUsage:   "<prefix> <start>",
		Flags:       slices.Concat(utils.NetworkFlags, utils.DatabaseFlags),
		Usage:       "Inspect the storage size for each type of data in the database",
		Description: `This commands iterates the entire database. If the optional 'prefix' and 'start' arguments are provided, then the iteration is limited to the given subset of data.`,
	}
	dbCheckStateContentCmd = &cli.Command{
		Action:    checkStateContent,
		Name:      "check-state-content",
		ArgsUsage: "<start (optional)>",
		Flags:     slices.Concat(utils.NetworkFlags, utils.DatabaseFlags),
		Usage:     "Verify that state data is cryptographically correct",
		Description: `This command iterates the entire database for 32-byte keys, looking for rlp-encoded trie nodes.
For each trie node encountered, it checks that the key corresponds to the keccak256(value). If this is not true, this indicates
a data corruption.`,
	}
	dbStatCmd = &cli.Command{
		Action: dbStats,
		Name:   "stats",
		Usage:  "Print leveldb statistics",
		Flags:  slices.Concat(utils.NetworkFlags, utils.DatabaseFlags),
	}
	dbCompactCmd = &cli.Command{
		Action: dbCompact,
		Name:   "compact",
		Usage:  "Compact leveldb database. WARNING: May take a very long time",
		Flags: slices.Concat([]cli.Flag{
			utils.CacheFlag,
			utils.CacheDatabaseFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Description: `This command performs a database compaction.
WARNING: This operation may take a very long time to finish, and may cause database
corruption if it is aborted during execution'!`,
	}
	dbGetCmd = &cli.Command{
		Action:      dbGet,
		Name:        "get",
		Usage:       "Show the value of a database key",
		ArgsUsage:   "<hex-encoded key>",
		Flags:       slices.Concat(utils.NetworkFlags, utils.DatabaseFlags),
		Description: "This command looks up the specified database key from the database.",
	}
	dbDeleteCmd = &cli.Command{
		Action:    dbDelete,
		Name:      "delete",
		Usage:     "Delete a database key (WARNING: may corrupt your database)",
		ArgsUsage: "<hex-encoded key>",
		Flags:     slices.Concat(utils.NetworkFlags, utils.DatabaseFlags),
		Description: `This command deletes the specified database key from the database.
WARNING: This is a low-level operation which may cause database corruption!`,
	}
	dbPutCmd = &cli.Command{
		Action:    dbPut,
		Name:      "put",
		Usage:     "Set the value of a database key (WARNING: may corrupt your database)",
		ArgsUsage: "<hex-encoded key> <hex-encoded value>",
		Flags:     slices.Concat(utils.NetworkFlags, utils.DatabaseFlags),
		Description: `This command sets a given database key to the given value.
WARNING: This is a low-level operation which may cause database corruption!`,
	}
	dbGetSlotsCmd = &cli.Command{
		Action:      dbDumpTrie,
		Name:        "dumptrie",
		Usage:       "Show the storage key/values of a given storage trie",
		ArgsUsage:   "<hex-encoded state root> <hex-encoded account hash> <hex-encoded storage trie root> <hex-encoded start (optional)> <int max elements (optional)>",
		Flags:       slices.Concat(utils.NetworkFlags, utils.DatabaseFlags),
		Description: "This command looks up the specified database key from the database.",
	}
	dbDumpFreezerIndex = &cli.Command{
		Action:      freezerInspect,
		Name:        "freezer-index",
		Usage:       "Dump out the index of a specific freezer table",
		ArgsUsage:   "<freezer-type> <table-type> <start (int)> <end (int)>",
		Flags:       slices.Concat(utils.NetworkFlags, utils.DatabaseFlags),
		Description: "This command displays information about the freezer index.",
	}
	dbImportCmd = &cli.Command{
		Action:      importLDBdata,
		Name:        "import",
		Usage:       "Imports leveldb-data from an exported RLP dump.",
		ArgsUsage:   "<dumpfile> <start (optional)",
		Flags:       slices.Concat(utils.NetworkFlags, utils.DatabaseFlags),
		Description: "The import command imports the specific chain data from an RLP encoded stream.",
	}
	dbExportCmd = &cli.Command{
		Action:      exportChaindata,
		Name:        "export",
		Usage:       "Exports the chain data into an RLP dump. If the <dumpfile> has .gz suffix, gzip compression will be used.",
		ArgsUsage:   "<type> <dumpfile>",
		Flags:       slices.Concat(utils.NetworkFlags, utils.DatabaseFlags),
		Description: "Exports the specified chain data to an RLP encoded stream, optionally gzip-compressed.",
	}
	dbMetadataCmd = &cli.Command{
		Action:      showMetaData,
		Name:        "metadata",
		Usage:       "Shows metadata about the chain status.",
		Flags:       slices.Concat(utils.NetworkFlags, utils.DatabaseFlags),
		Description: "Shows metadata about the chain status.",
	}
	dbInspectHistoryCmd = &cli.Command{
		Action:    inspectHistory,
		Name:      "inspect-history",
		Usage:     "Inspect the state history within block range",
		ArgsUsage: "<address> [OPTIONAL <storage-slot>]",
		Flags: slices.Concat([]cli.Flag{
			&cli.Uint64Flag{
				Name:  "start",
				Usage: "block number of the range start, zero means earliest history",
			},
			&cli.Uint64Flag{
				Name:  "end",
				Usage: "block number of the range end(included), zero means latest history",
			},
			&cli.BoolFlag{
				Name:  "raw",
				Usage: "display the decoded raw state value (otherwise shows rlp-encoded value)",
			},
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Description: "This command queries the history of the account or storage slot within the specified block range",
	}
	dbAddressStorageCmd = &cli.Command{
		Action:    analyzeAddressStorage,
		Name:      "address-storage",
		Usage:     "Analyze storage slots keyed by addresses",
		ArgsUsage: "",
		Flags: slices.Concat([]cli.Flag{
			&cli.StringFlag{
				Name:     "addresses",
				Usage:    "CSV file containing addresses (expects 'address' column)",
				Required: true,
			},
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Description: `
This command analyzes snapshot storage to determine how much is keyed by addresses.
It first collects all storage hashes from target contracts, then reads addresses
from a CSV file and computes 64 mapping slot hashes per address (for slots 0-63)
to check against the collected storage.`,
	}
	dbContractSlotsCmd = &cli.Command{
		Action:    countContractSlots,
		Name:      "contract-slots",
		Usage:     "Count storage slots for a contract address",
		ArgsUsage: "<contract-address>",
		Flags:     slices.Concat(utils.NetworkFlags, utils.DatabaseFlags),
		Description: `
This command counts the number of storage slots held by a contract address
by iterating over the snapshot storage prefix for the given account.`,
	}
)

func removeDB(ctx *cli.Context) error {
	stack, config := makeConfigNode(ctx)

	// Resolve folder paths.
	var (
		rootDir    = stack.ResolvePath("chaindata")
		ancientDir = config.Eth.DatabaseFreezer
	)
	switch {
	case ancientDir == "":
		ancientDir = filepath.Join(stack.ResolvePath("chaindata"), "ancient")
	case !filepath.IsAbs(ancientDir):
		ancientDir = config.Node.ResolvePath(ancientDir)
	}
	// Delete state data
	statePaths := []string{
		rootDir,
		filepath.Join(ancientDir, rawdb.MerkleStateFreezerName),
		filepath.Join(ancientDir, rawdb.VerkleStateFreezerName),
	}
	confirmAndRemoveDB(statePaths, "state data", ctx, removeStateDataFlag.Name)

	// Delete ancient chain
	chainPaths := []string{filepath.Join(
		ancientDir,
		rawdb.ChainFreezerName,
	)}
	confirmAndRemoveDB(chainPaths, "ancient chain", ctx, removeChainDataFlag.Name)
	return nil
}

// removeFolder deletes all files (not folders) inside the directory 'dir' (but
// not files in subfolders).
func removeFolder(dir string) {
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		// If we're at the top level folder, recurse into
		if path == dir {
			return nil
		}
		// Delete all the files, but not subfolders
		if !info.IsDir() {
			os.Remove(path)
			return nil
		}
		return filepath.SkipDir
	})
}

// confirmAndRemoveDB prompts the user for a last confirmation and removes the
// list of folders if accepted.
func confirmAndRemoveDB(paths []string, kind string, ctx *cli.Context, removeFlagName string) {
	var (
		confirm bool
		err     error
	)
	msg := fmt.Sprintf("Location(s) of '%s': \n", kind)
	for _, path := range paths {
		msg += fmt.Sprintf("\t- %s\n", path)
	}
	fmt.Println(msg)
	if ctx.IsSet(removeFlagName) {
		confirm = ctx.Bool(removeFlagName)
		if confirm {
			fmt.Printf("Remove '%s'? [y/n] y\n", kind)
		} else {
			fmt.Printf("Remove '%s'? [y/n] n\n", kind)
		}
	} else {
		confirm, err = prompt.Stdin.PromptConfirm(fmt.Sprintf("Remove '%s'?", kind))
	}
	switch {
	case err != nil:
		utils.Fatalf("%v", err)
	case !confirm:
		log.Info("Database deletion skipped", "kind", kind, "paths", paths)
	default:
		var (
			deleted []string
			start   = time.Now()
		)
		for _, path := range paths {
			if common.FileExist(path) {
				removeFolder(path)
				deleted = append(deleted, path)
			} else {
				log.Info("Folder is not existent", "path", path)
			}
		}
		log.Info("Database successfully deleted", "kind", kind, "paths", deleted, "elapsed", common.PrettyDuration(time.Since(start)))
	}
}

func inspect(ctx *cli.Context) error {
	var (
		prefix []byte
		start  []byte
	)
	if ctx.NArg() > 2 {
		return fmt.Errorf("max 2 arguments: %v", ctx.Command.ArgsUsage)
	}
	if ctx.NArg() >= 1 {
		if d, err := hexutil.Decode(ctx.Args().Get(0)); err != nil {
			return fmt.Errorf("failed to hex-decode 'prefix': %v", err)
		} else {
			prefix = d
		}
	}
	if ctx.NArg() >= 2 {
		if d, err := hexutil.Decode(ctx.Args().Get(1)); err != nil {
			return fmt.Errorf("failed to hex-decode 'start': %v", err)
		} else {
			start = d
		}
	}
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, true)
	defer db.Close()

	return rawdb.InspectDatabase(db, prefix, start)
}

func checkStateContent(ctx *cli.Context) error {
	var (
		prefix []byte
		start  []byte
	)
	if ctx.NArg() > 1 {
		return fmt.Errorf("max 1 argument: %v", ctx.Command.ArgsUsage)
	}
	if ctx.NArg() > 0 {
		if d, err := hexutil.Decode(ctx.Args().First()); err != nil {
			return fmt.Errorf("failed to hex-decode 'start': %v", err)
		} else {
			start = d
		}
	}
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, true)
	defer db.Close()
	var (
		it        = rawdb.NewKeyLengthIterator(db.NewIterator(prefix, start), 32)
		hasher    = crypto.NewKeccakState()
		got       = make([]byte, 32)
		errs      int
		count     int
		startTime = time.Now()
		lastLog   = time.Now()
	)
	for it.Next() {
		count++
		k := it.Key()
		v := it.Value()
		hasher.Reset()
		hasher.Write(v)
		hasher.Read(got)
		if !bytes.Equal(k, got) {
			errs++
			fmt.Printf("Error at %#x\n", k)
			fmt.Printf("  Hash:  %#x\n", got)
			fmt.Printf("  Data:  %#x\n", v)
		}
		if time.Since(lastLog) > 8*time.Second {
			log.Info("Iterating the database", "at", fmt.Sprintf("%#x", k), "elapsed", common.PrettyDuration(time.Since(startTime)))
			lastLog = time.Now()
		}
	}
	if err := it.Error(); err != nil {
		return err
	}
	log.Info("Iterated the state content", "errors", errs, "items", count)
	return nil
}

func showDBStats(db ethdb.KeyValueStater) {
	stats, err := db.Stat()
	if err != nil {
		log.Warn("Failed to read database stats", "error", err)
		return
	}
	fmt.Println(stats)
}

func dbStats(ctx *cli.Context) error {
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, true)
	defer db.Close()

	showDBStats(db)
	return nil
}

func dbCompact(ctx *cli.Context) error {
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, false)
	defer db.Close()

	log.Info("Stats before compaction")
	showDBStats(db)

	log.Info("Triggering compaction")
	if err := db.Compact(nil, nil); err != nil {
		log.Info("Compact err", "error", err)
		return err
	}
	log.Info("Stats after compaction")
	showDBStats(db)
	return nil
}

// dbGet shows the value of a given database key
func dbGet(ctx *cli.Context) error {
	if ctx.NArg() != 1 {
		return fmt.Errorf("required arguments: %v", ctx.Command.ArgsUsage)
	}
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, true)
	defer db.Close()

	key, err := common.ParseHexOrString(ctx.Args().Get(0))
	if err != nil {
		log.Info("Could not decode the key", "error", err)
		return err
	}

	data, err := db.Get(key)
	if err != nil {
		log.Info("Get operation failed", "key", fmt.Sprintf("%#x", key), "error", err)
		return err
	}
	fmt.Printf("key %#x: %#x\n", key, data)
	return nil
}

// dbDelete deletes a key from the database
func dbDelete(ctx *cli.Context) error {
	if ctx.NArg() != 1 {
		return fmt.Errorf("required arguments: %v", ctx.Command.ArgsUsage)
	}
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, false)
	defer db.Close()

	key, err := common.ParseHexOrString(ctx.Args().Get(0))
	if err != nil {
		log.Info("Could not decode the key", "error", err)
		return err
	}
	data, err := db.Get(key)
	if err == nil {
		fmt.Printf("Previous value: %#x\n", data)
	}
	if err = db.Delete(key); err != nil {
		log.Info("Delete operation returned an error", "key", fmt.Sprintf("%#x", key), "error", err)
		return err
	}
	return nil
}

// dbPut overwrite a value in the database
func dbPut(ctx *cli.Context) error {
	if ctx.NArg() != 2 {
		return fmt.Errorf("required arguments: %v", ctx.Command.ArgsUsage)
	}
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, false)
	defer db.Close()

	var (
		key   []byte
		value []byte
		data  []byte
		err   error
	)
	key, err = common.ParseHexOrString(ctx.Args().Get(0))
	if err != nil {
		log.Info("Could not decode the key", "error", err)
		return err
	}
	value, err = hexutil.Decode(ctx.Args().Get(1))
	if err != nil {
		log.Info("Could not decode the value", "error", err)
		return err
	}
	data, err = db.Get(key)
	if err == nil {
		fmt.Printf("Previous value: %#x\n", data)
	}
	return db.Put(key, value)
}

// dbDumpTrie shows the key-value slots of a given storage trie
func dbDumpTrie(ctx *cli.Context) error {
	if ctx.NArg() < 3 {
		return fmt.Errorf("required arguments: %v", ctx.Command.ArgsUsage)
	}
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, true)
	defer db.Close()

	triedb := utils.MakeTrieDatabase(ctx, stack, db, false, true, false)
	defer triedb.Close()

	var (
		state   []byte
		storage []byte
		account []byte
		start   []byte
		max     = int64(-1)
		err     error
	)
	if state, err = hexutil.Decode(ctx.Args().Get(0)); err != nil {
		log.Info("Could not decode the state root", "error", err)
		return err
	}
	if account, err = hexutil.Decode(ctx.Args().Get(1)); err != nil {
		log.Info("Could not decode the account hash", "error", err)
		return err
	}
	if storage, err = hexutil.Decode(ctx.Args().Get(2)); err != nil {
		log.Info("Could not decode the storage trie root", "error", err)
		return err
	}
	if ctx.NArg() > 3 {
		if start, err = hexutil.Decode(ctx.Args().Get(3)); err != nil {
			log.Info("Could not decode the seek position", "error", err)
			return err
		}
	}
	if ctx.NArg() > 4 {
		if max, err = strconv.ParseInt(ctx.Args().Get(4), 10, 64); err != nil {
			log.Info("Could not decode the max count", "error", err)
			return err
		}
	}
	id := trie.StorageTrieID(common.BytesToHash(state), common.BytesToHash(account), common.BytesToHash(storage))
	theTrie, err := trie.New(id, triedb)
	if err != nil {
		return err
	}
	trieIt, err := theTrie.NodeIterator(start)
	if err != nil {
		return err
	}
	var count int64
	it := trie.NewIterator(trieIt)
	for it.Next() {
		if max > 0 && count == max {
			fmt.Printf("Exiting after %d values\n", count)
			break
		}
		fmt.Printf("  %d. key %#x: %#x\n", count, it.Key, it.Value)
		count++
	}
	return it.Err
}

func freezerInspect(ctx *cli.Context) error {
	if ctx.NArg() < 4 {
		return fmt.Errorf("required arguments: %v", ctx.Command.ArgsUsage)
	}
	var (
		freezer = ctx.Args().Get(0)
		table   = ctx.Args().Get(1)
	)
	start, err := strconv.ParseInt(ctx.Args().Get(2), 10, 64)
	if err != nil {
		log.Info("Could not read start-param", "err", err)
		return err
	}
	end, err := strconv.ParseInt(ctx.Args().Get(3), 10, 64)
	if err != nil {
		log.Info("Could not read count param", "err", err)
		return err
	}
	stack, _ := makeConfigNode(ctx)
	ancient := stack.ResolveAncient("chaindata", ctx.String(utils.AncientFlag.Name))
	stack.Close()
	return rawdb.InspectFreezerTable(ancient, freezer, table, start, end)
}

func importLDBdata(ctx *cli.Context) error {
	start := 0
	switch ctx.NArg() {
	case 1:
		break
	case 2:
		s, err := strconv.Atoi(ctx.Args().Get(1))
		if err != nil {
			return fmt.Errorf("second arg must be an integer: %v", err)
		}
		start = s
	default:
		return fmt.Errorf("required arguments: %v", ctx.Command.ArgsUsage)
	}
	var (
		fName     = ctx.Args().Get(0)
		stack, _  = makeConfigNode(ctx)
		interrupt = make(chan os.Signal, 1)
		stop      = make(chan struct{})
	)
	defer stack.Close()
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(interrupt)
	defer close(interrupt)
	go func() {
		if _, ok := <-interrupt; ok {
			log.Info("Interrupted during ldb import, stopping at next batch")
		}
		close(stop)
	}()
	db := utils.MakeChainDatabase(ctx, stack, false)
	defer db.Close()
	return utils.ImportLDBData(db, fName, int64(start), stop)
}

type preimageIterator struct {
	iter ethdb.Iterator
}

func (iter *preimageIterator) Next() (byte, []byte, []byte, bool) {
	for iter.iter.Next() {
		key := iter.iter.Key()
		if bytes.HasPrefix(key, rawdb.PreimagePrefix) && len(key) == (len(rawdb.PreimagePrefix)+common.HashLength) {
			return utils.OpBatchAdd, key, iter.iter.Value(), true
		}
	}
	return 0, nil, nil, false
}

func (iter *preimageIterator) Release() {
	iter.iter.Release()
}

type snapshotIterator struct {
	init    bool
	account ethdb.Iterator
	storage ethdb.Iterator
}

func (iter *snapshotIterator) Next() (byte, []byte, []byte, bool) {
	if !iter.init {
		iter.init = true
		return utils.OpBatchDel, rawdb.SnapshotRootKey, nil, true
	}
	for iter.account.Next() {
		key := iter.account.Key()
		if bytes.HasPrefix(key, rawdb.SnapshotAccountPrefix) && len(key) == (len(rawdb.SnapshotAccountPrefix)+common.HashLength) {
			return utils.OpBatchAdd, key, iter.account.Value(), true
		}
	}
	for iter.storage.Next() {
		key := iter.storage.Key()
		if bytes.HasPrefix(key, rawdb.SnapshotStoragePrefix) && len(key) == (len(rawdb.SnapshotStoragePrefix)+2*common.HashLength) {
			return utils.OpBatchAdd, key, iter.storage.Value(), true
		}
	}
	return 0, nil, nil, false
}

func (iter *snapshotIterator) Release() {
	iter.account.Release()
	iter.storage.Release()
}

// chainExporters defines the export scheme for all exportable chain data.
var chainExporters = map[string]func(db ethdb.Database) utils.ChainDataIterator{
	"preimage": func(db ethdb.Database) utils.ChainDataIterator {
		iter := db.NewIterator(rawdb.PreimagePrefix, nil)
		return &preimageIterator{iter: iter}
	},
	"snapshot": func(db ethdb.Database) utils.ChainDataIterator {
		account := db.NewIterator(rawdb.SnapshotAccountPrefix, nil)
		storage := db.NewIterator(rawdb.SnapshotStoragePrefix, nil)
		return &snapshotIterator{account: account, storage: storage}
	},
}

func exportChaindata(ctx *cli.Context) error {
	if ctx.NArg() < 2 {
		return fmt.Errorf("required arguments: %v", ctx.Command.ArgsUsage)
	}
	// Parse the required chain data type, make sure it's supported.
	kind := ctx.Args().Get(0)
	kind = strings.ToLower(strings.Trim(kind, " "))
	exporter, ok := chainExporters[kind]
	if !ok {
		var kinds []string
		for kind := range chainExporters {
			kinds = append(kinds, kind)
		}
		return fmt.Errorf("invalid data type %s, supported types: %s", kind, strings.Join(kinds, ", "))
	}
	var (
		stack, _  = makeConfigNode(ctx)
		interrupt = make(chan os.Signal, 1)
		stop      = make(chan struct{})
	)
	defer stack.Close()
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(interrupt)
	defer close(interrupt)
	go func() {
		if _, ok := <-interrupt; ok {
			log.Info("Interrupted during db export, stopping at next batch")
		}
		close(stop)
	}()
	db := utils.MakeChainDatabase(ctx, stack, true)
	defer db.Close()
	return utils.ExportChaindata(ctx.Args().Get(1), kind, exporter(db), stop)
}

func showMetaData(ctx *cli.Context) error {
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()
	db := utils.MakeChainDatabase(ctx, stack, true)
	defer db.Close()

	ancients, err := db.Ancients()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error accessing ancients: %v", err)
	}
	data := rawdb.ReadChainMetadata(db)
	data = append(data, []string{"frozen", fmt.Sprintf("%d items", ancients)})
	data = append(data, []string{"snapshotGenerator", snapshot.ParseGeneratorStatus(rawdb.ReadSnapshotGenerator(db))})
	if b := rawdb.ReadHeadBlock(db); b != nil {
		data = append(data, []string{"headBlock.Hash", fmt.Sprintf("%v", b.Hash())})
		data = append(data, []string{"headBlock.Root", fmt.Sprintf("%v", b.Root())})
		data = append(data, []string{"headBlock.Number", fmt.Sprintf("%d (%#x)", b.Number(), b.Number())})
	}
	if h := rawdb.ReadHeadHeader(db); h != nil {
		data = append(data, []string{"headHeader.Hash", fmt.Sprintf("%v", h.Hash())})
		data = append(data, []string{"headHeader.Root", fmt.Sprintf("%v", h.Root)})
		data = append(data, []string{"headHeader.Number", fmt.Sprintf("%d (%#x)", h.Number, h.Number)})
	}
	table := rawdb.NewTableWriter(os.Stdout)
	table.SetHeader([]string{"Field", "Value"})
	table.AppendBulk(data)
	table.Render()
	return nil
}

func inspectAccount(db *triedb.Database, start uint64, end uint64, address common.Address, raw bool) error {
	stats, err := db.AccountHistory(address, start, end)
	if err != nil {
		return err
	}
	fmt.Printf("Account history:\n\taddress: %s\n\tblockrange: [#%d-#%d]\n", address.Hex(), stats.Start, stats.End)

	from := stats.Start
	for i := 0; i < len(stats.Blocks); i++ {
		var content string
		if len(stats.Origins[i]) == 0 {
			content = "<empty>"
		} else {
			if !raw {
				content = fmt.Sprintf("%#x", stats.Origins[i])
			} else {
				account := new(types.SlimAccount)
				if err := rlp.DecodeBytes(stats.Origins[i], account); err != nil {
					panic(err)
				}
				code := "<nil>"
				if len(account.CodeHash) > 0 {
					code = fmt.Sprintf("%#x", account.CodeHash)
				}
				root := "<nil>"
				if len(account.Root) > 0 {
					root = fmt.Sprintf("%#x", account.Root)
				}
				content = fmt.Sprintf("nonce: %d, balance: %d, codeHash: %s, root: %s", account.Nonce, account.Balance, code, root)
			}
		}
		fmt.Printf("#%d - #%d: %s\n", from, stats.Blocks[i], content)
		from = stats.Blocks[i]
	}
	return nil
}

func inspectStorage(db *triedb.Database, start uint64, end uint64, address common.Address, slot common.Hash, raw bool) error {
	// The hash of storage slot key is utilized in the history
	// rather than the raw slot key, make the conversion.
	stats, err := db.StorageHistory(address, slot, start, end)
	if err != nil {
		return err
	}
	fmt.Printf("Storage history:\n\taddress: %s\n\tslot: %s\n\tblockrange: [#%d-#%d]\n", address.Hex(), slot.Hex(), stats.Start, stats.End)

	from := stats.Start
	for i := 0; i < len(stats.Blocks); i++ {
		var content string
		if len(stats.Origins[i]) == 0 {
			content = "<empty>"
		} else {
			if !raw {
				content = fmt.Sprintf("%#x", stats.Origins[i])
			} else {
				_, data, _, err := rlp.Split(stats.Origins[i])
				if err != nil {
					fmt.Printf("Failed to decode storage slot, %v", err)
					return err
				}
				content = fmt.Sprintf("%#x", data)
			}
		}
		fmt.Printf("#%d - #%d: %s\n", from, stats.Blocks[i], content)
		from = stats.Blocks[i]
	}
	return nil
}

func inspectHistory(ctx *cli.Context) error {
	if ctx.NArg() == 0 || ctx.NArg() > 2 {
		return fmt.Errorf("required arguments: %v", ctx.Command.ArgsUsage)
	}
	var (
		address common.Address
		slot    common.Hash
	)
	if err := address.UnmarshalText([]byte(ctx.Args().Get(0))); err != nil {
		return err
	}
	if ctx.NArg() > 1 {
		if err := slot.UnmarshalText([]byte(ctx.Args().Get(1))); err != nil {
			return err
		}
	}
	// Load the databases.
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, true)
	defer db.Close()

	triedb := utils.MakeTrieDatabase(ctx, stack, db, false, false, false)
	defer triedb.Close()

	var (
		err   error
		start uint64 // the id of first history object to query
		end   uint64 // the id (included) of last history object to query
	)
	// State histories are identified by state ID rather than block number.
	// To address this, load the corresponding block header and perform the
	// conversion by this function.
	blockToID := func(blockNumber uint64) (uint64, error) {
		header := rawdb.ReadHeader(db, rawdb.ReadCanonicalHash(db, blockNumber), blockNumber)
		if header == nil {
			return 0, fmt.Errorf("block #%d is not existent", blockNumber)
		}
		id := rawdb.ReadStateID(db, header.Root)
		if id == nil {
			first, last, err := triedb.HistoryRange()
			if err == nil {
				return 0, fmt.Errorf("history of block #%d is not existent, available history range: [#%d-#%d]", blockNumber, first, last)
			}
			return 0, fmt.Errorf("history of block #%d is not existent", blockNumber)
		}
		return *id, nil
	}
	// Parse the starting block number for inspection.
	startNumber := ctx.Uint64("start")
	if startNumber != 0 {
		start, err = blockToID(startNumber)
		if err != nil {
			return err
		}
	}
	// Parse the ending block number for inspection.
	endBlock := ctx.Uint64("end")
	if endBlock != 0 {
		end, err = blockToID(endBlock)
		if err != nil {
			return err
		}
	}
	// Inspect the state history.
	if slot == (common.Hash{}) {
		return inspectAccount(triedb, start, end, address, ctx.Bool("raw"))
	}
	return inspectStorage(triedb, start, end, address, slot, ctx.Bool("raw"))
}

// analyzeAddressStorage analyzes snapshot storage to determine how much is keyed by addresses.
// It uses an inverted approach:
// Phase 1: Collect all storage hashes from target contracts into a map (per-contract)
// Phase 2: Iterate addresses from CSV, compute 64 hashes per address on-the-fly, check against storage
func analyzeAddressStorage(ctx *cli.Context) error {
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, true)
	defer db.Close()

	// Target contracts to analyze
	targetContracts := []common.Address{
		common.HexToAddress("0x06450dee7fd2fb8e39061434babcfc05599a6fb8"), // Xen Crypto
		common.HexToAddress("0x000000000022d473030f116ddee9f6b43ac78ba3"), // Permit2
		common.HexToAddress("0xdac17f958d2ee523a2206206994597c13d831ec7"), // USDT
		common.HexToAddress("0x00000000006c3852cbef3e08e8df289169ede581"), // Seaport
		common.HexToAddress("0x5acc84a3e955bdd76467d3348077d003f00ffb97"), // Forsage.io
		common.HexToAddress("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"), // USDC
		common.HexToAddress("0x1a2a1c938ce3ec39b6d47113c7955baa9dd454f2"), // Axie Infinity: Ronin Bridge
		common.HexToAddress("0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e"), // ENSRegistryWithFallback
		common.HexToAddress("0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9"), // Aave Token
		common.HexToAddress("0x06012c8cf97bead5deae237070f9587f8e7a266d"), // CryptoKitties: Core
		common.HexToAddress("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"), // WETH
		common.HexToAddress("0x8315177ab297ba92a06054ce80a67ed4dbd7ed3a"), // Arbitrum: Bridge
	}

	log.Info("Target contracts", "count", len(targetContracts))

	// Build set of excluded addresses (precompiles and special addresses)
	excludedAddresses := make(map[common.Address]struct{}, 20)
	// Zero/burn address
	excludedAddresses[common.Address{}] = struct{}{}
	// Precompiles: 0x01-0x11
	for i := 1; i <= 0x11; i++ {
		excludedAddresses[common.BytesToAddress([]byte{byte(i)})] = struct{}{}
	}
	// p256Verify at 0x100
	excludedAddresses[common.BytesToAddress([]byte{0x1, 0x00})] = struct{}{}
	log.Info("Built excluded address set", "count", len(excludedAddresses))

	// Per-contract statistics and storage hash sets
	type contractData struct {
		name              string
		address           common.Address
		accountHash       common.Hash
		storageCache      *fastcache.Cache // storageHash -> nil (existence check only)
		totalSlots        uint64
		addressKeyedSlots atomic.Uint64 // atomic for concurrent access
	}

	contracts := make([]*contractData, len(targetContracts))
	for i, addr := range targetContracts {
		contracts[i] = &contractData{
			name:         addr.Hex(),
			address:      addr,
			accountHash:  crypto.Keccak256Hash(addr.Bytes()),
			storageCache: fastcache.New(64 * 1024 * 1024), // 64MB per contract initially
		}
	}

	// Phase 1: Collect all storage hashes from target contracts
	log.Info("Phase 1: Collecting storage hashes from target contracts")
	var (
		startTime  = time.Now()
		lastReport = time.Now()
	)

	for _, contract := range contracts {
		log.Info("Collecting storage for contract", "contract", contract.name, "accountHash", contract.accountHash.Hex())

		// Use prefix iterator for this specific account's storage
		prefix := append(rawdb.SnapshotStoragePrefix, contract.accountHash.Bytes()...)
		storageIt := rawdb.NewKeyLengthIterator(
			db.NewIterator(prefix, nil),
			len(rawdb.SnapshotStoragePrefix)+2*common.HashLength,
		)

		for storageIt.Next() {
			key := storageIt.Key()
			// Extract storageHash (after prefix and accountHash)
			storageHash := key[len(rawdb.SnapshotStoragePrefix)+common.HashLength:]

			// Store the hash for existence check only
			contract.storageCache.Set(storageHash, nil)
			contract.totalSlots++

			if time.Since(lastReport) > 8*time.Second {
				log.Info("Collecting storage",
					"contract", contract.name,
					"slots", contract.totalSlots,
					"elapsed", common.PrettyDuration(time.Since(startTime)),
				)
				lastReport = time.Now()
			}
		}
		storageIt.Release()

		log.Info("Contract storage collected",
			"contract", contract.name,
			"slots", contract.totalSlots,
		)
	}

	// Calculate total slots collected
	var totalContractSlots uint64
	for _, contract := range contracts {
		totalContractSlots += contract.totalSlots
	}
	log.Info("Phase 1 complete",
		"totalSlots", totalContractSlots,
		"elapsed", common.PrettyDuration(time.Since(startTime)),
	)

	// Phase 2: Read addresses from CSV and check against storage hashes
	csvPath := ctx.String("addresses")
	log.Info("Phase 2: Checking addresses against storage hashes", "file", csvPath, "slotsPerAddress", 64)

	file, err := os.Open(csvPath)
	if err != nil {
		return fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(bufio.NewReader(file))

	// Read header to find address column
	header, err := reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read CSV header: %w", err)
	}
	addressCol := -1
	for i, col := range header {
		if strings.ToLower(col) == "address" {
			addressCol = i
			break
		}
	}
	if addressCol == -1 {
		return fmt.Errorf("CSV file must have an 'address' column")
	}

	var (
		totalAddresses   atomic.Uint64
		skippedAddresses atomic.Uint64
	)
	startTime = time.Now()
	lastReport = time.Now()

	// Set up worker pool for parallel processing
	numWorkers := runtime.NumCPU()
	addrChan := make(chan common.Address, numWorkers*100)
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var input [64]byte

			for addr := range addrChan {
				copy(input[12:32], addr.Bytes()) // Left-padded address in first 32 bytes

				for slot := 32; slot < 64; slot++ {
					// Set slot number in second 32 bytes (big-endian)
					// Clear previous slot value
					for j := 32; j < 64; j++ {
						input[j] = 0
					}
					input[63] = byte(slot)

					// storageKey = keccak256(abi.encode(addr, slot))
					storageKey := crypto.Keccak256(input[:])
					// storageHash = keccak256(storageKey) - this is what's stored in the snapshot
					storageHash := crypto.Keccak256(storageKey)

					// Check against each contract's storage (fastcache is thread-safe)
					for _, contract := range contracts {
						if contract.storageCache.Has(storageHash) {
							contract.addressKeyedSlots.Add(1)
							// Remove from cache to avoid double counting
							contract.storageCache.Del(storageHash)
						}
					}
				}
				totalAddresses.Add(1)
			}
		}()
	}

	// Progress reporter goroutine
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(8 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				log.Info("Processing addresses",
					"addresses", totalAddresses.Load(),
					"skipped", skippedAddresses.Load(),
					"elapsed", common.PrettyDuration(time.Since(startTime)),
				)
			case <-done:
				return
			}
		}
	}()

	// Read addresses and send to workers
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			close(addrChan)
			close(done)
			return fmt.Errorf("failed to read CSV record: %w", err)
		}
		if addressCol >= len(record) {
			continue
		}

		addrStr := strings.TrimSpace(record[addressCol])
		if addrStr == "" {
			continue
		}

		// Parse address
		addr := common.HexToAddress(addrStr)

		// Skip excluded addresses (precompiles, zero address, etc.)
		if _, excluded := excludedAddresses[addr]; excluded {
			skippedAddresses.Add(1)
			continue
		}

		addrChan <- addr
	}

	close(addrChan)
	wg.Wait()
	close(done)

	log.Info("Phase 2 complete",
		"totalAddresses", totalAddresses.Load(),
		"skipped", skippedAddresses.Load(),
		"elapsed", common.PrettyDuration(time.Since(startTime)),
	)

	// Print per-contract results
	var (
		totalSlots        uint64
		addressKeyedSlots uint64
	)

	fmt.Println("\n=== Per-Contract Results ===")
	for _, contract := range contracts {
		keyed := contract.addressKeyedSlots.Load()
		var pct float64
		if contract.totalSlots > 0 {
			pct = float64(keyed) * 100 / float64(contract.totalSlots)
		}
		log.Info("Contract analysis",
			"contract", contract.name,
			"totalSlots", contract.totalSlots,
			"addressKeyedSlots", keyed,
			"percent", fmt.Sprintf("%.2f%%", pct),
		)

		totalSlots += contract.totalSlots
		addressKeyedSlots += keyed
	}

	// Print final summary
	var addressKeyedPercent float64
	if totalSlots > 0 {
		addressKeyedPercent = float64(addressKeyedSlots) * 100 / float64(totalSlots)
	}

	fmt.Println("\n=== Final Summary ===")
	log.Info("Analysis complete",
		"totalAddresses", totalAddresses.Load(),
		"totalStorageSlots", totalSlots,
		"addressKeyedSlots", addressKeyedSlots,
		"addressKeyedPercent", fmt.Sprintf("%.2f%%", addressKeyedPercent),
	)
	return nil
}

// countContractSlots counts the number of storage slots held by a contract address.
func countContractSlots(ctx *cli.Context) error {
	if ctx.NArg() != 1 {
		return fmt.Errorf("required argument: <contract-address>")
	}

	// Parse contract address
	addrStr := ctx.Args().Get(0)
	var addr common.Address
	if err := addr.UnmarshalText([]byte(addrStr)); err != nil {
		return fmt.Errorf("invalid address %q: %w", addrStr, err)
	}

	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, true)
	defer db.Close()

	// Compute account hash
	accountHash := crypto.Keccak256Hash(addr.Bytes())

	log.Info("Counting storage slots",
		"address", addr.Hex(),
		"accountHash", accountHash.Hex(),
	)

	// Iterate storage snapshot for this account
	var (
		count      uint64
		startTime  = time.Now()
		lastReport = time.Now()
	)

	prefix := append(rawdb.SnapshotStoragePrefix, accountHash.Bytes()...)
	it := rawdb.NewKeyLengthIterator(
		db.NewIterator(prefix, nil),
		len(rawdb.SnapshotStoragePrefix)+2*common.HashLength,
	)
	defer it.Release()

	for it.Next() {
		count++

		if time.Since(lastReport) > 8*time.Second {
			log.Info("Counting slots",
				"count", count,
				"elapsed", common.PrettyDuration(time.Since(startTime)),
			)
			lastReport = time.Now()
		}
	}

	if err := it.Error(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}

	log.Info("Storage slot count complete",
		"address", addr.Hex(),
		"slots", count,
		"elapsed", common.PrettyDuration(time.Since(startTime)),
	)
	return nil
}

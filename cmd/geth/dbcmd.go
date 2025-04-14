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
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	// "github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/console/prompt"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	trieutils "github.com/ethereum/go-ethereum/trie/utils"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-verkle"
	"github.com/holiman/uint256"
	"github.com/olekukonko/tablewriter"
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
			dbCheckMetaCmd,
			dbDebugSnapshotAccCmd,
			dbExpiryAnalysisCmd,
			dbAccExpiryAnalysisCmd,
			dbSlotExpiryAnalysisCmd,
			dbSlotExpiryAnalysisV2Cmd,
			dbSlotExpiryAnalysisV3Cmd,
			dbStemAnalysisCmd,
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
		},
	}
	dbInspectCmd = &cli.Command{
		Action:    inspect,
		Name:      "inspect",
		ArgsUsage: "<prefix> <start>",
		Flags: slices.Concat([]cli.Flag{
			utils.SyncModeFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Usage:       "Inspect the storage size for each type of data in the database",
		Description: `This commands iterates the entire database. If the optional 'prefix' and 'start' arguments are provided, then the iteration is limited to the given subset of data.`,
	}
	dbCheckMetaCmd = &cli.Command{
		Action:    checkMeta,
		Name:      "check-meta",
		ArgsUsage: "",
		Flags: slices.Concat([]cli.Flag{
			utils.SyncModeFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Usage:       "Check the meta data of the database",
		Description: `TODO: add description`,
	}
	dbDebugSnapshotAccCmd = &cli.Command{
		Action:    debugSnapshotAcc,
		Name:      "debug-acc",
		ArgsUsage: "",
		Flags: slices.Concat([]cli.Flag{
			utils.SyncModeFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Usage:       "Check the meta data of the database",
		Description: `TODO: add description`,
	}
	dbExpiryAnalysisCmd = &cli.Command{
		Action:    expiryAnalysis,
		Name:      "expiry-analysis",
		ArgsUsage: "<start> <periodLength>",
		Flags: slices.Concat([]cli.Flag{
			utils.SyncModeFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Usage:       "TODO: add this",
		Description: `TODO: add description`,
	}
	dbAccExpiryAnalysisCmd = &cli.Command{
		Action:      accExpiryAnalysis,
		Name:        "acc-expiry",
		ArgsUsage:   "<start> <periodLength>",
		Flags:       slices.Concat(utils.NetworkFlags, utils.DatabaseFlags),
		Usage:       "TODO: add this",
		Description: `TODO: add description`,
	}
	dbSlotExpiryAnalysisCmd = &cli.Command{
		Action:      slotExpiryAnalysis,
		Name:        "slot-expiry",
		ArgsUsage:   "<start> <periodLength>",
		Flags:       slices.Concat(utils.NetworkFlags, utils.DatabaseFlags),
		Usage:       "TODO: add this",
		Description: `TODO: add description`,
	}
	dbSlotExpiryAnalysisV2Cmd = &cli.Command{
		Action:      slotExpiryAnalysisV2,
		Name:        "slot-expiry-v2",
		ArgsUsage:   "<start> <periodLength>",
		Flags:       slices.Concat(utils.NetworkFlags, utils.DatabaseFlags),
		Usage:       "TODO: add this",
		Description: `TODO: add description`,
	}
	dbStemAnalysisCmd = &cli.Command{
		Action:      stemAnalysis,
		Name:        "stem-analysis",
		ArgsUsage:   "<start> <periodLength>",
		Flags:       slices.Concat(utils.NetworkFlags, utils.DatabaseFlags),
		Usage:       "TODO: add this",
		Description: `TODO: add description`,
	}
	dbSlotExpiryAnalysisV3Cmd = &cli.Command{
		Action:      slotExpiryAnalysisV3,
		Name:        "slot-expiry-v3",
		ArgsUsage:   "<start> <periodLength>",
		Flags:       slices.Concat(utils.NetworkFlags, utils.DatabaseFlags),
		Usage:       "TODO: add this",
		Description: `TODO: add description`,
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
		Flags: slices.Concat([]cli.Flag{
			utils.SyncModeFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
	}
	dbCompactCmd = &cli.Command{
		Action: dbCompact,
		Name:   "compact",
		Usage:  "Compact leveldb database. WARNING: May take a very long time",
		Flags: slices.Concat([]cli.Flag{
			utils.SyncModeFlag,
			utils.CacheFlag,
			utils.CacheDatabaseFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Description: `This command performs a database compaction.
WARNING: This operation may take a very long time to finish, and may cause database
corruption if it is aborted during execution'!`,
	}
	dbGetCmd = &cli.Command{
		Action:    dbGet,
		Name:      "get",
		Usage:     "Show the value of a database key",
		ArgsUsage: "<hex-encoded key>",
		Flags: slices.Concat([]cli.Flag{
			utils.SyncModeFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Description: "This command looks up the specified database key from the database.",
	}
	dbDeleteCmd = &cli.Command{
		Action:    dbDelete,
		Name:      "delete",
		Usage:     "Delete a database key (WARNING: may corrupt your database)",
		ArgsUsage: "<hex-encoded key>",
		Flags: slices.Concat([]cli.Flag{
			utils.SyncModeFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Description: `This command deletes the specified database key from the database.
WARNING: This is a low-level operation which may cause database corruption!`,
	}
	dbPutCmd = &cli.Command{
		Action:    dbPut,
		Name:      "put",
		Usage:     "Set the value of a database key (WARNING: may corrupt your database)",
		ArgsUsage: "<hex-encoded key> <hex-encoded value>",
		Flags: slices.Concat([]cli.Flag{
			utils.SyncModeFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Description: `This command sets a given database key to the given value.
WARNING: This is a low-level operation which may cause database corruption!`,
	}
	dbGetSlotsCmd = &cli.Command{
		Action:    dbDumpTrie,
		Name:      "dumptrie",
		Usage:     "Show the storage key/values of a given storage trie",
		ArgsUsage: "<hex-encoded state root> <hex-encoded account hash> <hex-encoded storage trie root> <hex-encoded start (optional)> <int max elements (optional)>",
		Flags: slices.Concat([]cli.Flag{
			utils.SyncModeFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Description: "This command looks up the specified database key from the database.",
	}
	dbDumpFreezerIndex = &cli.Command{
		Action:    freezerInspect,
		Name:      "freezer-index",
		Usage:     "Dump out the index of a specific freezer table",
		ArgsUsage: "<freezer-type> <table-type> <start (int)> <end (int)>",
		Flags: slices.Concat([]cli.Flag{
			utils.SyncModeFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Description: "This command displays information about the freezer index.",
	}
	dbImportCmd = &cli.Command{
		Action:    importLDBdata,
		Name:      "import",
		Usage:     "Imports leveldb-ata from an exported RLP dump.",
		ArgsUsage: "<dumpfile> <start (optional)",
		Flags: slices.Concat([]cli.Flag{
			utils.SyncModeFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Description: "The import command imports the specific chain data from an RLP encoded stream.",
	}
	dbExportCmd = &cli.Command{
		Action:    exportChaindata,
		Name:      "export",
		Usage:     "Exports the chain data into an RLP dump. If the <dumpfile> has .gz suffix, gzip compression will be used.",
		ArgsUsage: "<type> <dumpfile>",
		Flags: slices.Concat([]cli.Flag{
			utils.SyncModeFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Description: "Exports the specified chain data to an RLP encoded stream, optionally gzip-compressed.",
	}
	dbMetadataCmd = &cli.Command{
		Action: showMetaData,
		Name:   "metadata",
		Usage:  "Shows metadata about the chain status.",
		Flags: slices.Concat([]cli.Flag{
			utils.SyncModeFlag,
		}, utils.NetworkFlags, utils.DatabaseFlags),
		Description: "Shows metadata about the chain status.",
	}
	dbInspectHistoryCmd = &cli.Command{
		Action:    inspectHistory,
		Name:      "inspect-history",
		Usage:     "Inspect the state history within block range",
		ArgsUsage: "<address> [OPTIONAL <storage-slot>]",
		Flags: slices.Concat([]cli.Flag{
			utils.SyncModeFlag,
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

func checkMeta(ctx *cli.Context) error {
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, true)
	defer db.Close()

	accHash := common.HexToHash("0x0059a771a6fa49af4b0778f7d1053619d98f129e8ccfb055477f65e0ccb89b0c")
	it := db.NewIterator(append(rawdb.SnapshotStoragePrefix, accHash.Bytes()...), nil)
	defer it.Release()

	fmt.Println("Normal iterator")
	for it.Next() {
		key := it.Key()

		if len(key) != len(rawdb.SnapshotStoragePrefix)+common.HashLength+common.HashLength {
			log.Crit("Invalid storage key", "key", key)
		}

		addrHash := common.BytesToHash(key[len(rawdb.SnapshotAccountPrefix) : len(rawdb.SnapshotAccountPrefix)+common.HashLength])
		storageHash := common.BytesToHash(key[len(rawdb.SnapshotAccountPrefix)+common.HashLength : len(rawdb.SnapshotAccountPrefix)+common.HashLength+common.HashLength])
		fmt.Printf("account %s, storage %s\n", addrHash.String(), storageHash.String())
	}

	it = db.NewIterator(append(rawdb.SnapshotStorageMetaPrefix, accHash.Bytes()...), nil)
	defer it.Release()

	for it.Next() {
		key := it.Key()
		val := it.Value()

		if len(key) != len(rawdb.SnapshotStorageMetaPrefix)+common.HashLength+common.HashLength {
			log.Crit("Invalid storage meta key", "key", key)
		}

		addrHash := common.BytesToHash(key[len(rawdb.SnapshotStorageMetaPrefix) : len(rawdb.SnapshotStorageMetaPrefix)+common.HashLength])
		storageHash := common.BytesToHash(key[len(rawdb.SnapshotStorageMetaPrefix)+common.HashLength : len(rawdb.SnapshotStorageMetaPrefix)+common.HashLength+common.HashLength])

		fmt.Printf("account %s, storage %s, meta %d\n", addrHash.String(), storageHash.String(), binary.BigEndian.Uint64(val))
	}
	return nil
}

func debugSnapshotAcc(ctx *cli.Context) error {
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, true)
	defer db.Close()

	it := db.NewIterator(rawdb.SnapshotAccountPrefix, nil)
	defer it.Release()

	for it.Next() {
		key := it.Key()
		val := it.Value()
		addrHash := common.BytesToHash(key[1:])
		addr := common.BytesToAddress(rawdb.ReadPreimage(db, addrHash))
		log.Info("account", "addr", addr.Hex())
		var slim types.SlimAccount
		if err := rlp.DecodeBytes(val, &slim); err != nil {
			log.Error("failed to decode account", "error", err)
		}
		acc := types.StateAccount{
			Nonce:    slim.Nonce,
			Balance:  slim.Balance,
			Root:     common.BytesToHash(slim.Root),
			CodeHash: slim.CodeHash,
		}
		fmt.Printf("(%d) account %x, nonce %d, balance %s\n", len(val), addr.Hex(), acc.Nonce, acc.Balance)
	}

	return nil
}

// Worker job structure
type slotJob struct {
	addrHash   common.Hash
	addrPoint  *verkle.Point
	rawSlotKey []byte
	db         ethdb.Database
}

// Worker result structure
type slotResult struct {
	stem   string
	period verkle.StatePeriod
}

// Worker function
func processSlot(slotsCache *fastcache.Cache, job slotJob, start uint64, periodLength uint64) slotResult {
	slotHash := common.BytesToHash(job.rawSlotKey[len(rawdb.SnapshotStoragePrefix)+common.HashLength:])

	// Get slot from cache or database
	slot := slotsCache.Get(nil, slotHash[:])
	if len(slot) == 0 {
		slot = rawdb.ReadPreimage(job.db, slotHash)
		slotsCache.Set(slotHash[:], slot)
	}

	slotPeriod := getPeriod(rawdb.ReadStorageSnapshotMeta(job.db, job.addrHash, common.BytesToHash(slot)), start, periodLength)
	slotStem := getSlotStem(job.addrPoint, slot)

	return slotResult{
		stem:   string(slotStem),
		period: slotPeriod,
	}
}

func getSlotStem(addrPoint *verkle.Point, slotNum []byte) []byte {
	key := trieutils.StorageSlotKeyWithEvaluatedAddress(addrPoint, slotNum)
	return key[:verkle.StemSize]
}

func getPeriod(blockNum uint64, start uint64, periodLength uint64) verkle.StatePeriod {
	/*
		+-------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------+
		|          FIELD          |                                                                       VALUE                                                                        |
		+-------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------+
		| databaseVersion         | 8 (0x8)                                                                                                                                            |
		| headBlockHash           | 0xc4232ce7ab40d61b7fae4bd1f3e74b4da5b724fcb13fcb30d10411020c2d19f9                                                                                 |
		| headFastBlockHash       | 0xc4232ce7ab40d61b7fae4bd1f3e74b4da5b724fcb13fcb30d10411020c2d19f9                                                                                 |
		| headHeaderHash          | 0xc4232ce7ab40d61b7fae4bd1f3e74b4da5b724fcb13fcb30d10411020c2d19f9                                                                                 |
		| lastPivotNumber         | <nil>                                                                                                                                              |
		| len(snapshotSyncStatus) | 0 bytes                                                                                                                                            |
		| snapshotDisabled        | false                                                                                                                                              |
		| snapshotJournal         | 34 bytes                                                                                                                                           |
		| snapshotRecoveryNumber  | <nil>                                                                                                                                              |
		| snapshotRoot            | 0xef3bcc645cbffd57f1b827100ec7db04a2ead9fe47d409b6c6f1336237987836                                                                                 |
		| txIndexTail             | 14815430 (0xe210c6)                                                                                                                                |
		| SkeletonSyncStatus      | {"Subchains":[{"Head":19838928,"Tail":17165430,"Next":"0xc4232ce7ab40d61b7fae4bd1f3e74b4da5b724fcb13fcb30d10411020c2d19f9"}],"Finalized":17427632} |
		| frozen                  | 17075430 items                                                                                                                                     |
		| snapshotGenerator       | Done: true, Accounts: 0,                                                                                                                           |
		|                         | Slots: 0, Storage: 0, Marker:                                                                                                                      |
		| headBlock.Hash          | 0xc4232ce7ab40d61b7fae4bd1f3e74b4da5b724fcb13fcb30d10411020c2d19f9                                                                                 |
		| headBlock.Root          | 0x17cb88baed411c0ff15b0550c1b7471985a3c8fb733e2f5fb83229b44be405fe                                                                                 |
		| headBlock.Number        | 17165429 (0x105ec75)                                                                                                                               |
		| headHeader.Hash         | 0xc4232ce7ab40d61b7fae4bd1f3e74b4da5b724fcb13fcb30d10411020c2d19f9                                                                                 |
		| headHeader.Root         | 0x17cb88baed411c0ff15b0550c1b7471985a3c8fb733e2f5fb83229b44be405fe                                                                                 |
		| headHeader.Number       | 17165429 (0x105ec75)                                                                                                                               |
		+-------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------+
	*/
	if blockNum == 0 {
		return verkle.StatePeriod(0)
	}
	if blockNum < start { // this should not happen
		log.Warn("block number is less than the starting block number", "blockNum", blockNum)
		return verkle.StatePeriod(0)
	}
	return verkle.StatePeriod((blockNum - start) / periodLength)
}

func expiryAnalysis(ctx *cli.Context) error {
	if ctx.NArg() > 2 {
		return fmt.Errorf("max 2 argument: %v", ctx.Command.ArgsUsage)
	}

	var start uint64
	var periodLength uint64
	switch ctx.NArg() {
	case 2:
		s, err := strconv.ParseUint(ctx.Args().First(), 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse period: %v", err)
		}
		start = s

		p, err := strconv.ParseUint(ctx.Args().Get(1), 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse period: %v", err)
		}
		periodLength = p
	case 1:
		s, err := strconv.ParseUint(ctx.Args().First(), 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse period: %v", err)
		}
		start = s
		periodLength = 1314000
	default:
		start = 17165429 // This is the starting block number of the node snapshot

		// 1314000 = About 6 months worth of blocks.
		// 6 months = 15768000s
		// number of blocks = 15768000 / 12s = 1314000
		// 12s is the average block time.
		periodLength = 1314000
	}

	getCodeStem := func(addrPoint *verkle.Point, code []byte) [][]byte {
		var chunkStems [][]byte
		chunks := trie.ChunkifyCode(code)
		for i := 128; i < len(chunks)/32; {
			chunkKey := trieutils.CodeChunkKeyWithEvaluatedAddress(addrPoint, uint256.NewInt(uint64(i)))
			chunkStems = append(chunkStems, chunkKey[:verkle.StemSize])
			i += 256
		}
		return chunkStems
	}

	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, true)
	defer db.Close()

	it := db.NewIterator(rawdb.SnapshotAccountPrefix, nil)
	defer it.Release()

	count := 0
	accCount := 0
	slotCount := 0
	periodCount := make(map[verkle.StatePeriod]int)

	startTime := time.Now()
	logged := time.Now()
	logProgress := func() {
		count++
		if count%1000 == 0 && time.Since(logged) > 8*time.Second {
			log.Info("Inspecting database", "count", count, "elapsed", common.PrettyDuration(time.Since(startTime)))
			logged = time.Now()
		}
	}

	slotPreCache := fastcache.New(10 * 1024 * 1024 * 1024)
	for it.Next() {
		rawAccKey := it.Key()
		rawAccVal := it.Value()

		// ...So because we are using hash-based, where the hash node isn't prefixed,
		// we might get some false positives here...
		// Therefore, we need to ensure that the expected snapshot key length is correct.
		if len(rawAccKey) != len(rawdb.SnapshotAccountPrefix)+common.HashLength {
			continue
		}

		accCount++

		addrHash := common.BytesToHash(rawAccKey[len(rawdb.SnapshotAccountPrefix) : len(rawdb.SnapshotAccountPrefix)+common.HashLength])
		addrPre := rawdb.ReadPreimage(db, addrHash)
		if len(addrPre) != common.AddressLength {
			log.Error("failed to read preimage", "addr", addrHash.Hex())
			continue
		}
		addr := common.BytesToAddress(addrPre)
		acc, err := types.FullAccount(rawAccVal)
		if err != nil {
			log.Error("failed to decode account", "hash", addrHash.Hex(), "addr", addr.Hex(), "error", err)
			continue
		}
		accPeriod := getPeriod(rawdb.ReadAccountSnapshotMeta(db, addrHash), start, periodLength)
		addrPoint := trieutils.EvaluateAddressPoint(addr[:])
		accStem := trieutils.BasicDataKeyWithEvaluatedAddress(addrPoint)[:verkle.StemSize]
		periodCount[accPeriod]++ // account stem is always unique

		var codeStems [][]byte
		if !bytes.Equal(acc.CodeHash, types.EmptyCodeHash[:]) {
			code := rawdb.ReadCode(db, common.BytesToHash(acc.CodeHash))
			codeStems = getCodeStem(addrPoint, code)
			for range codeStems { // if code stems present, they are always unique
				periodCount[accPeriod]++
			}
		}

		numWorkers := runtime.NumCPU()
		jobs := make(chan slotJob, 100)
		results := make(chan slotResult, 100)

		// Start workers
		var wg sync.WaitGroup
		for range numWorkers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for job := range jobs {
					results <- processSlot(slotPreCache, job, start, periodLength)
				}
			}()
		}

		// Create a goroutine to close results channel after all jobs are processed
		go func() {
			wg.Wait()
			close(results)
		}()

		storageIt := db.NewIterator(append(rawdb.SnapshotStoragePrefix, addrHash.Bytes()...), nil)
		slotStemToPeriod := make(map[string]verkle.StatePeriod)

		// Send jobs to workers
		go func() {
			for storageIt.Next() {
				slotCount++
				jobs <- slotJob{
					addrHash:   addrHash,
					addrPoint:  addrPoint,
					rawSlotKey: storageIt.Key(),
					db:         db,
				}
			}
			storageIt.Release()
			close(jobs)
		}()

		// Collect results
		for result := range results {
			logProgress()
			if bytes.Equal([]byte(result.stem), accStem) { // skip if the stem is the same as the account stem
				continue
			}

			if existingPeriod, ok := slotStemToPeriod[result.stem]; !ok {
				slotStemToPeriod[result.stem] = result.period
			} else if result.period > existingPeriod {
				slotStemToPeriod[result.stem] = result.period
			}
		}

		// Populate the period count for each unique slot stem
		for _, period := range slotStemToPeriod {
			periodCount[period]++
		}

		logProgress()
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Period", "Stem Count"})
	total := 0
	// Create a sorted slice of periods
	var periods []verkle.StatePeriod
	for period := range periodCount {
		periods = append(periods, period)
	}
	slices.Sort(periods)

	// Append to table in sorted order
	for _, period := range periods {
		count := periodCount[period]
		table.Append([]string{fmt.Sprintf("%d", period), fmt.Sprintf("%d", count)})
		total += count
	}
	table.SetFooter([]string{"Total", fmt.Sprintf("%d", total)})
	table.Render()
	log.Info("Account count", "count", accCount)
	log.Info("Slot count", "count", slotCount)

	return nil
}

func accExpiryAnalysis(ctx *cli.Context) error {
	if ctx.NArg() > 2 {
		return fmt.Errorf("max 2 argument: %v", ctx.Command.ArgsUsage)
	}

	var start uint64
	var periodLength uint64
	blockRange := uint64(219000) // 1 month worth of blocks
	switch ctx.NArg() {
	case 2:
		s, err := strconv.ParseUint(ctx.Args().First(), 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse period: %v", err)
		}
		start = s

		p, err := strconv.ParseUint(ctx.Args().Get(1), 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse period: %v", err)
		}
		periodLength = p
	case 1:
		s, err := strconv.ParseUint(ctx.Args().First(), 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse period: %v", err)
		}
		start = s
		periodLength = 1314000
	default:
		start = 17165429 // This is the starting block number of the node snapshot

		// 1314000 = About 6 months worth of blocks.
		// 6 months = 15768000s
		// number of blocks = 15768000 / 12s = 1314000
		// 12s is the average block time.
		periodLength = 1314000
	}

	getCodeStem := func(addrPoint *verkle.Point, code []byte) [][]byte {
		var chunkStems [][]byte
		chunks := trie.ChunkifyCode(code)
		for i := 128; i < len(chunks)/32; {
			chunkKey := trieutils.CodeChunkKeyWithEvaluatedAddress(addrPoint, uint256.NewInt(uint64(i)))
			chunkStems = append(chunkStems, chunkKey[:verkle.StemSize])
			i += 256
		}
		return chunkStems
	}

	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, true)
	defer db.Close()

	it := db.NewIterator(rawdb.SnapshotAccountPrefix, nil)
	defer it.Release()

	accBuckets := make(map[int]int)
	addBucket := func(bn uint64, buckets map[int]int) {
		if bn == 0 {
			buckets[0]++
			return
		}
		if bn < start {
			buckets[-1]++
			return
		}
		bucket := int((bn-start)/blockRange) + 1
		buckets[bucket]++
	}

	count := 0
	logged := time.Now()
	startTime := time.Now()
	logProgress := func() {
		count++
		if count%1000 == 0 && time.Since(logged) > 8*time.Second {
			log.Info("Inspecting database", "count", count, "elapsed", common.PrettyDuration(time.Since(startTime)))
			logged = time.Now()
		}
	}

	accPeriodCount := make(map[verkle.StatePeriod]int)
	codePeriodCount := make(map[verkle.StatePeriod]int)
	for it.Next() {
		rawAccKey := it.Key()
		rawAccVal := it.Value()

		// ...So because we are using hash-based, where the hash node isn't prefixed,
		// we might get some false positives here...
		// Therefore, we need to ensure that the expected snapshot key length is correct.
		if len(rawAccKey) != len(rawdb.SnapshotAccountPrefix)+common.HashLength {
			continue
		}

		addrHash := common.BytesToHash(rawAccKey[len(rawdb.SnapshotAccountPrefix) : len(rawdb.SnapshotAccountPrefix)+common.HashLength])
		addrPre := rawdb.ReadPreimage(db, addrHash)
		addr := common.BytesToAddress(addrPre)
		if len(addrPre) != common.AddressLength {
			log.Error("failed to read preimage", "addr", addrHash.Hex())
			continue
		}
		acc, err := types.FullAccount(rawAccVal)
		if err != nil {
			log.Error("failed to decode account", "hash", addrHash.Hex(), "addr", addr.Hex(), "error", err)
			continue
		}
		addrBn := rawdb.ReadAccountSnapshotMeta(db, addrHash)
		accPeriod := getPeriod(addrBn, start, periodLength)
		addrPoint := trieutils.EvaluateAddressPoint(addr[:])
		accPeriodCount[accPeriod]++
		addBucket(addrBn, accBuckets)

		var codeStems [][]byte
		if !bytes.Equal(acc.CodeHash, types.EmptyCodeHash[:]) {
			code := rawdb.ReadCode(db, common.BytesToHash(acc.CodeHash))
			codeStems = getCodeStem(addrPoint, code)
			for range codeStems { // if code stems present, they are always unique
				codePeriodCount[accPeriod]++
			}
		}

		logProgress()
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Account Period", "Stem Count"})
	total := 0
	// Create a sorted slice of periods
	var periods []verkle.StatePeriod
	for period := range accPeriodCount {
		periods = append(periods, period)
	}
	slices.Sort(periods)

	// Append to table in sorted order
	for _, period := range periods {
		count := accPeriodCount[period]
		table.Append([]string{fmt.Sprintf("%d", period), fmt.Sprintf("%d", count)})
		total += count
	}
	table.SetFooter([]string{"Total", fmt.Sprintf("%d", total)})
	table.Render()

	// Display code period count
	table = tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Code Period", "Stem Count"})
	total = 0
	var codePeriods []verkle.StatePeriod
	for period := range codePeriodCount {
		codePeriods = append(codePeriods, period)
	}
	slices.Sort(codePeriods)

	for _, period := range codePeriods {
		count := codePeriodCount[period]
		table.Append([]string{fmt.Sprintf("%d", period), fmt.Sprintf("%d", count)})
		total += count
	}
	table.SetFooter([]string{"Total", fmt.Sprintf("%d", total)})
	table.Render()

	// Display account in block range
	table = tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Block Range", "Account Count"})
	total = 0

	// Get sorted bucket numbers
	var bucketNums []int
	for bucket := range accBuckets {
		bucketNums = append(bucketNums, bucket)
	}
	slices.Sort(bucketNums)

	// Display results with proper block ranges
	for _, bucket := range bucketNums {
		var rangeStr string
		switch bucket {
		case -1:
			rangeStr = fmt.Sprintf("< %d", start)
		case 0:
			rangeStr = "Empty/Zero"
		default:
			bucketStart := start + uint64(bucket-1)*blockRange
			bucketEnd := bucketStart + blockRange - 1
			rangeStr = fmt.Sprintf("%d - %d", bucketStart, bucketEnd)
		}
		count := accBuckets[bucket]
		table.Append([]string{rangeStr, fmt.Sprintf("%d", count)})
		total += count
	}
	table.SetFooter([]string{"Total", fmt.Sprintf("%d", total)})
	table.Render()

	return nil
}

var (
	_uint64  = new(uint256.Int).SetUint64(64)
	_uint256 = new(uint256.Int).SetUint64(256)
	_uint0   = new(uint256.Int).SetUint64(0)
	_uint1   = new(uint256.Int).SetUint64(1)
)

func slotExpiryAnalysis(ctx *cli.Context) error {
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, true)
	defer db.Close()

	if ctx.NArg() > 2 {
		return fmt.Errorf("max 2 argument: %v", ctx.Command.ArgsUsage)
	}

	var start uint64
	var periodLength uint64
	blockRange := uint64(219000) // 1 month worth of blocks
	switch ctx.NArg() {
	case 2:
		s, err := strconv.ParseUint(ctx.Args().First(), 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse period: %v", err)
		}
		start = s

		p, err := strconv.ParseUint(ctx.Args().Get(1), 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse period: %v", err)
		}
		periodLength = p
	case 1:
		s, err := strconv.ParseUint(ctx.Args().First(), 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse period: %v", err)
		}
		start = s
		periodLength = 1314000
	default:
		start = 17165429 // This is the starting block number of the node snapshot

		// 1314000 = About 6 months worth of blocks.
		// 6 months = 15768000s
		// number of blocks = 15768000 / 12s = 1314000
		// 12s is the average block time.
		periodLength = 1314000
	}

	count := 0
	logged := time.Now()
	startTime := time.Now()
	logProgress := func() {
		count++
		if count%1000 == 0 && time.Since(logged) > 8*time.Second {
			log.Info("Inspecting database", "count", count, "elapsed", common.PrettyDuration(time.Since(startTime)))
			logged = time.Now()
		}
	}

	slotBuckets := make(map[int]int)
	addBucket := func(bn uint64, buckets map[int]int) {
		if bn == 0 {
			buckets[0]++
			return
		}
		if bn < start {
			buckets[-1]++
			return
		}
		bucket := int((bn-start)/blockRange) + 1
		buckets[bucket]++
	}

	getGroup := func(slotNum *uint256.Int) *uint256.Int {
		// If slot number <= 64, return group 0
		if slotNum.Cmp(_uint64) <= 0 {
			return _uint0
		}

		group := new(uint256.Int).Sub(slotNum, _uint64)
		quotient := new(uint256.Int).Div(group, _uint256)
		remainder := new(uint256.Int).Mod(group, _uint256)
		// If there's any remainder, add 1 to round up to next group
		if remainder.Cmp(_uint0) > 0 {
			return new(uint256.Int).Add(quotient, _uint1)
		}
		return quotient
	}

	checkPoint := 100000000 // print every 100 million slots
	printResult := func(count int, periodCount map[verkle.StatePeriod]int) {
		fmt.Printf("Checkpoint reached at %d\n", count)
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"Slot Period", "Stem Count"})
		total := 0
		// Create a sorted slice of periods
		periods := make([]verkle.StatePeriod, 0, len(periodCount))
		for period := range periodCount {
			periods = append(periods, period)
		}
		slices.Sort(periods)

		for _, period := range periods {
			count := periodCount[period]
			table.Append([]string{fmt.Sprintf("%d", period), fmt.Sprintf("%d", count)})
			total += count
		}
		table.SetFooter([]string{"Total", fmt.Sprintf("%d", total)})
		table.Render()

		// Display slot in block range
		table = tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"Block Range", "Slot Count"})
		total = 0

		// Get sorted bucket numbers
		var bucketNums []int
		for bucket := range slotBuckets {
			bucketNums = append(bucketNums, bucket)
		}
		slices.Sort(bucketNums)

		// Display results with proper block ranges
		for _, bucket := range bucketNums {
			var rangeStr string
			switch bucket {
			case -1:
				rangeStr = fmt.Sprintf("< %d", start)
			case 0:
				rangeStr = "Empty/Zero"
			default:
				bucketStart := start + uint64(bucket-1)*blockRange
				bucketEnd := bucketStart + blockRange - 1
				rangeStr = fmt.Sprintf("%d - %d", bucketStart, bucketEnd)
			}
			count := slotBuckets[bucket]
			table.Append([]string{rangeStr, fmt.Sprintf("%d", count)})
			total += count
		}
		table.SetFooter([]string{"Total", fmt.Sprintf("%d", total)})
		table.Render()
	}

	periodCount := make(map[verkle.StatePeriod]int)
	preimageCache := fastcache.New(1 * 1024 * 1024 * 1024)
	curGroup := make(map[uint256.Int]verkle.StatePeriod)
	curAddr := common.Hash{}
	it := db.NewIterator(rawdb.SnapshotStoragePrefix, nil)
	defer it.Release()
	for it.Next() {
		rawSlotKey := it.Key()

		if len(rawSlotKey) != len(rawdb.SnapshotStoragePrefix)+2*common.HashLength {
			continue
		}

		addrHash := common.BytesToHash(rawSlotKey[len(rawdb.SnapshotStoragePrefix) : len(rawdb.SnapshotStoragePrefix)+common.HashLength])
		if !bytes.Equal(addrHash[:], curAddr[:]) { // we are on a new address
			for _, period := range curGroup {
				periodCount[period]++
			}

			curAddr = addrHash
			curGroup = make(map[uint256.Int]verkle.StatePeriod)
		}

		slotHash := common.BytesToHash(rawSlotKey[len(rawdb.SnapshotStoragePrefix)+common.HashLength : len(rawdb.SnapshotStoragePrefix)+2*common.HashLength])
		slot := preimageCache.Get(nil, slotHash[:])
		if len(slot) == 0 {
			slot = rawdb.ReadPreimage(db, slotHash)
			preimageCache.Set(slotHash[:], slot)
		}

		slotBn := rawdb.ReadStorageSnapshotMeta(db, addrHash, common.BytesToHash(slot))
		slotPeriod := getPeriod(slotBn, start, periodLength)
		group := getGroup(new(uint256.Int).SetBytes32(slot))
		if prev, ok := curGroup[*group]; !ok {
			curGroup[*group] = slotPeriod
		} else if slotPeriod > prev {
			curGroup[*group] = slotPeriod
		}
		addBucket(slotBn, slotBuckets)
		logProgress()

		if count%checkPoint == 0 {
			printResult(count, periodCount)
		}
	}

	// Add the last group
	for _, period := range curGroup {
		periodCount[period]++
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Slot Period", "Stem Count"})
	total := 0
	// Create a sorted slice of periods
	var periods []verkle.StatePeriod
	for period := range periodCount {
		periods = append(periods, period)
	}
	slices.Sort(periods)

	for _, period := range periods {
		count := periodCount[period]
		table.Append([]string{fmt.Sprintf("%d", period), fmt.Sprintf("%d", count)})
		total += count
	}
	table.SetFooter([]string{"Total", fmt.Sprintf("%d", total)})
	table.Render()

	// Display slot in block range
	table = tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Block Range", "Slot Count"})
	total = 0

	// Get sorted bucket numbers
	var bucketNums []int
	for bucket := range slotBuckets {
		bucketNums = append(bucketNums, bucket)
	}
	slices.Sort(bucketNums)

	// Display results with proper block ranges
	for _, bucket := range bucketNums {
		var rangeStr string
		switch bucket {
		case -1:
			rangeStr = fmt.Sprintf("< %d", start)
		case 0:
			rangeStr = "Empty/Zero"
		default:
			bucketStart := start + uint64(bucket-1)*blockRange
			bucketEnd := bucketStart + blockRange - 1
			rangeStr = fmt.Sprintf("%d - %d", bucketStart, bucketEnd)
		}
		count := slotBuckets[bucket]
		table.Append([]string{rangeStr, fmt.Sprintf("%d", count)})
		total += count
	}
	table.SetFooter([]string{"Total", fmt.Sprintf("%d", total)})
	table.Render()

	return nil
}

func stemAnalysis(ctx *cli.Context) error {
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, true)
	defer db.Close()

	count := 0
	logged := time.Now()
	startTime := time.Now()
	logProgress := func() {
		count++
		if count%1000 == 0 && time.Since(logged) > 8*time.Second {
			log.Info("Inspecting database", "count", count, "elapsed", common.PrettyDuration(time.Since(startTime)))
			logged = time.Now()
		}
	}

	getGroup := func(slotNum *uint256.Int) *uint256.Int {
		// If slot number <= 64, return group 0
		if slotNum.Cmp(_uint64) <= 0 {
			return _uint0
		}

		group := new(uint256.Int).Sub(slotNum, _uint64)
		quotient := new(uint256.Int).Div(group, _uint256)
		remainder := new(uint256.Int).Mod(group, _uint256)
		// If there's any remainder, add 1 to round up to next group
		if remainder.Cmp(_uint0) > 0 {
			return new(uint256.Int).Add(quotient, _uint1)
		}
		return quotient
	}

	stemCount := 0
	slotHashCache := fastcache.New(1 * 1024 * 1024 * 1024)
	curGroup := make(map[uint256.Int]struct{})
	curAddr := common.Hash{}

	prefix := rawdb.SnapshotStoragePrefix
	it := db.NewIterator(prefix, nil)
	defer it.Release()
	for it.Next() {
		rawSlotKey := it.Key()

		if len(rawSlotKey) != len(prefix)+2*common.HashLength {
			continue
		}

		addrHash := common.BytesToHash(rawSlotKey[len(prefix) : len(prefix)+common.HashLength])
		if !bytes.Equal(addrHash[:], curAddr[:]) { // we are on a new address
			for range curGroup {
				stemCount++
			}

			curAddr = addrHash
			curGroup = make(map[uint256.Int]struct{})
		}

		slotHash := common.BytesToHash(rawSlotKey[len(prefix)+common.HashLength : len(prefix)+2*common.HashLength])

		var slot []byte
		slot = slotHashCache.Get(nil, slotHash[:])
		if len(slot) == 0 {
			slot = rawdb.ReadPreimage(db, slotHash)
			slotHashCache.Set(slotHash[:], slot)
		}

		uSlot := new(uint256.Int).SetBytes32(slot)
		group := getGroup(uSlot)
		if _, ok := curGroup[*group]; !ok {
			curGroup[*group] = struct{}{}
		}
		logProgress()
	}

	for range curGroup {
		stemCount++
	}

	log.Info("Stem count", "count", stemCount)

	return nil
}

// Meta slot is stored in the database as hash instead of the original slot
func slotExpiryAnalysisV2(ctx *cli.Context) error {
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, true)
	defer db.Close()

	if ctx.NArg() > 2 {
		return fmt.Errorf("max 2 argument: %v", ctx.Command.ArgsUsage)
	}

	var start uint64
	var periodLength uint64
	blockRange := uint64(219000) // 1 month worth of blocks
	switch ctx.NArg() {
	case 2:
		s, err := strconv.ParseUint(ctx.Args().First(), 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse period: %v", err)
		}
		start = s

		p, err := strconv.ParseUint(ctx.Args().Get(1), 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse period: %v", err)
		}
		periodLength = p
	case 1:
		s, err := strconv.ParseUint(ctx.Args().First(), 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse period: %v", err)
		}
		start = s
		periodLength = 1314000
	default:
		start = 17165429 // This is the starting block number of the node snapshot

		// 1314000 = About 6 months worth of blocks.
		// 6 months = 15768000s
		// number of blocks = 15768000 / 12s = 1314000
		// 12s is the average block time.
		periodLength = 1314000
	}

	count := 0
	logged := time.Now()
	startTime := time.Now()
	logProgress := func() {
		count++
		if count%1000 == 0 && time.Since(logged) > 8*time.Second {
			log.Info("Inspecting database", "count", count, "elapsed", common.PrettyDuration(time.Since(startTime)))
			logged = time.Now()
		}
	}

	slotBuckets := make(map[int]int)
	addBucket := func(bn uint64, buckets map[int]int) {
		if bn == 0 {
			buckets[0]++
			return
		}
		if bn < start {
			buckets[-1]++
			return
		}
		bucket := int((bn-start)/blockRange) + 1
		buckets[bucket]++
	}

	getGroup := func(slotNum *uint256.Int) *uint256.Int {
		// If slot number <= 64, return group 0
		if slotNum.Cmp(_uint64) <= 0 {
			return _uint0
		}

		group := new(uint256.Int).Sub(slotNum, _uint64)
		quotient := new(uint256.Int).Div(group, _uint256)
		remainder := new(uint256.Int).Mod(group, _uint256)
		// If there's any remainder, add 1 to round up to next group
		if remainder.Cmp(_uint0) > 0 {
			return new(uint256.Int).Add(quotient, _uint1)
		}
		return quotient
	}

	periodCount := make(map[verkle.StatePeriod]int)
	preimageCache := fastcache.New(10 * 1024 * 1024 * 1024)
	curGroup := make(map[uint256.Int]verkle.StatePeriod)
	curAddr := common.Hash{}
	it := db.NewIterator(rawdb.SnapshotStoragePrefix, nil)
	defer it.Release()
	for it.Next() {
		rawSlotKey := it.Key()

		if len(rawSlotKey) != len(rawdb.SnapshotStoragePrefix)+2*common.HashLength {
			continue
		}

		addrHash := common.BytesToHash(rawSlotKey[len(rawdb.SnapshotStoragePrefix) : len(rawdb.SnapshotStoragePrefix)+common.HashLength])
		if !bytes.Equal(addrHash[:], curAddr[:]) { // we are on a new address
			for _, period := range curGroup {
				periodCount[period]++
			}

			curAddr = addrHash
			curGroup = make(map[uint256.Int]verkle.StatePeriod)
		}

		slotHash := common.BytesToHash(rawSlotKey[len(rawdb.SnapshotStoragePrefix)+common.HashLength : len(rawdb.SnapshotStoragePrefix)+2*common.HashLength])
		slot := preimageCache.Get(nil, slotHash[:])
		if len(slot) == 0 {
			slot = rawdb.ReadPreimage(db, slotHash)
			preimageCache.Set(slotHash[:], slot)
		}

		slotBn := rawdb.ReadStorageSnapshotMeta(db, addrHash, slotHash)
		slotPeriod := getPeriod(slotBn, start, periodLength)
		group := getGroup(new(uint256.Int).SetBytes32(slot))
		if prev, ok := curGroup[*group]; !ok {
			curGroup[*group] = slotPeriod
		} else if slotPeriod > prev {
			curGroup[*group] = slotPeriod
		}
		addBucket(slotBn, slotBuckets)
		logProgress()
	}

	// Add the last group
	for _, period := range curGroup {
		periodCount[period]++
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Slot Period", "Stem Count"})
	total := 0
	// Create a sorted slice of periods
	var periods []verkle.StatePeriod
	for period := range periodCount {
		periods = append(periods, period)
	}
	slices.Sort(periods)

	for _, period := range periods {
		count := periodCount[period]
		table.Append([]string{fmt.Sprintf("%d", period), fmt.Sprintf("%d", count)})
		total += count
	}
	table.SetFooter([]string{"Total", fmt.Sprintf("%d", total)})
	table.Render()

	// Display slot in block range
	table = tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Block Range", "Slot Count"})
	total = 0

	// Get sorted bucket numbers
	var bucketNums []int
	for bucket := range slotBuckets {
		bucketNums = append(bucketNums, bucket)
	}
	slices.Sort(bucketNums)

	// Display results with proper block ranges
	for _, bucket := range bucketNums {
		var rangeStr string
		switch bucket {
		case -1:
			rangeStr = fmt.Sprintf("< %d", start)
		case 0:
			rangeStr = "Empty/Zero"
		default:
			bucketStart := start + uint64(bucket-1)*blockRange
			bucketEnd := bucketStart + blockRange - 1
			rangeStr = fmt.Sprintf("%d - %d", bucketStart, bucketEnd)
		}
		count := slotBuckets[bucket]
		table.Append([]string{rangeStr, fmt.Sprintf("%d", count)})
		total += count
	}
	table.SetFooter([]string{"Total", fmt.Sprintf("%d", total)})
	table.Render()

	return nil
}

func slotExpiryAnalysisV3(ctx *cli.Context) error {
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, true)
	defer db.Close()

	if ctx.NArg() > 2 {
		return fmt.Errorf("max 2 argument: %v", ctx.Command.ArgsUsage)
	}

	head := rawdb.ReadHeadHeader(db)
	headNumber := head.Number.Uint64()

	var start uint64
	var periodLength uint64
	switch ctx.NArg() {
	case 2:
		s, err := strconv.ParseUint(ctx.Args().First(), 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse period: %v", err)
		}
		start = s

		p, err := strconv.ParseUint(ctx.Args().Get(1), 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse period: %v", err)
		}
		periodLength = p
	case 1:
		s, err := strconv.ParseUint(ctx.Args().First(), 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse period: %v", err)
		}
		start = s
		periodLength = 1314000
	default:
		start = 17165429 // This is the starting block number of the node snapshot

		// 1314000 = About 6 months worth of blocks.
		// 6 months = 15768000s
		// number of blocks = 15768000 / 12s = 1314000
		// 12s is the average block time.
		periodLength = 1314000
	}

	count := 0
	logged := time.Now()
	startTime := time.Now()
	logProgress := func() {
		count++
		if count%1000 == 0 && time.Since(logged) > 8*time.Second {
			log.Info("Inspecting database", "count", count, "elapsed", common.PrettyDuration(time.Since(startTime)))
			logged = time.Now()
		}
	}

	blockRange := uint64(219000)
	slotBuckets := make(map[int]int)
	addBucket := func(bn uint64, buckets map[int]int) {
		if bn == 0 {
			buckets[0]++
			return
		}
		if bn < start {
			buckets[-1]++
			return
		}
		bucket := int((bn-start)/blockRange) + 1
		buckets[bucket]++
	}

	getGroup := func(slotNum *uint256.Int) *uint256.Int {
		// If slot number <= 64, return group 0
		if slotNum.Cmp(_uint64) <= 0 {
			return _uint0
		}

		group := new(uint256.Int).Sub(slotNum, _uint64)
		quotient := new(uint256.Int).Div(group, _uint256)
		remainder := new(uint256.Int).Mod(group, _uint256)
		// If there's any remainder, add 1 to round up to next group
		if remainder.Cmp(_uint0) > 0 {
			return new(uint256.Int).Add(quotient, _uint1)
		}
		return quotient
	}

	checkPoint := 100000000 // print every 100 million slots
	printResult := func(periodCount map[verkle.StatePeriod]int) {
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"Slot Period", "Stem Count"})
		total := 0
		// Create a sorted slice of periods
		periods := make([]verkle.StatePeriod, 0, len(periodCount))
		for period := range periodCount {
			periods = append(periods, period)
		}
		slices.Sort(periods)

		for _, period := range periods {
			count := periodCount[period]
			table.Append([]string{fmt.Sprintf("%d", period), fmt.Sprintf("%d", count)})
			total += count
		}
		table.SetFooter([]string{"Total", fmt.Sprintf("%d", total)})
		table.Render()

		// Display slot in block range
		table = tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"Block Range", "Slot Count"})
		total = 0

		// Get sorted bucket numbers
		var bucketNums []int
		for bucket := range slotBuckets {
			bucketNums = append(bucketNums, bucket)
		}
		slices.Sort(bucketNums)

		// Display results with proper block ranges
		for _, bucket := range bucketNums {
			var rangeStr string
			switch bucket {
			case -1:
				rangeStr = fmt.Sprintf("< %d", start)
			case 0:
				rangeStr = "Empty/Zero"
			default:
				bucketStart := start + uint64(bucket-1)*blockRange
				bucketEnd := bucketStart + blockRange - 1
				bucketEnd = min(bucketEnd, headNumber)
				rangeStr = fmt.Sprintf("%d - %d", bucketStart, bucketEnd)
			}
			count := slotBuckets[bucket]
			table.Append([]string{rangeStr, fmt.Sprintf("%d", count)})
			total += count
		}
		table.SetFooter([]string{"Total", fmt.Sprintf("%d", total)})
		table.Render()
	}

	buf := crypto.NewKeccakState()
	periodCount := make(map[verkle.StatePeriod]int)
	slotHashCache := fastcache.New(1 * 1024 * 1024 * 1024)
	curGroup := make(map[uint256.Int]verkle.StatePeriod)
	curAddr := common.Hash{}

	prefix := rawdb.SnapshotStorageMetaPrefix
	it := db.NewIterator(prefix, nil)
	defer it.Release()
	for it.Next() {
		rawSlotKey := it.Key()
		rawBlockNum := it.Value()

		if len(rawSlotKey) != len(prefix)+2*common.HashLength {
			continue
		}

		addrHash := common.BytesToHash(rawSlotKey[len(prefix) : len(prefix)+common.HashLength])
		if !bytes.Equal(addrHash[:], curAddr[:]) { // we are on a new address
			for _, period := range curGroup {
				periodCount[period]++
			}

			curAddr = addrHash
			curGroup = make(map[uint256.Int]verkle.StatePeriod)
		}

		slot := rawSlotKey[len(prefix)+common.HashLength : len(prefix)+2*common.HashLength]
		slotBn := binary.BigEndian.Uint64(rawBlockNum)
		slotPeriod := getPeriod(slotBn, start, periodLength)
		uSlot := new(uint256.Int).SetBytes32(slot)

		var slotHash common.Hash
		rawHash := slotHashCache.Get(nil, slot)
		if len(rawHash) == 0 {
			slotHash = crypto.HashData(buf, slot)
			slotHashCache.Set(slot, slotHash[:])
		} else {
			slotHash = common.BytesToHash(rawHash)
		}

		// Check if the main storge snapshot has this slot
		has := rawdb.HasStorageSnapshot(db, addrHash, slotHash)
		if !has {
			logProgress()
			if count%checkPoint == 0 {
				log.Info("Checkpoint reached", "count", count, "addrHash", addrHash.Hex(), "slot", uSlot.String())
				printResult(periodCount)
			}
			continue
		}

		group := getGroup(uSlot)
		if prev, ok := curGroup[*group]; !ok {
			curGroup[*group] = slotPeriod
		} else if slotPeriod > prev {
			curGroup[*group] = slotPeriod
		}
		addBucket(slotBn, slotBuckets)
		logProgress()

		if count%checkPoint == 0 {
			log.Info("Checkpoint reached", "count", count, "addrHash", addrHash.Hex(), "slot", uSlot.String())
			printResult(periodCount)
		}
	}

	// Add the last group
	for _, period := range curGroup {
		periodCount[period]++
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Slot Period", "Stem Count"})
	total := 0
	// Create a sorted slice of periods
	var periods []verkle.StatePeriod
	for period := range periodCount {
		periods = append(periods, period)
	}
	slices.Sort(periods)

	for _, period := range periods {
		count := periodCount[period]
		table.Append([]string{fmt.Sprintf("%d", period), fmt.Sprintf("%d", count)})
		total += count
	}
	table.SetFooter([]string{"Total", fmt.Sprintf("%d", total)})
	table.Render()

	printResult(periodCount)

	return nil
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

	triedb := utils.MakeTrieDatabase(ctx, db, false, true, false)
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
	table := tablewriter.NewWriter(os.Stdout)
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

	triedb := utils.MakeTrieDatabase(ctx, db, false, false, false)
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

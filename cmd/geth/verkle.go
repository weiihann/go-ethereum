// Copyright 2022 The go-ethereum Authors
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
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"os"
	"runtime"
	"time"

	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/internal/flags"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	tutils "github.com/ethereum/go-ethereum/trie/utils"
	"github.com/gballet/go-verkle"
	"github.com/holiman/uint256"
	cli "github.com/urfave/cli/v2"
)

var (
	zero [32]byte

	verkleCommand = &cli.Command{
		Name:        "verkle",
		Usage:       "A set of experimental verkle tree management commands",
		Description: "",
		Subcommands: []*cli.Command{
			{
				Name:      "to-verkle",
				Usage:     "use the snapshot to compute a translation of a MPT into a verkle tree",
				ArgsUsage: "<root>",
				Action:    convertToVerkle,
				Flags:     flags.Merge([]cli.Flag{}, utils.NetworkFlags, utils.DatabasePathFlags, []cli.Flag{utils.StateExpiryFlag}),
				Description: `
geth verkle to-verkle <state-root>
This command takes a snapshot and inserts its values in a fresh verkle tree.

The argument is interpreted as the root hash. If none is provided, the latest
block is used.
 `,
			},
			{
				Name:      "verify",
				Usage:     "verify the conversion of a MPT into a verkle tree",
				ArgsUsage: "<root>",
				Action:    verifyVerkle,
				Flags:     flags.Merge(utils.NetworkFlags, utils.DatabasePathFlags),
				Description: `
geth verkle verify <state-root>
This command takes a root commitment and attempts to rebuild the tree.
 `,
			},
			{
				Name:      "dump",
				Usage:     "Dump a verkle tree to a DOT file",
				ArgsUsage: "<root> <key1> [<key 2> ...]",
				Action:    expandVerkle,
				Flags:     flags.Merge(utils.NetworkFlags, utils.DatabasePathFlags),
				Description: `
geth verkle dump <state-root> <key 1> [<key 2> ...]
This command will produce a dot file representing the tree, rooted at <root>.
in which key1, key2, ... are expanded.
 `,
			},
		},
	}
)

func convertToVerkle(ctx *cli.Context) error {
	stack, _ := makeConfigNode(ctx)
	enableStateExpiry := false
	defer stack.Close()

	if ctx.IsSet(utils.StateExpiryFlag.Name) {
		enableStateExpiry = ctx.Bool(utils.StateExpiryFlag.Name)
	}
	log.Info("State expiry", "enabled", enableStateExpiry)

	chaindb := utils.MakeChainDatabase(ctx, stack, false)
	if chaindb == nil {
		return errors.New("nil chaindb")
	}
	headBlock := rawdb.ReadHeadBlock(chaindb)
	if headBlock == nil {
		log.Error("Failed to load head block")
		return errors.New("no head block")
	}
	if ctx.NArg() > 1 {
		log.Error("Too many arguments given")
		return errors.New("too many arguments")
	}
	var (
		root common.Hash
		err  error
	)

	root = headBlock.Root()
	log.Info("Start traversing the state", "root", root, "number", headBlock.NumberU64())

	var (
		accounts   int
		slots      int
		lastReport time.Time
		start      = time.Now()
		vRoot      = verkle.New().(*verkle.InternalNode)
	)

	latestBlockNum := rawdb.ReadHeadBlock(chaindb).Number()
	epochPeriod := big.NewInt(1_051_200)

	if epochPeriod.Cmp(latestBlockNum) == 1 {
		log.Error("Epoch period is larger than the latest block number", "period", epochPeriod, "blockNum", latestBlockNum)
	}
	epoch := verkle.StateEpoch(latestBlockNum.Div(latestBlockNum, epochPeriod).Uint64())
	log.Info("Epoch", "epoch", epoch)

	saveverkle := func(path []byte, node verkle.VerkleNode) {
		var key []byte
		if ln, ok := node.(*verkle.ExpiryLeafNode); ok {
			ln.UpdateCurrEpoch(epoch)
		}
		node.Commit()
		s, err := node.Serialize()
		if err != nil {
			panic(err)
		}
		if enableStateExpiry {
			key = rawdb.VktTrieNodeKey(path)
		} else {
			key = rawdb.VktTrieNoExpiryNodeKey(path)
		}
		if err := chaindb.Put(key, s); err != nil {
			panic(err)
		}
	}

	nodeResolver := func(path []byte) ([]byte, error) {
		if enableStateExpiry {
			return chaindb.Get(rawdb.VktTrieNodeKey(path))
		} else {
			return chaindb.Get(rawdb.VktTrieNoExpiryNodeKey(path))
		}
	}

	if enableStateExpiry {
		err = deleteVerkleNodes(rawdb.VktNodePrefix, chaindb)
	} else {
		err = deleteVerkleNodes(rawdb.VktNodeNoExpiryPrefix, chaindb)
	}
	if err != nil {
		return err
	}

	snaptree, err := snapshot.New(snapshot.Config{CacheSize: 256}, chaindb, trie.NewDatabase(chaindb), root)
	if err != nil {
		return err
	}
	accIt, err := snaptree.AccountIterator(root, common.Hash{})
	if err != nil {
		return err
	}
	defer accIt.Release()

	// root.FlushAtDepth(depth, saveverkle)

	// Process all accounts sequentially
	for accIt.Next() {
		accounts += 1
		acc, err := types.FullAccount(accIt.Account())
		if err != nil {
			log.Error("Invalid account encountered during traversal", "error", err)
			return err
		}

		// Store the basic account data
		var (
			nonce, balance, version, size [32]byte
			newValues                     = make([][]byte, 256)
			accEpoch                      verkle.StateEpoch
		)
		newValues[0] = version[:]
		newValues[1] = balance[:]
		newValues[2] = nonce[:]
		newValues[4] = version[:] // memory-saving trick: by default, an account has 0 size
		binary.LittleEndian.PutUint64(nonce[:8], acc.Nonce)
		for i, b := range acc.Balance.Bytes() {
			balance[len(acc.Balance.Bytes())-1-i] = b
		}

		addr := rawdb.ReadPreimage(chaindb, accIt.Hash())
		if addr == nil {
			return fmt.Errorf("could not find preimage for address %x %v %v", accIt.Hash(), acc, accIt.Error())
		}
		addrPoint := tutils.EvaluateAddressPoint(addr)
		stem := tutils.GetTreeKeyVersionWithEvaluatedAddress(addrPoint)

		if enableStateExpiry {
			accEpoch, err = readAccountEpochFromDb(chaindb, accIt.Hash())
			if err != nil {
				log.Error("Failed to read account epoch from database", "error", err)
			}
		}

		// Store the account code if present
		if !bytes.Equal(acc.CodeHash, types.EmptyRootHash[:]) {
			code := rawdb.ReadCode(chaindb, common.BytesToHash(acc.CodeHash))
			chunks := trie.ChunkifyCode(code)

			for i := 0; i < 128 && i < len(chunks)/32; i++ {
				newValues[128+i] = chunks[32*i : 32*(i+1)]
			}

			for i := 128; i < len(chunks)/32; {
				values := make([][]byte, 256)
				chunkkey := tutils.GetTreeKeyCodeChunkWithEvaluatedAddress(addrPoint, uint256.NewInt(uint64(i)))
				j := i
				for ; (j-i) < 256 && j < len(chunks)/32; j++ {
					values[(j-128)%256] = chunks[32*j : 32*(j+1)]
				}
				i = j

				// Otherwise, store the previous group in the tree with a
				// stem insertion.
				if enableStateExpiry {
					vRoot.InsertStemWithEpoch(chunkkey[:31], chunkkey[31], values, nodeResolver, accEpoch)
				} else {
					vRoot.InsertStem(chunkkey[:31], values, nodeResolver)
				}
			}

			// Write the code size in the account header group
			binary.LittleEndian.PutUint64(size[:8], uint64(len(code)))
		}
		newValues[3] = acc.CodeHash[:]
		newValues[4] = size[:]

		// Save every slot into the tree
		if acc.Root != types.EmptyRootHash {
			var translatedStorage = map[string][][]byte{}
			var translatedEpoch = map[string]verkle.StateEpoch{}

			storageIt, err := snaptree.StorageIterator(root, accIt.Hash(), common.Hash{})
			if err != nil {
				log.Error("Failed to open storage trie", "root", acc.Root, "error", err)
				return err
			}
			for storageIt.Next() {
				slots += 1
				// The value is RLP-encoded, decode it
				var (
					value        []byte   // slot value after RLP decoding
					safeValue    [32]byte // 32-byte aligned value
					storageEpoch verkle.StateEpoch
				)
				if err := rlp.DecodeBytes(storageIt.Slot(), &value); err != nil {
					return fmt.Errorf("error decoding bytes %x: %w", storageIt.Slot(), err)
				}
				copy(safeValue[32-len(value):], value)

				if enableStateExpiry {
					storageEpoch, err = readStorageEpochFromDb(chaindb, accIt.Hash(), storageIt.Hash())
					if err != nil {
						log.Error("Failed to read storage epoch from database", "error", err)
					}
				}

				// TODO: read from preimage file
				slotnr := rawdb.ReadPreimage(chaindb, storageIt.Hash())
				if slotnr == nil {
					return fmt.Errorf("could not find preimage for slot %x", storageIt.Hash())
				}

				// if the slot belongs to the header group, store it there - and skip
				// calculating the slot key.
				slotnrbig := uint256.NewInt(0).SetBytes(slotnr)
				if slotnrbig.Cmp(uint256.NewInt(64)) < 0 {
					newValues[64+slotnr[31]] = safeValue[:]
					continue
				}

				// Slot not in the header group, get its tree key
				slotkey := tutils.GetTreeKeyStorageSlotWithEvaluatedAddress(addrPoint, slotnr)

				// Create the group if need be
				values := translatedStorage[string(slotkey[:31])]
				if values == nil {
					values = make([][]byte, 256)
				}

				// Store value in group
				values[slotkey[31]] = safeValue[:]
				translatedStorage[string(slotkey[:31])] = values
				if enableStateExpiry {
					translatedEpoch[string(slotkey[:31])] = storageEpoch
				}

				// Dump the stuff to disk if we ran out of space
				var mem runtime.MemStats
				runtime.ReadMemStats(&mem)
				if mem.Alloc > 25*1024*1024*1024 {
					fmt.Println("Memory usage exceeded threshold, calling mitigation function")
					for s, vs := range translatedStorage {
						var k [31]byte
						copy(k[:], []byte(s))
						// reminder that InsertStem will merge leaves
						// if they exist.
						if enableStateExpiry {
							epoch, ok := translatedEpoch[s]
							if !ok {
								log.Error("Failed to find epoch for storage slot", "slot", s)
							}
							vRoot.InsertStemWithEpoch(k[:31], s[31], vs, nodeResolver, epoch)
						} else {
							vRoot.InsertStem(k[:31], vs, nodeResolver)
						}
					}
					translatedStorage = make(map[string][][]byte)
					vRoot.FlushAtDepth(2, saveverkle)
				}
			}
			for s, vs := range translatedStorage {
				var k [31]byte
				copy(k[:], []byte(s))
				epoch, ok := translatedEpoch[s]
				if !ok {
					log.Error("Failed to find epoch for storage slot", "slot", s)
				}
				if enableStateExpiry {
					vRoot.InsertStemWithEpoch(k[:31], s[31], vs, nodeResolver, epoch)
				} else {
					vRoot.InsertStem(k[:31], vs, nodeResolver)
				}
			}
			storageIt.Release()
			if storageIt.Error() != nil {
				log.Error("Failed to traverse storage trie", "root", acc.Root, "error", storageIt.Error())
				return storageIt.Error()
			}
		}
		// Finish with storing the complete account header group inside the tree.
		if enableStateExpiry {
			vRoot.InsertStemWithEpoch(stem[:31], stem[31], newValues, nodeResolver, accEpoch)
		} else {
			vRoot.InsertStem(stem[:31], newValues, nodeResolver)
		}

		if time.Since(lastReport) > time.Second*8 {
			log.Info("Traversing state", "accounts", accounts, "slots", slots, "elapsed", common.PrettyDuration(time.Since(start)))
			lastReport = time.Now()
		}

		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		if mem.Alloc > 25*1024*1024*1024 {
			fmt.Println("Memory usage exceeded threshold, calling mitigation function")
			vRoot.FlushAtDepth(2, saveverkle)
		}
	}
	if accIt.Error() != nil {
		log.Error("Failed to compute commitment", "root", root, "error", accIt.Error())
		return accIt.Error()
	}
	log.Info("Wrote all leaves", "accounts", accounts, "slots", slots, "elapsed", common.PrettyDuration(time.Since(start)))

	vRoot.Commit()
	vRoot.Flush(saveverkle)

	log.Info("Conversion complete", "root commitment", fmt.Sprintf("%x", vRoot.Commit().Bytes()), "accounts", accounts, "elapsed", common.PrettyDuration(time.Since(start)))
	return nil
}

// Create a function to iterate through the database with vkt prefix
func deleteVerkleNodes(prefix []byte, chaindb ethdb.Database) error {
	it := chaindb.NewIterator(prefix, nil)
	if it.Error() != nil {
		log.Error("Failed to initialize iterator", "err", it.Error())
		return it.Error()
	}
	defer it.Release()

	var (
		count  int64
		start  = time.Now()
		logged = time.Now()
		batch  = chaindb.NewBatch()
	)

	for it.Next() {
		count++
		if count%1000 == 0 && time.Since(logged) > 8*time.Second {
			log.Info("Deleting verkle trie nodes from database", "count", count, "elapsed", common.PrettyDuration(time.Since(start)))
			logged = time.Now()
		}

		batch.Delete(it.Key())
		if batch.ValueSize() > ethdb.IdealBatchSize {
			if err := batch.Write(); err != nil {
				log.Error("Failed to write batch", "err", err)
				return err
			}
			batch.Reset()
		}
	}

	log.Info("Complete deleting verkle trie nodes from database", "count", count, "elapsed", common.PrettyDuration(time.Since(start)))
	return nil
}

func readAccountEpochFromDb(chaindb ethdb.Database, addr common.Hash) (verkle.StateEpoch, error) {
	epoch, err := chaindb.Get(rawdb.AccountSnapshotKeyMeta(addr))
	if err != nil {
		return 0, err
	}
	return verkle.BytesToEpoch(epoch), nil
}

func readStorageEpochFromDb(chaindb ethdb.Database, addr common.Hash, slot common.Hash) (verkle.StateEpoch, error) {
	epoch, err := chaindb.Get(rawdb.StorageSnapshotKeyMeta(addr, slot))
	if err != nil {
		return 0, err
	}
	return verkle.BytesToEpoch(epoch), nil
}

// recurse into each child to ensure they can be loaded from the db. The tree isn't rebuilt
// (only its nodes are loaded) so there is no need to flush them, the garbage collector should
// take care of that for us.
func checkChildren(root verkle.VerkleNode, resolver verkle.NodeResolverFn) error {
	switch node := root.(type) {
	case *verkle.InternalNode:
		for i, child := range node.Children() {
			childC := child.Commitment().Bytes()

			childS, err := resolver(childC[:])
			if bytes.Equal(childC[:], zero[:]) {
				continue
			}
			if err != nil {
				return fmt.Errorf("could not find child %x in db: %w", childC, err)
			}
			// depth is set to 0, the tree isn't rebuilt so it's not a problem
			childN, err := verkle.ParseNode(childS, 0)
			if err != nil {
				return fmt.Errorf("decode error child %x in db: %w", child.Commitment().Bytes(), err)
			}
			if err := checkChildren(childN, resolver); err != nil {
				return fmt.Errorf("%x%w", i, err) // write the path to the erroring node
			}
		}
	case *verkle.LeafNode:
		// sanity check: ensure at least one value is non-zero

		for i := 0; i < verkle.NodeWidth; i++ {
			if len(node.Value(i)) != 0 {
				return nil
			}
		}
		return errors.New("both balance and nonce are 0")
	case verkle.Empty:
		// nothing to do
	default:
		return fmt.Errorf("unsupported type encountered %v", root)
	}

	return nil
}

func verifyVerkle(ctx *cli.Context) error {
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	chaindb := utils.MakeChainDatabase(ctx, stack, true)
	headBlock := rawdb.ReadHeadBlock(chaindb)
	if headBlock == nil {
		log.Error("Failed to load head block")
		return errors.New("no head block")
	}
	if ctx.NArg() > 1 {
		log.Error("Too many arguments given")
		return errors.New("too many arguments")
	}
	var (
		rootC common.Hash
		err   error
	)
	if ctx.NArg() == 1 {
		rootC, err = parseRoot(ctx.Args().First())
		if err != nil {
			log.Error("Failed to resolve state root", "error", err)
			return err
		}
		log.Info("Rebuilding the tree", "root", rootC)
	} else {
		rootC = headBlock.Root()
		log.Info("Rebuilding the tree", "root", rootC, "number", headBlock.NumberU64())
	}

	serializedRoot, err := chaindb.Get(rootC[:])
	if err != nil {
		return err
	}
	root, err := verkle.ParseNode(serializedRoot, 0)
	if err != nil {
		return err
	}

	if err := checkChildren(root, chaindb.Get); err != nil {
		log.Error("Could not rebuild the tree from the database", "err", err)
		return err
	}

	log.Info("Tree was rebuilt from the database")
	return nil
}

func expandVerkle(ctx *cli.Context) error {
	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	chaindb := utils.MakeChainDatabase(ctx, stack, true)
	var (
		rootC   common.Hash
		keylist [][]byte
		err     error
	)
	if ctx.NArg() >= 2 {
		rootC, err = parseRoot(ctx.Args().First())
		if err != nil {
			log.Error("Failed to resolve state root", "error", err)
			return err
		}
		keylist = make([][]byte, 0, ctx.Args().Len()-1)
		args := ctx.Args().Slice()
		for i := range args[1:] {
			key, err := hex.DecodeString(args[i+1])
			log.Info("decoded key", "arg", args[i+1], "key", key)
			if err != nil {
				return fmt.Errorf("error decoding key #%d: %w", i+1, err)
			}
			keylist = append(keylist, key)
		}
		log.Info("Rebuilding the tree", "root", rootC)
	} else {
		return fmt.Errorf("usage: %s root key1 [key 2...]", ctx.App.Name)
	}

	serializedRoot, err := chaindb.Get(rootC[:])
	if err != nil {
		return err
	}
	root, err := verkle.ParseNode(serializedRoot, 0)
	if err != nil {
		return err
	}

	for i, key := range keylist {
		log.Info("Reading key", "index", i, "key", keylist[0])
		root.Get(key, chaindb.Get)
	}

	if err := os.WriteFile("dump.dot", []byte(verkle.ToDot(root)), 0o600); err != nil {
		log.Error("Failed to dump file", "err", err)
	} else {
		log.Info("Tree was dumped to file", "file", "dump.dot")
	}
	return nil
}

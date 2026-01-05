package main

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/holiman/uint256"
)

// Database key size constants matching the tracer
var (
	accountKeySize            = int64(len(rawdb.SnapshotAccountPrefix) + common.HashLength)
	storageKeySize            = int64(len(rawdb.SnapshotStoragePrefix) + common.HashLength*2)
	accountTrienodePrefixSize = int64(len(rawdb.TrieNodeAccountPrefix))
	storageTrienodePrefixSize = int64(len(rawdb.TrieNodeStoragePrefix) + common.HashLength)
	codeKeySize               = int64(len(rawdb.CodePrefix) + common.HashLength)
)

// RLP types matching the tracer output

// StateUpdateRLP is the RLP-serializable version of tracing.StateUpdate.
type StateUpdateRLP struct {
	OriginRoot  common.Hash
	Root        common.Hash
	BlockNumber uint64

	AccountChanges []AccountChangeEntry
	StorageChanges []StorageChangeEntry
	CodeChanges    []CodeChangeEntry
	TrieChanges    []TrieChangeEntry
}

// AccountChangeEntry represents a single account change for RLP encoding.
type AccountChangeEntry struct {
	Address common.Address
	Prev    *StateAccountRLP `rlp:"nil"`
	New     *StateAccountRLP `rlp:"nil"`
}

// StateAccountRLP is an RLP-compatible version of types.StateAccount.
type StateAccountRLP struct {
	Nonce    uint64
	Balance  *uint256.Int
	Root     common.Hash
	CodeHash []byte
}

// StorageChangeEntry represents storage changes for a single address.
type StorageChangeEntry struct {
	Address common.Address
	Slots   []SlotChange
}

// SlotChange represents a single storage slot change.
type SlotChange struct {
	Key  common.Hash
	Prev common.Hash
	New  common.Hash
}

// CodeChangeEntry represents a code change for RLP encoding.
type CodeChangeEntry struct {
	Address common.Address
	Prev    *ContractCodeRLP `rlp:"nil"`
	New     *ContractCodeRLP `rlp:"nil"`
}

// ContractCodeRLP is an RLP-compatible version of tracing.ContractCode.
type ContractCodeRLP struct {
	Hash   common.Hash
	Code   []byte
	Exists bool
}

// TrieChangeEntry represents trie changes for a single owner.
type TrieChangeEntry struct {
	Owner common.Hash
	Nodes []TrieNodeEntry
}

// TrieNodeEntry represents a single trie node change.
type TrieNodeEntry struct {
	Path string
	Prev *TrieNodeRLP `rlp:"nil"`
	New  *TrieNodeRLP `rlp:"nil"`
}

// TrieNodeRLP is an RLP-compatible version of trienode.Node.
type TrieNodeRLP struct {
	Blob []byte
	Hash common.Hash
}

// StateDelta contains the calculated deltas for a block
type StateDelta struct {
	BlockNumber uint64
	Root        common.Hash
	ParentRoot  common.Hash

	// Account deltas
	AccountsDelta     int64
	AccountBytesDelta int64

	// Storage deltas
	StoragesDelta     int64
	StorageBytesDelta int64

	// Trie node deltas
	AccountTrienodesDelta     int64
	AccountTrienodeBytesDelta int64
	StorageTrienodesDelta     int64
	StorageTrienodeBytesDelta int64

	// Code deltas
	CodesDelta     int64
	CodeBytesDelta int64

	// Depth statistics (delta = created - deleted)
	AccountDepthDelta [65]int64
	StorageDepthDelta [65]int64
}

func main() {
	// Parse command line flags
	inputDir := flag.String("input", "", "Input directory containing .rlp.gz batch files")
	outputPath := flag.String("output", "statesize.csv", "Output CSV file path")
	depthOutputPath := flag.String("depth-output", "statesize_depth.csv", "Output CSV file path for depth statistics")
	flag.Parse()

	if *inputDir == "" {
		log.Crit("Input directory is required (-input)")
	}

	// Setup logging
	log.SetDefault(log.NewLogger(log.NewTerminalHandlerWithLevel(os.Stderr, log.LevelInfo, true)))

	// Find all batch files
	batchFiles, err := findBatchFiles(*inputDir)
	if err != nil {
		log.Crit("Failed to find batch files", "err", err)
	}
	if len(batchFiles) == 0 {
		log.Crit("No batch files found in directory", "dir", *inputDir)
	}
	log.Info("Found batch files", "count", len(batchFiles))

	// Open CSV files
	statFile, err := os.Create(*outputPath)
	if err != nil {
		log.Crit("Failed to create output file", "err", err)
	}
	defer statFile.Close()

	depthFile, err := os.Create(*depthOutputPath)
	if err != nil {
		log.Crit("Failed to create depth output file", "err", err)
	}
	defer depthFile.Close()

	statWriter := csv.NewWriter(statFile)
	defer statWriter.Flush()

	depthWriter := csv.NewWriter(depthFile)
	defer depthWriter.Flush()

	// Write headers
	if err := statWriter.Write(statHeaders()); err != nil {
		log.Crit("Failed to write stat headers", "err", err)
	}
	if err := depthWriter.Write(depthHeaders()); err != nil {
		log.Crit("Failed to write depth headers", "err", err)
	}

	// Process each batch file
	totalBlocks := uint64(0)
	for _, batchFile := range batchFiles {
		log.Info("Processing batch file", "file", filepath.Base(batchFile))

		processed, err := processBatchFile(batchFile, statWriter, depthWriter)
		if err != nil {
			log.Error("Failed to process batch file", "file", batchFile, "err", err)
			continue
		}
		totalBlocks += processed

		// Flush after each batch
		statWriter.Flush()
		depthWriter.Flush()
	}

	log.Info("Done", "totalBlocks", totalBlocks)
}

// findBatchFiles finds all .rlp.gz files in the directory and returns them sorted by start block.
func findBatchFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if filepath.Ext(entry.Name()) == ".gz" {
			files = append(files, filepath.Join(dir, entry.Name()))
		}
	}

	// Sort files by start block number (numeric sort)
	sort.Slice(files, func(i, j int) bool {
		return parseStartBlock(files[i]) < parseStartBlock(files[j])
	})
	return files, nil
}

// parseStartBlock extracts the start block number from a filename like "0_1999.rlp.gz".
func parseStartBlock(path string) uint64 {
	name := filepath.Base(path)
	// Parse the first number before the underscore
	for i, c := range name {
		if c == '_' {
			if num, err := strconv.ParseUint(name[:i], 10, 64); err == nil {
				return num
			}
			break
		}
	}
	return 0
}

// processBatchFile reads a single batch file and writes deltas to CSV.
func processBatchFile(filename string, statWriter, depthWriter *csv.Writer) (uint64, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return 0, err
	}
	defer gzReader.Close()

	bufReader := bufio.NewReaderSize(gzReader, 256*1024)
	var processed uint64

	for {
		// Read 4-byte length prefix
		var lenBuf [4]byte
		if _, err := io.ReadFull(bufReader, lenBuf[:]); err != nil {
			if err == io.EOF {
				break
			}
			return processed, err
		}
		dataLen := binary.BigEndian.Uint32(lenBuf[:])

		// Read the RLP data
		data := make([]byte, dataLen)
		if _, err := io.ReadFull(bufReader, data); err != nil {
			return processed, err
		}

		// Decode RLP
		var update StateUpdateRLP
		if err := rlp.DecodeBytes(data, &update); err != nil {
			return processed, fmt.Errorf("failed to decode RLP: %w", err)
		}

		// Calculate deltas
		delta := calculateDelta(&update)

		// Write to CSV files
		if err := statWriter.Write(deltaToStatRow(delta)); err != nil {
			return processed, err
		}
		if err := depthWriter.Write(deltaToDepthRow(delta)); err != nil {
			return processed, err
		}

		processed++

		// Log progress periodically
		if processed%500 == 0 {
			log.Info("Processing progress", "blocks", processed, "lastBlock", update.BlockNumber)
		}
	}

	return processed, nil
}

func statHeaders() []string {
	return []string{
		"block_number",
		"root",
		"parent_root",
		"accounts_delta",
		"account_bytes_delta",
		"storages_delta",
		"storage_bytes_delta",
		"account_trienodes_delta",
		"account_trienode_bytes_delta",
		"storage_trienodes_delta",
		"storage_trienode_bytes_delta",
		"codes_delta",
		"code_bytes_delta",
	}
}

func depthHeaders() []string {
	headers := []string{"block_number", "root", "parent_root"}
	for i := 0; i <= 64; i++ {
		headers = append(headers, fmt.Sprintf("account_depth_delta_%d", i))
	}
	for i := 0; i <= 64; i++ {
		headers = append(headers, fmt.Sprintf("storage_depth_delta_%d", i))
	}
	return headers
}

func deltaToStatRow(d *StateDelta) []string {
	return []string{
		strconv.FormatUint(d.BlockNumber, 10),
		d.Root.Hex(),
		d.ParentRoot.Hex(),
		strconv.FormatInt(d.AccountsDelta, 10),
		strconv.FormatInt(d.AccountBytesDelta, 10),
		strconv.FormatInt(d.StoragesDelta, 10),
		strconv.FormatInt(d.StorageBytesDelta, 10),
		strconv.FormatInt(d.AccountTrienodesDelta, 10),
		strconv.FormatInt(d.AccountTrienodeBytesDelta, 10),
		strconv.FormatInt(d.StorageTrienodesDelta, 10),
		strconv.FormatInt(d.StorageTrienodeBytesDelta, 10),
		strconv.FormatInt(d.CodesDelta, 10),
		strconv.FormatInt(d.CodeBytesDelta, 10),
	}
}

func deltaToDepthRow(d *StateDelta) []string {
	row := []string{
		strconv.FormatUint(d.BlockNumber, 10),
		d.Root.Hex(),
		d.ParentRoot.Hex(),
	}
	for i := 0; i <= 64; i++ {
		row = append(row, strconv.FormatInt(d.AccountDepthDelta[i], 10))
	}
	for i := 0; i <= 64; i++ {
		row = append(row, strconv.FormatInt(d.StorageDepthDelta[i], 10))
	}
	return row
}

func calculateDelta(update *StateUpdateRLP) *StateDelta {
	delta := &StateDelta{
		BlockNumber: update.BlockNumber,
		Root:        update.Root,
		ParentRoot:  update.OriginRoot,
	}

	// Calculate account deltas
	for _, change := range update.AccountChanges {
		prevLen := accountSize(change.Prev)
		newLen := accountSize(change.New)

		switch {
		case prevLen > 0 && newLen == 0:
			delta.AccountsDelta--
			delta.AccountBytesDelta -= accountKeySize + int64(prevLen)
		case prevLen == 0 && newLen > 0:
			delta.AccountsDelta++
			delta.AccountBytesDelta += accountKeySize + int64(newLen)
		default:
			delta.AccountBytesDelta += int64(newLen - prevLen)
		}
	}

	// Calculate storage deltas
	for _, entry := range update.StorageChanges {
		for _, change := range entry.Slots {
			prevLen := storageValueSize(change.Prev)
			newLen := storageValueSize(change.New)

			switch {
			case prevLen > 0 && newLen == 0:
				delta.StoragesDelta--
				delta.StorageBytesDelta -= storageKeySize + int64(prevLen)
			case prevLen == 0 && newLen > 0:
				delta.StoragesDelta++
				delta.StorageBytesDelta += storageKeySize + int64(newLen)
			default:
				delta.StorageBytesDelta += int64(newLen - prevLen)
			}
		}
	}

	// Calculate trie node deltas and depth statistics
	for _, entry := range update.TrieChanges {
		isAccount := entry.Owner == (common.Hash{})
		var keyPrefix int64
		if isAccount {
			keyPrefix = accountTrienodePrefixSize
		} else {
			keyPrefix = storageTrienodePrefixSize
		}

		// Convert to map for depth calculation
		nodesMap := make(map[string]*TrieNodeEntry, len(entry.Nodes))
		for i := range entry.Nodes {
			nodesMap[entry.Nodes[i].Path] = &entry.Nodes[i]
		}

		// Calculate depth deltas for all nodes
		depthDeltas := calculateDepthDeltas(nodesMap)

		for _, node := range entry.Nodes {
			var prevLen, newLen int
			if node.Prev != nil {
				prevLen = len(node.Prev.Blob)
			}
			if node.New != nil {
				newLen = len(node.New.Blob)
			}
			keySize := keyPrefix + int64(len(node.Path))

			switch {
			case prevLen > 0 && newLen == 0:
				if isAccount {
					delta.AccountTrienodesDelta--
					delta.AccountTrienodeBytesDelta -= keySize + int64(prevLen)
				} else {
					delta.StorageTrienodesDelta--
					delta.StorageTrienodeBytesDelta -= keySize + int64(prevLen)
				}
			case prevLen == 0 && newLen > 0:
				if isAccount {
					delta.AccountTrienodesDelta++
					delta.AccountTrienodeBytesDelta += keySize + int64(newLen)
				} else {
					delta.StorageTrienodesDelta++
					delta.StorageTrienodeBytesDelta += keySize + int64(newLen)
				}
			default:
				if isAccount {
					delta.AccountTrienodeBytesDelta += int64(newLen - prevLen)
				} else {
					delta.StorageTrienodeBytesDelta += int64(newLen - prevLen)
				}
			}
		}

		// Accumulate depth deltas
		if isAccount {
			for i := range 65 {
				delta.AccountDepthDelta[i] += depthDeltas[i]
			}
		} else {
			for i := range 65 {
				delta.StorageDepthDelta[i] += depthDeltas[i]
			}
		}
	}

	// Calculate code deltas
	codeExists := make(map[common.Hash]struct{}, len(update.CodeChanges))
	for _, change := range update.CodeChanges {
		if change.New == nil {
			continue
		}
		if _, ok := codeExists[change.New.Hash]; ok || change.New.Exists {
			continue
		}
		delta.CodesDelta++
		delta.CodeBytesDelta += codeKeySize + int64(len(change.New.Code))
		codeExists[change.New.Hash] = struct{}{}
	}

	return delta
}

func accountSize(acct *StateAccountRLP) int {
	if acct == nil {
		return 0
	}
	balance := acct.Balance
	if balance == nil {
		balance = new(uint256.Int)
	}
	stateAcct := types.StateAccount{
		Nonce:    acct.Nonce,
		Balance:  balance,
		Root:     acct.Root,
		CodeHash: acct.CodeHash,
	}
	return len(types.SlimAccountRLP(stateAcct))
}

func storageValueSize(val common.Hash) int {
	if val == (common.Hash{}) {
		return 0
	}
	// Use actual RLP encoding like go-ethereum does
	blob, _ := rlp.EncodeToBytes(common.TrimLeftZeroes(val[:]))
	return len(blob)
}

// calculateDepthDeltas calculates the depth of each node in the trie
// and returns the delta (created - deleted) at each depth.
func calculateDepthDeltas(nodes map[string]*TrieNodeEntry) (deltas [65]int64) {
	if len(nodes) == 0 {
		return
	}

	// Collect and sort paths
	paths := make([]string, 0, len(nodes))
	for path := range nodes {
		paths = append(paths, path)
	}
	slices.Sort(paths)

	// Calculate depth using tree structure
	depthMap := make(map[string]int, len(nodes))
	stack := make([]string, 0, 65)

	for _, path := range paths {
		// Pop until stack top is a strict prefix of path
		for len(stack) > 0 {
			top := stack[len(stack)-1]
			if len(top) < len(path) && path[:len(top)] == top {
				break
			}
			stack = stack[:len(stack)-1]
		}

		depth := len(stack)
		depthMap[path] = depth
		stack = append(stack, path)
	}

	// Calculate delta at each depth
	for path, node := range nodes {
		depth := depthMap[path]

		var prevLen, newLen int
		if node.Prev != nil {
			prevLen = len(node.Prev.Blob)
		}
		if node.New != nil {
			newLen = len(node.New.Blob)
		}

		switch {
		case prevLen == 0 && newLen > 0:
			deltas[depth]++ // Created
		case prevLen > 0 && newLen == 0:
			deltas[depth]-- // Deleted
		}
	}

	return
}

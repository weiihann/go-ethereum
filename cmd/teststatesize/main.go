package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"syscall"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/gorilla/websocket"
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

// JSON types matching the tracer output

type StateUpdateJSON struct {
	OriginRoot     common.Hash                                           `json:"originRoot"`
	Root           common.Hash                                           `json:"root"`
	BlockNumber    uint64                                                `json:"blockNumber"`
	AccountChanges map[common.Address]*AccountChangeJSON                 `json:"accountChanges,omitempty"`
	StorageChanges map[common.Address]map[common.Hash]*StorageChangeJSON `json:"storageChanges,omitempty"`
	CodeChanges    map[common.Address]*CodeChangeJSON                    `json:"codeChanges,omitempty"`
	TrieChanges    map[common.Hash]map[string]*TrieNodeChangeJSON        `json:"trieChanges,omitempty"`
}

type AccountChangeJSON struct {
	Prev *StateAccountJSON `json:"prev,omitempty"`
	New  *StateAccountJSON `json:"new,omitempty"`
}

type StateAccountJSON struct {
	Nonce    uint64        `json:"nonce"`
	Balance  *hexutil.Big  `json:"balance"`
	Root     common.Hash   `json:"root"`
	CodeHash hexutil.Bytes `json:"codeHash"`
}

type StorageChangeJSON struct {
	Prev common.Hash `json:"prev"`
	New  common.Hash `json:"new"`
}

type CodeChangeJSON struct {
	Prev *ContractCodeJSON `json:"prev,omitempty"`
	New  *ContractCodeJSON `json:"new,omitempty"`
}

type ContractCodeJSON struct {
	Hash   common.Hash   `json:"hash"`
	Code   hexutil.Bytes `json:"code"`
	Exists bool          `json:"exists"`
}

type TrieNodeChangeJSON struct {
	Prev *TrieNodeJSON `json:"prev,omitempty"`
	New  *TrieNodeJSON `json:"new,omitempty"`
}

type TrieNodeJSON struct {
	Blob hexutil.Bytes `json:"blob"`
	Hash common.Hash   `json:"hash"`
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
	wsURL := flag.String("url", "ws://localhost:8546/", "WebSocket URL to connect to")
	outputPath := flag.String("output", "statesize.csv", "Output CSV file path")
	depthOutputPath := flag.String("depth-output", "statesize_depth.csv", "Output CSV file path for depth statistics")
	flag.Parse()

	// Setup logging
	log.SetDefault(log.NewLogger(log.NewTerminalHandlerWithLevel(os.Stderr, log.LevelInfo, true)))

	// Open CSV files
	statFile, err := os.OpenFile(*outputPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Crit("Failed to open output file", "err", err)
	}
	defer statFile.Close()

	depthFile, err := os.OpenFile(*depthOutputPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Crit("Failed to open depth output file", "err", err)
	}
	defer depthFile.Close()

	statWriter := csv.NewWriter(statFile)
	defer statWriter.Flush()

	depthWriter := csv.NewWriter(depthFile)
	defer depthWriter.Flush()

	// Write headers if files are empty
	statInfo, _ := statFile.Stat()
	if statInfo.Size() == 0 {
		if err := statWriter.Write(statHeaders()); err != nil {
			log.Crit("Failed to write stat headers", "err", err)
		}
		statWriter.Flush()
	}

	depthInfo, _ := depthFile.Stat()
	if depthInfo.Size() == 0 {
		if err := depthWriter.Write(depthHeaders()); err != nil {
			log.Crit("Failed to write depth headers", "err", err)
		}
		depthWriter.Flush()
	}

	// Connect to WebSocket
	log.Info("Connecting to WebSocket", "url", *wsURL)
	conn, _, err := websocket.DefaultDialer.Dial(*wsURL, nil)
	if err != nil {
		log.Crit("Failed to connect to WebSocket", "err", err)
	}
	defer conn.Close()
	log.Info("Connected to WebSocket")

	// Handle interrupt signal
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

	// Message processing loop
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Error("WebSocket read error", "err", err)
				return
			}

			var update StateUpdateJSON
			if err := json.Unmarshal(message, &update); err != nil {
				log.Warn("Failed to unmarshal message", "err", err)
				continue
			}

			// Calculate deltas
			delta := calculateDelta(&update)

			// Write to CSV files
			if err := statWriter.Write(deltaToStatRow(delta)); err != nil {
				log.Warn("Failed to write stat row", "err", err)
			}
			if err := depthWriter.Write(deltaToDepthRow(delta)); err != nil {
				log.Warn("Failed to write depth row", "err", err)
			}

			// Flush periodically
			if delta.BlockNumber%128 == 0 {
				statWriter.Flush()
				depthWriter.Flush()
				log.Info("Processed block", "number", delta.BlockNumber)
			}
		}
	}()

	// Wait for interrupt or connection close
	select {
	case <-done:
		log.Info("Connection closed")
	case <-interrupt:
		log.Info("Interrupted, closing connection")
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	}

	statWriter.Flush()
	depthWriter.Flush()
	log.Info("Done")
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

func calculateDelta(update *StateUpdateJSON) *StateDelta {
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
	for _, slots := range update.StorageChanges {
		for _, change := range slots {
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
	for owner, nodes := range update.TrieChanges {
		isAccount := owner == (common.Hash{})
		var keyPrefix int64
		if isAccount {
			keyPrefix = accountTrienodePrefixSize
		} else {
			keyPrefix = storageTrienodePrefixSize
		}

		// Calculate depth deltas for all nodes
		depthDeltas := calculateDepthDeltas(nodes)

		for path, change := range nodes {
			var prevLen, newLen int
			if change.Prev != nil {
				prevLen = len(change.Prev.Blob)
			}
			if change.New != nil {
				newLen = len(change.New.Blob)
			}
			keySize := keyPrefix + int64(len(path))

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

func accountSize(acct *StateAccountJSON) int {
	if acct == nil {
		return 0
	}
	// Convert to types.StateAccount and use SlimAccountRLP for actual size
	var balance *uint256.Int
	if acct.Balance != nil {
		balance = uint256.MustFromBig(acct.Balance.ToInt())
	} else {
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
func calculateDepthDeltas(nodes map[string]*TrieNodeChangeJSON) (deltas [65]int64) {
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
	for path, change := range nodes {
		depth := depthMap[path]

		var prevLen, newLen int
		if change.Prev != nil {
			prevLen = len(change.Prev.Blob)
		}
		if change.New != nil {
			newLen = len(change.New.Blob)
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

package krogan

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"math/rand"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient/kroganclient"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"
)

const (
	maxBytecodeRequestCount = 100
)

type accountRequest struct {
	nodeID string
	reqID  uint64
	time   time.Time

	ctx     context.Context
	deliver chan *accountResponse
	revert  chan *accountRequest
	timeout *time.Timer
	stale   chan struct{}

	start common.Hash

	task *accountTask
}

type accountResponse struct {
	task *accountTask

	blockNumber hexutil.Uint64
	accounts    map[common.Hash]*types.SlimAccount

	cont bool // Whether the account range has a continuation
}

type bytecodeRequest struct {
	nodeID string
	reqID  uint64
	time   time.Time

	ctx     context.Context
	deliver chan *bytecodeResponse
	revert  chan *bytecodeRequest
	timeout *time.Timer
	stale   chan struct{}

	hashes []common.Hash
	task   *accountTask
}

type bytecodeResponse struct {
	task *accountTask

	codes map[common.Hash][]byte
}

type storageRequest struct {
	nodeID string
	reqID  uint64
	time   time.Time

	ctx     context.Context
	deliver chan *storageResponse
	revert  chan *storageRequest
	cancel  chan struct{}
	timeout *time.Timer
	stale   chan struct{}

	account common.Hash
	start   common.Hash

	mainTask *accountTask
	subTask  *storageTask
}

type storageResponse struct {
	mainTask *accountTask
	subTask  *storageTask

	accounts []common.Hash

	hashes [][]common.Hash
	slots  [][][]byte
	cont   bool // Whether the storage range has a continuation
}

type accountTask struct {
	Next     common.Hash                    // Next account to sync (starting point for iteration)
	Last     common.Hash                    // Last account in this task's range (ending boundary)
	SubTasks map[common.Hash][]*storageTask // Storage intervals needing fetching for large contracts

	StorageCompleted []common.Hash `json:",omitempty"`

	req  *accountRequest
	res  *accountResponse
	pend int

	needCode  map[common.Hash]bool // Flags whether the filling accounts need code retrieval
	needState map[common.Hash]bool // Flags whether the filling accounts need storage retrieval

	codeTasks      map[common.Hash]struct{}
	stateTasks     map[common.Hash]common.Hash
	stateCompleted map[common.Hash]struct{}

	done bool
}

type storageTask struct {
	Next common.Hash // Next storage slot to sync (starting point for iteration)

	req *storageRequest

	done bool
}

type SyncProgress struct {
	Tasks []*accountTask

	// Status report during syncing phase
	AccountSynced  uint64             // Number of accounts downloaded
	AccountBytes   common.StorageSize // Number of account bytes persisted to disk
	BytecodeSynced uint64             // Number of bytecodes downloaded
	BytecodeBytes  common.StorageSize // Number of bytecode bytes persisted to disk
	StorageSynced  uint64             // Number of storage slots downloaded
	StorageBytes   common.StorageSize // Number of storage bytes persisted to disk
}

// StateSyncer is responsible for syncing the state (accounts, storage and codes).
// TODO(weiihann):
// 1. spread workload across multiple master nodes
// 2. deal with the initial state diffs sync
// 3. feature: add new node on the fly
type StateSyncer struct {
	db          ethdb.KeyValueStore
	stateWriter ethdb.Batch

	nodes  map[string]*kroganclient.Client
	idlers map[string]struct{}

	tasks        []*accountTask
	accountReqs  map[uint64]*accountRequest
	bytecodeReqs map[uint64]*bytecodeRequest
	storageReqs  map[uint64]*storageRequest

	accountSynced  uint64             // Number of accounts downloaded
	accountBytes   common.StorageSize // Number of account bytes persisted to disk
	bytecodeSynced uint64             // Number of bytecodes downloaded
	bytecodeBytes  common.StorageSize // Number of bytecode bytes downloaded
	storageSynced  uint64             // Number of storage slots downloaded
	storageBytes   common.StorageSize // Number of storage bytes persisted to disk

	// Specifies the range of accounts to sync. If not set, it will sync all accounts
	accountRangeStart common.Hash
	accountRangeEnd   common.Hash

	startTime time.Time
	logTime   time.Time

	done bool

	update chan struct{}
	pend   sync.WaitGroup
	lock   sync.RWMutex
}

// NewStateSyncer creates a new StateSyncer instance.
func NewStateSyncer(db ethdb.KeyValueStore) *StateSyncer {
	return &StateSyncer{
		db:           db,
		stateWriter:  db.NewBatch(),
		nodes:        make(map[string]*kroganclient.Client),
		idlers:       make(map[string]struct{}),
		accountReqs:  make(map[uint64]*accountRequest),
		bytecodeReqs: make(map[uint64]*bytecodeRequest),
		storageReqs:  make(map[uint64]*storageRequest),
		update:       make(chan struct{}, 1),
	}
}

func (s *StateSyncer) Register(urls []string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	for _, url := range urls {
		client, err := rpc.DialHTTP(url)
		if err != nil {
			return err
		}

		s.nodes[url] = kroganclient.New(url, client)
		s.idlers[url] = struct{}{}
	}

	return nil
}

func (s *StateSyncer) Sync(ctx context.Context) error {
	if s.startTime.IsZero() {
		s.startTime = time.Now()
	}

	if err := s.initSync(); err != nil {
		return err
	}

	if len(s.tasks) == 0 {
		log.Debug("State sync already completed")
		return nil
	}

	defer func() { // Persist any progress, independent of failure
		for _, task := range s.tasks {
			s.forwardAccountTask(task)
		}
		s.cleanAccountTasks()
		s.saveSyncStatus()
	}()

	// Flush out the last committed raw states
	defer func() {
		if s.stateWriter.ValueSize() > 0 {
			s.stateWriter.Write()
			s.stateWriter.Reset()
		}
	}()

	defer s.reportSyncProgress(true)

	var (
		accountReqFails  = make(chan *accountRequest)
		bytecodeReqFails = make(chan *bytecodeRequest)
		accountResps     = make(chan *accountResponse)
		bytecodeResps    = make(chan *bytecodeResponse)
	)

	for {
		s.cleanAccountTasks()
		if len(s.tasks) == 0 {
			log.Info("State sync completed")
			return nil
		}

		s.assignAccountTasks(ctx, accountResps, accountReqFails)
		s.assignBytecodeTasks(ctx, bytecodeResps, bytecodeReqFails)

		if len(s.tasks) == 0 {
			log.Info("State sync completed")
			return nil
		}

		select {
		case <-s.update:
			// Something happened, recheck tasks
		case <-ctx.Done():
			return ctx.Err()
		case req := <-accountReqFails:
			s.revertAccountRequest(req)
		case req := <-bytecodeReqFails:
			s.revertBytecodeRequest(req)
		case res := <-accountResps:
			s.processAccountResponse(res)
		case res := <-bytecodeResps:
			s.processBytecodeResponse(res)
		}

		s.reportSyncProgress(false)
	}
}

func (s *StateSyncer) initSync() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if len(s.nodes) == 0 {
		return errors.New("no nodes to sync from")
	}

	// Load sync status from disk. Continue if sync is not complete.
	cont := s.loadSyncStatus()
	if cont {
		return nil
	}

	// We're starting a fresh sync.
	var (
		rangeStart = s.accountRangeStart
		rangeEnd   = s.accountRangeEnd
	)

	accountConcurrency := len(s.nodes)

	// Default to full range if not specified
	if rangeEnd == (common.Hash{}) {
		rangeEnd = common.MaxHash
	}

	// Validate range
	if rangeStart.Big().Cmp(rangeEnd.Big()) >= 0 {
		return errors.New("invalid account range: start >= end")
	}

	// Calculate step size to divide the hash space across tasks
	rangeSize := new(big.Int).Sub(rangeEnd.Big(), rangeStart.Big())
	step := new(big.Int).Div(rangeSize, big.NewInt(int64(accountConcurrency)))

	next := rangeStart
	for i := range accountConcurrency {
		last := common.BigToHash(new(big.Int).Add(next.Big(), step))

		// Ensure last task reaches the end
		if i == accountConcurrency-1 {
			last = rangeEnd
		}

		s.tasks = append(s.tasks, &accountTask{
			Next:           next,
			Last:           last,
			SubTasks:       make(map[common.Hash][]*storageTask),
			stateCompleted: make(map[common.Hash]struct{}),
		})

		// Advance to next range (last + 1)
		next = common.BigToHash(new(big.Int).Add(last.Big(), common.Big1))
	}

	return nil
}

func (s *StateSyncer) loadSyncStatus() bool {
	var progress SyncProgress

	status := rawdb.ReadSnapshotSyncStatus(s.db)
	if status == nil {
		return false
	}

	if err := json.Unmarshal(status, &progress); err != nil {
		log.Error("Failed to decode snap sync status", "err", err)
		return false
	}

	s.tasks = progress.Tasks
	s.accountSynced = progress.AccountSynced
	s.accountBytes = progress.AccountBytes
	s.bytecodeSynced = progress.BytecodeSynced
	s.bytecodeBytes = progress.BytecodeBytes
	s.storageSynced = progress.StorageSynced
	s.storageBytes = progress.StorageBytes
	s.done = len(s.tasks) == 0

	return true
}

func (s *StateSyncer) saveSyncStatus() {
	progress := &SyncProgress{
		Tasks:          s.tasks,
		AccountSynced:  s.accountSynced,
		AccountBytes:   s.accountBytes,
		BytecodeSynced: s.bytecodeSynced,
		BytecodeBytes:  s.bytecodeBytes,
		StorageSynced:  s.storageSynced,
		StorageBytes:   s.storageBytes,
	}

	status, err := json.Marshal(progress)
	if err != nil {
		panic(err) // This can only fail during implementation
	}
	rawdb.WriteSnapshotSyncStatus(s.db, status)
}

func (s *StateSyncer) cleanAccountTasks() {
	if len(s.tasks) == 0 {
		return
	}

	// Check for any task that can be finalized
	for i := 0; i < len(s.tasks); i++ {
		if s.tasks[i].done {
			s.tasks = append(s.tasks[:i], s.tasks[i+1:]...)
			i--
		}
	}

	// If everything was just finalized, marks as done
	if len(s.tasks) == 0 {
		s.lock.Lock()
		s.done = true
		s.lock.Unlock()

		// Push the final sync report
		s.reportSyncProgress(true)
	}
}

func (s *StateSyncer) assignAccountTasks(ctx context.Context, success chan *accountResponse, fail chan *accountRequest) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if len(s.idlers) == 0 {
		return
	}

	idlers := make([]string, 0, len(s.idlers))
	for id := range s.idlers {
		idlers = append(idlers, id)
	}

	for _, task := range s.tasks {
		// Skip tasks that are already assigned
		if task.req != nil || task.res != nil {
			continue
		}

		if len(idlers) == 0 {
			return
		}

		idle := idlers[0]
		node := s.nodes[idle]
		idlers = idlers[1:] // Remove the first idle node since it's going to be assigned

		// Matched a pending task to an idle peer, allocate a unique request id
		var reqid uint64
		for {
			reqid = uint64(rand.Int63())
			if reqid == 0 {
				continue
			}
			if _, ok := s.accountReqs[reqid]; ok {
				continue
			}
			break
		}

		// Generate request
		req := &accountRequest{
			nodeID:  idle,
			reqID:   reqid,
			time:    time.Now(),
			deliver: success,
			revert:  fail,
			ctx:     ctx,
			stale:   make(chan struct{}),
			start:   task.Next,
			task:    task,
		}

		// TODO(weiihann): handle timeout

		s.accountReqs[reqid] = req
		delete(s.idlers, idle)

		s.pend.Add(1)
		// TODO(weiihann): handle this
		go func() {
			defer func() {
				s.pend.Done()

				s.lock.Lock()
				defer s.lock.Unlock()

				if _, ok := s.nodes[idle]; ok {
					s.idlers[idle] = struct{}{}
				}

				select {
				case s.update <- struct{}{}:
				default:
				}
			}()
			blockNumber, accounts, err := node.AccountRange(ctx, req.start)
			if err != nil {
				log.Debug("Failed to get account range", "url", idle, "err", err)
				s.scheduleRevertAccountRequest(req)
				return
			}

			s.onAccount(reqid, blockNumber, accounts)
		}()

		task.req = req
	}
}

func (s *StateSyncer) assignBytecodeTasks(ctx context.Context, success chan *bytecodeResponse, fail chan *bytecodeRequest) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if len(s.idlers) == 0 {
		return
	}

	idlers := make([]string, 0, len(s.idlers))
	for id := range s.idlers {
		idlers = append(idlers, id)
	}

	for _, task := range s.tasks {
		// Skip tasks that don't have pending code tasks
		if len(task.codeTasks) == 0 {
			continue
		}

		if len(idlers) == 0 {
			return
		}

		idle := idlers[0]
		node := s.nodes[idle]
		idlers = idlers[1:] // Remove the first idle node since it's going to be assigned

		// Matched a pending task to an idle peer, allocate a unique request id
		var reqid uint64
		for {
			reqid = uint64(rand.Int63())
			if reqid == 0 {
				continue
			}
			if _, ok := s.bytecodeReqs[reqid]; ok {
				continue
			}
			break
		}

		hashes := make([]common.Hash, 0, maxBytecodeRequestCount)
		for hash := range task.codeTasks {
			delete(task.codeTasks, hash)
			hashes = append(hashes, hash)
			if len(hashes) >= maxBytecodeRequestCount {
				break
			}
		}

		// Generate request
		req := &bytecodeRequest{
			nodeID:  idle,
			reqID:   reqid,
			time:    time.Now(),
			deliver: success,
			revert:  fail,
			ctx:     ctx,
			stale:   make(chan struct{}),
			hashes:  hashes,
			task:    task,
		}

		s.bytecodeReqs[reqid] = req
		delete(s.idlers, idle)

		s.pend.Add(1)
		go func() {
			defer func() {
				s.pend.Done()

				s.lock.Lock()
				defer s.lock.Unlock()

				if _, ok := s.nodes[idle]; ok {
					s.idlers[idle] = struct{}{}
				}

				select {
				case s.update <- struct{}{}:
				default:
				}
			}()
			bytecodes, err := node.Bytecodes(ctx, req.hashes)
			if err != nil {
				log.Debug("Failed to get bytecodes", "url", idle, "err", err)
				s.scheduleRevertBytecodeRequest(req)
				return
			}

			s.onBytecode(reqid, bytecodes)
		}()
	}
}

func (s *StateSyncer) scheduleRevertAccountRequest(req *accountRequest) {
	select {
	case req.revert <- req:
		// Sync event loop notified
	case <-req.ctx.Done():
		// Sync cycle got cancelled
	case <-req.stale:
		// Request already reverted
	}
}

func (s *StateSyncer) revertAccountRequest(req *accountRequest) {
	log.Debug("Reverting account request", "reqID", req.reqID, "start", req.start)
	req.task.req = nil
}

func (s *StateSyncer) scheduleRevertBytecodeRequest(req *bytecodeRequest) {
	select {
	case req.revert <- req:
		// Sync event loop notified
	case <-req.ctx.Done():
		// Sync cycle got cancelled
	case <-req.stale:
		// Request already reverted
	}
}

func (s *StateSyncer) revertBytecodeRequest(req *bytecodeRequest) {
	log.Debug("Reverting bytecode request", "reqID", req.reqID, "hashes", len(req.hashes))

	// Re-add the hashes back to the task's code tasks so they can be retried
	if req.task.codeTasks == nil {
		req.task.codeTasks = make(map[common.Hash]struct{}, len(req.hashes))
	}
	for _, hash := range req.hashes {
		req.task.codeTasks[hash] = struct{}{}
	}
}

func (s *StateSyncer) onAccount(reqID uint64, blockNumber hexutil.Uint64, accounts map[common.Hash]*types.SlimAccount) {
	s.lock.Lock()

	// Ensure the response is for a valid request
	req, ok := s.accountReqs[reqID]
	if !ok {
		s.lock.Unlock()
		log.Warn("Unexpected account response")
		return
	}

	delete(s.accountReqs, reqID)
	s.lock.Unlock()

	// Response is valid, but node is not returning any data.
	// Reschedule the request.
	if blockNumber == 0 && len(accounts) == 0 {
		s.scheduleRevertAccountRequest(req)
		return
	}

	response := &accountResponse{
		task:        req.task,
		blockNumber: blockNumber,
		accounts:    accounts,
	}

	select {
	case req.deliver <- response:
	case <-req.ctx.Done():
	case <-req.stale:
	}
}

func (s *StateSyncer) onBytecode(reqID uint64, bytecodes map[common.Hash][]byte) {
	s.lock.Lock()

	req, ok := s.bytecodeReqs[reqID]
	if !ok {
		s.lock.Unlock()
		log.Warn("Unexpected bytecode response")
		return
	}

	delete(s.bytecodeReqs, reqID)
	s.lock.Unlock()

	response := &bytecodeResponse{
		task:  req.task,
		codes: bytecodes,
	}

	select {
	case req.deliver <- response:
	case <-req.ctx.Done():
	case <-req.stale:
	}
}

func (s *StateSyncer) processAccountResponse(res *accountResponse) {
	res.task.req = nil
	res.task.res = res

	// Initially assume continuation is needed
	res.cont = true

	// Find the maximum hash to update task.Next for continuation
	var maxHash common.Hash
	lastBig := res.task.Last.Big()

	for hash := range res.accounts {
		// Mark the range complete if the last is already included.
		// Keep iteration to delete the extra states if exists.
		cmp := hash.Big().Cmp(lastBig)
		if cmp == 0 {
			res.cont = false
			continue
		} else if cmp > 0 {
			// Chunk overflown, cut off excess
			delete(res.accounts, hash)
			res.cont = false // Mark range completed
			continue
		}

		// Track the maximum hash for continuation
		if bytes.Compare(hash[:], maxHash[:]) > 0 {
			maxHash = hash
		}
	}

	// Update Next for continuation (maxHash + 1)
	if res.cont && len(res.accounts) > 0 {
		res.task.Next = common.BigToHash(new(big.Int).Add(maxHash.Big(), common.Big1))
	}

	// Iterate over all the accounts and assemble which ones need further sub-tasks
	// filling before the entire account range can be persisted.
	res.task.needCode = make(map[common.Hash]bool, len(res.accounts))
	res.task.needState = make(map[common.Hash]bool, len(res.accounts))

	res.task.codeTasks = make(map[common.Hash]struct{})
	res.task.stateTasks = make(map[common.Hash]common.Hash)

	res.task.pend = 0
	for hash, account := range res.accounts {
		// Check if the account is a contract with an unknown code
		if !bytes.Equal(account.CodeHash, types.EmptyCodeHash.Bytes()) {
			if !rawdb.HasCodeWithPrefix(s.db, common.BytesToHash(account.CodeHash)) {
				res.task.codeTasks[common.BytesToHash(account.CodeHash)] = struct{}{}
				res.task.needCode[hash] = true
				res.task.pend++
			}
		}

		// TODO(weiihann): deal with storage retrieval, how to check if the storage is complete?
	}

	if res.task.pend == 0 {
		s.forwardAccountTask(res.task)
		return
	}

	// Some accounts are incomplete, leave as is for the storage and contract
	// task assigners to pick up and fill
}

func (s *StateSyncer) processBytecodeResponse(res *bytecodeResponse) {
	batch := s.db.NewBatch()

	var codes uint64
	for hash, code := range res.codes {
		// Check if this bytecode satisfies any pending account's code requirement
		if res.task.res != nil {
			for addrHash, acc := range res.task.res.accounts {
				if res.task.needCode[addrHash] && hash == common.BytesToHash(acc.CodeHash) {
					res.task.needCode[addrHash] = false
					res.task.pend--
				}
			}
		}

		// Push the bytecode into a database batch
		codes++
		rawdb.WriteCode(batch, hash, code)
	}

	bytes := common.StorageSize(batch.ValueSize())
	if err := batch.Write(); err != nil {
		log.Crit("Failed to persist bytecodes", "err", err)
	}
	s.bytecodeSynced += codes
	s.bytecodeBytes += bytes

	log.Debug("Persisted set of bytecodes", "codes", codes, "bytes", bytes)

	// If all pending requirements are satisfied, forward the task
	if res.task.pend == 0 && res.task.res != nil {
		s.forwardAccountTask(res.task)
	}
}

func (s *StateSyncer) forwardAccountTask(task *accountTask) {
	// Remove any pending delivery
	res := task.res
	if res == nil {
		return // nothing to forward
	}
	task.res = nil

	// Persist the received account segments. These flat state maybe
	// outdated during the sync, but it can be fixed later during the
	// snapshot generation.
	oldAccountBytes := s.accountBytes
	batch := ethdb.HookedBatch{
		Batch: s.db.NewBatch(),
		OnPut: func(key []byte, value []byte) {
			s.accountBytes += common.StorageSize(len(key) + len(value))
		},
	}

	var persisted int
	for hash, account := range res.accounts {
		// Skip accounts that still need code or state retrieval
		if task.needCode[hash] || task.needState[hash] {
			continue
		}

		enc, err := rlp.EncodeToBytes(account)
		if err != nil {
			panic(err)
		}

		rawdb.WriteAccountSnapshot(batch, hash, enc)
		persisted++
	}

	// Flush anything written just now and update the stats
	if err := batch.Write(); err != nil {
		log.Crit("Failed to persist accounts", "err", err)
	}
	s.accountSynced += uint64(persisted)

	// Check if the task is complete (no continuation and no pending sub-tasks)
	if !res.cont && task.pend == 0 {
		task.done = true
	}

	log.Debug("Persisted range of accounts", "accounts", persisted, "bytes", s.accountBytes-oldAccountBytes)
}

// hashSpace is the total size of the 256 bit hash space for accounts.
var hashSpace = new(big.Int).Exp(common.Big2, common.Big256, nil)

func (s *StateSyncer) reportSyncProgress(force bool) {
	if !force && time.Since(s.logTime) < 8*time.Second {
		return
	}

	// Don't report anything until we have a meaningful progress
	synced := s.accountBytes + s.bytecodeBytes + s.storageBytes
	if synced == 0 {
		return
	}

	accountGaps := new(big.Int)
	for _, task := range s.tasks {
		accountGaps.Add(accountGaps, new(big.Int).Sub(task.Last.Big(), task.Next.Big()))
	}
	accountFills := new(big.Int).Sub(hashSpace, accountGaps)
	if accountFills.BitLen() == 0 {
		return
	}
	s.logTime = time.Now()
	estBytes := float64(new(big.Int).Div(
		new(big.Int).Mul(new(big.Int).SetUint64(uint64(synced)), hashSpace),
		accountFills,
	).Uint64())
	// Don't report anything until we have a meaningful progress
	if estBytes < 1.0 {
		return
	}
	// Cap the estimated state size using the synced size to avoid negative values
	if estBytes < float64(synced) {
		estBytes = float64(synced)
	}
	elapsed := time.Since(s.startTime)
	estTime := elapsed / time.Duration(synced) * time.Duration(estBytes)

	// Create a mega progress report
	var (
		progress = fmt.Sprintf("%.2f%%", float64(synced)*100/estBytes)
		accounts = fmt.Sprintf("%v@%v", log.FormatLogfmtUint64(s.accountSynced), s.accountBytes.TerminalString())
		storage  = fmt.Sprintf("%v@%v", log.FormatLogfmtUint64(s.storageSynced), s.storageBytes.TerminalString())
		bytecode = fmt.Sprintf("%v@%v", log.FormatLogfmtUint64(s.bytecodeSynced), s.bytecodeBytes.TerminalString())
	)
	log.Info("Syncing: state sync in progress", "synced", progress, "state", synced,
		"accounts", accounts, "slots", storage, "codes", bytecode, "eta", common.PrettyDuration(estTime-elapsed))
}

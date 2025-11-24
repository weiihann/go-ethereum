package krogan

import (
	"errors"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
)

var errBlockNotFound = errors.New("block not found")

type BlockEntry struct {
	data *BlockData

	// For linked list traversal (single chain only)
	parent *BlockEntry // Points to parent (may be nil if evicted)
	child  *BlockEntry // Points to next block (single child only)
}

// BlockData contains the block and state diff data to be stored
type BlockData struct {
	Block    *types.Block
	Receipts types.Receipts
	Accounts map[common.Hash]*types.SlimAccount
	Storages map[common.Hash]map[common.Hash]common.Hash
	Codes    map[common.Hash][]byte
}

// Accessor methods to derive fields from Block
func (bs *BlockEntry) Number() uint64 {
	return bs.data.Block.NumberU64()
}

func (bs *BlockEntry) Hash() common.Hash {
	return bs.data.Block.Hash()
}

func (bs *BlockEntry) ParentHash() common.Hash {
	return bs.data.Block.ParentHash()
}

// ChainWindow combines ring buffer with parent-hash linking
type ChainWindow struct {
	capacity uint64 // size of the window (i.e. how many blocks to keep)
	head     uint64 // latest block number
	tail     uint64 // oldest block number still in buffer

	numToBlock    map[uint64]*BlockEntry                      // block number -> block entry
	hashToBlock   map[common.Hash]*BlockEntry                 // block hash -> block entry
	txLookupCache *lru.Cache[common.Hash, *types.Transaction] // tx hash -> tx

	mu sync.RWMutex
}

// NewChainWindow creates a new ring buffer with the given capacity
func NewChainWindow(capacity uint64) *ChainWindow {
	return &ChainWindow{
		capacity:    capacity,
		head:        0,
		tail:        0,
		numToBlock:  make(map[uint64]*BlockEntry, capacity),
		hashToBlock: make(map[common.Hash]*BlockEntry, capacity),
	}
}

func (c *ChainWindow) InsertBlock(data *BlockData) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Debug("debug(weiihann): insert block", "blockNum", data.Block.NumberU64(), "head", c.head, "tail", c.tail)

	blockNum := data.Block.NumberU64()
	blockHash := data.Block.Hash()
	parentHash := data.Block.ParentHash()

	// Check if block at this height already exists (REORG case)
	if existing, exists := c.numToBlock[blockNum]; exists {
		if existing.Hash() == blockHash {
			return nil // Duplicate, ignore
		}

		// REORG: Different block at same height
		// Since we enforce single-chain, truncate everything >= this height
		c.truncateFrom(blockNum)
	}

	// Check if we need to evict oldest block (ring buffer full)
	if c.isFull() {
		c.evictOldest()
	}

	// Find parent block (may not exist if evicted)
	parent := c.hashToBlock[parentHash]

	// Create new block slot
	slot := &BlockEntry{
		data:   data,
		parent: parent,
		child:  nil, // Will be set when next block is added
	}

	// Link to parent (single child only)
	if parent != nil {
		// If parent already has a child, this is a reorg
		if parent.child != nil {
			c.truncateFrom(parent.child.Number())
		}
		parent.child = slot
	}

	// Store in ring buffer
	c.numToBlock[blockNum] = slot
	c.hashToBlock[blockHash] = slot

	// Update head/tail
	if blockNum > c.head {
		c.head = blockNum
		log.Debug("debug(weiihann): update head to", "head", c.head)
	}
	if c.tail == 0 || blockNum < c.tail {
		c.tail = blockNum
	}

	return nil
}

// truncateFrom removes all blocks >= fromNumber (used for reorgs)
func (c *ChainWindow) truncateFrom(fromNumber uint64) {
	// Remove all blocks from fromNumber to head
	for num := fromNumber; num <= c.head; num++ {
		if block := c.numToBlock[num]; block != nil {
			// Unlink from parent
			if block.parent != nil {
				block.parent.child = nil
			}

			// Remove from maps
			delete(c.numToBlock, num)
			delete(c.hashToBlock, block.Hash())
		}
	}

	// Update head to the block before truncation point
	if fromNumber > 0 {
		c.head = fromNumber - 1
	} else {
		c.head = 0
		c.tail = 0
	}
}

func (c *ChainWindow) evictOldest() {
	oldest := c.numToBlock[c.tail]
	if oldest != nil {
		// Unlink from child (if exists)
		if oldest.child != nil {
			oldest.child.parent = nil
		}

		// Remove from maps
		delete(c.numToBlock, c.tail)
		delete(c.hashToBlock, oldest.Hash())
	}

	// Move tail forward
	c.tail++

	// Skip empty slots until we find next block
	for c.tail <= c.head && c.numToBlock[c.tail] == nil {
		c.tail++
	}
}

func (c *ChainWindow) isFull() bool {
	return c.head-c.tail+1 >= c.capacity
}

func (c *ChainWindow) GetBlockByNumber(number uint64) (*types.Block, error) {
	block, err := c.getBlockByNumber(number)
	if err != nil {
		return nil, err
	}
	return block.data.Block, nil
}

func (c *ChainWindow) GetBlockByHash(hash common.Hash) (*types.Block, error) {
	block, err := c.getBlockByHash(hash)
	if err != nil {
		return nil, err
	}
	return block.data.Block, nil
}

func (c *ChainWindow) CurrentHeader() (*types.Header, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	block, exists := c.numToBlock[c.head]
	if !exists {
		return nil, errBlockNotFound
	}

	return block.data.Block.Header(), nil
}

func (c *ChainWindow) CurrentBlock() (*types.Block, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	log.Debug("debug(weiihann): current head", "head", c.head)
	block, exists := c.numToBlock[c.head]
	if !exists {
		log.Debug("debug(weiihann): current block not found", "head", c.head)
		return nil, errBlockNotFound
	}

	return block.data.Block, nil
}

func (c *ChainWindow) CurrentFinalBlock() *types.Header {
	panic("TODO(weiihann): implement")
}

func (c *ChainWindow) CurrentSafeBlock() *types.Header {
	panic("TODO(weiihann): implement")
}

// TODO(weiihann): header is copied, handle it
func (c *ChainWindow) GetHeaderByNumber(number uint64) (*types.Header, error) {
	block, err := c.getBlockByNumber(number)
	if err != nil {
		return nil, err
	}

	return block.data.Block.Header(), nil
}

func (c *ChainWindow) GetHeaderByHash(hash common.Hash) (*types.Header, error) {
	block, err := c.getBlockByHash(hash)
	if err != nil {
		return nil, err
	}

	return block.data.Block.Header(), nil
}

func (c *ChainWindow) GetBlockNumber(hash common.Hash) (uint64, error) {
	block, err := c.getBlockByHash(hash)
	if err != nil {
		return 0, err
	}

	return block.Number(), nil
}

func (c *ChainWindow) GetBodyByHash(hash common.Hash) (*types.Body, error) {
	block, err := c.getBlockByHash(hash)
	if err != nil {
		return nil, err
	}

	return block.data.Block.Body(), nil
}

func (c *ChainWindow) GetBodyByNumber(number uint64) (*types.Body, error) {
	block, err := c.getBlockByNumber(number)
	if err != nil {
		return nil, err
	}

	return block.data.Block.Body(), nil
}

func (c *ChainWindow) GetCanonicalReceipt(tx *types.Transaction, blockHash common.Hash, blockNumber uint64, txIndex uint64) (*types.Receipt, error) {
	panic("TODO(weiihann): implement")
}

func (c *ChainWindow) GetReceiptsByHash(hash common.Hash) (types.Receipts, error) {
	block, err := c.getBlockByHash(hash)
	if err != nil {
		return nil, err
	}

	return block.data.Receipts, nil
}

func (c *ChainWindow) getBlockByHash(hash common.Hash) (*BlockEntry, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	block, exists := c.hashToBlock[hash]
	if !exists {
		return nil, errBlockNotFound
	}

	return block, nil
}

func (c *ChainWindow) getBlockByNumber(number uint64) (*BlockEntry, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Check if block is in valid range (inside lock to avoid race)
	if number < c.tail || number > c.head {
		return nil, errBlockNotFound
	}

	block, exists := c.numToBlock[number]
	if !exists {
		return nil, errBlockNotFound
	}

	return block, nil
}

func (c *ChainWindow) Code(addr common.Address, codeHash common.Hash) ([]byte, error) {
	panic("TODO(weiihann): implement")
}

func (c *ChainWindow) CodeSize(addr common.Address, codeHash common.Hash) (int, error) {
	panic("TODO(weiihann): implement")
}

func (c *ChainWindow) Account(addr common.Address) (*types.StateAccount, error) {
	panic("TODO(weiihann): implement")
}

func (c *ChainWindow) Storage(addr common.Address, slot common.Hash) (common.Hash, error) {
	panic("TODO(weiihann): implement")
}

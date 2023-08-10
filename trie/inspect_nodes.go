package trie

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"

	"github.com/olekukonko/tablewriter"
)

type MetaInspector struct {
	db          ethdb.Database
	fromBlock   uint64
	toBlock     uint64
	bucketRange uint64

	result    *[]*MetaStat
	totalNode stat
}

type MetaStat struct {
	ShortNode stat
	FullNode  stat
}

type counter uint64

func (c counter) String() string {
	return fmt.Sprintf("%d", c)
}

func (c counter) Percentage(current uint64) string {
	return fmt.Sprintf("%d", current*100/uint64(c))
}

type stat struct {
	size  common.StorageSize
	count counter
}

func (s *stat) Add(size common.StorageSize) {
	s.size += size
	s.count++
}

func (s *stat) Size() string {
	return s.size.String()
}

func (s *stat) Count() string {
	return s.count.String()
}

func NewMetaInspector(db ethdb.Database, fromBlock, toBlock, bucketRange uint64) (*MetaInspector, error) {

	if fromBlock > toBlock {
		return nil, fmt.Errorf("invalid block range")
	}

	// Create a list of buckets for the given range
	bucketNum := (toBlock-fromBlock+1)/bucketRange + 1
	buckets := make([]*MetaStat, bucketNum)

	// Initialize the buckets
	for i := range buckets {
		buckets[i] = &MetaStat{
			ShortNode: stat{},
			FullNode:  stat{},
		}
	}

	ins := &MetaInspector{
		db:          db,
		fromBlock:   fromBlock,
		toBlock:     toBlock,
		result:      &buckets,
		bucketRange: bucketRange,
	}

	return ins, nil
}

func (ins *MetaInspector) Run() {
	err := ins.Iterate()
	if err != nil {
		panic(err)
	}
}

func (ins *MetaInspector) Iterate() error {
	db := ins.db
	it := db.NewIterator([]byte{}, []byte{})
	defer it.Release()

	var (
		count  int64
		start  = time.Now()
		logged = time.Now()
	)

	for it.Next() {

		count++
		if count%1000 == 0 && time.Since(logged) > 8*time.Second {
			log.Info("Inspecting database", "count", count, "elapsed", common.PrettyDuration(time.Since(start)))
			logged = time.Now()
		}

		key := it.Key()
		val := it.Value()

		if len(key) == common.HashLength {
			ins.totalNode.Add(common.StorageSize(len(key) + len(val)))
		}

		if !bytes.HasPrefix(key, rawdb.MetaPrefix) || len(key) != (len(rawdb.MetaPrefix)+common.HashLength) {
			continue
		}

		valBlockNum := binary.BigEndian.Uint64(val)
		if !ins.include(valBlockNum) {
			continue
		}

		bucketNum := ins.resolveBucket(valBlockNum)
		bucket := (*ins.result)[bucketNum]

		// Get the original trie node's hash
		nodeKey := key[len(rawdb.MetaPrefix):]
		nodeHash := common.BytesToHash(nodeKey)

		// Read from database
		nodeVal, err := db.Get(nodeKey)
		if err != nil {
			return fmt.Errorf("failed to read trie node %x: %v", nodeHash, err)
		}

		// Decode the node
		node := mustDecodeNodeUnsafe(nodeHash[:], nodeVal)

		size := common.StorageSize(len(nodeKey) + len(nodeVal))

		switch node.(type) {
		case *shortNode:
			bucket.ShortNode.Add(size)
		case *fullNode:
			bucket.FullNode.Add(size)
		}
	}

	return nil
}

func (ins *MetaInspector) include(blockNum uint64) bool {
	return blockNum >= ins.fromBlock && blockNum <= ins.toBlock
}

func (ins *MetaInspector) resolveBucket(blockNum uint64) uint64 {
	return blockNum / ins.bucketRange
}

func (ins *MetaInspector) Display() {
	fmt.Printf("Trie nodes storage used from block %d to block %d\n", ins.fromBlock, ins.toBlock)
	table := tablewriter.NewWriter(os.Stdout)

	stats := make([][]string, len(*ins.result))
	totalBucketShort := common.StorageSize(0)
	totalBucketFull := common.StorageSize(0)

	for i := 0; i < len(*ins.result); i++ {
		bucket := (*ins.result)[i]
		bucketSize := bucket.ShortNode.size + bucket.FullNode.size

		low := uint64(i) * ins.bucketRange
		high := uint64(i+1)*ins.bucketRange - 1

		if i == 0 {
			low = ins.fromBlock
		}

		if high > ins.toBlock {
			high = ins.toBlock
		}

		rowStr := fmt.Sprintf("%d-%d", low, high)

		bucketPerc := fmt.Sprintf("%.2f%%", calcPercentage(bucketSize, ins.totalNode.size))
		stats = append(stats, []string{rowStr, bucket.ShortNode.Size(), bucket.FullNode.Size(), bucketSize.String(), bucketPerc})

		totalBucketShort += bucket.ShortNode.size
		totalBucketFull += bucket.FullNode.size
	}

	totalBucketSize := totalBucketShort + totalBucketFull
	totalBucketPerc := fmt.Sprintf("%.2f%%", calcPercentage(totalBucketSize, ins.totalNode.size))

	table.SetAlignment(1)
	table.SetHeader([]string{"BLOCKRANGE", "SHORTNODE", "FULLNODE", "TOTALSIZE", "PERCENTAGE"})
	table.SetFooter([]string{"Total", totalBucketShort.String(), totalBucketFull.String(), totalBucketSize.String(), totalBucketPerc})
	table.AppendBulk(stats)
	fmt.Printf("Total Trie Nodes Size: %v\nUsed Size: %v\nPercentage Used: %.2f%%\n", ins.totalNode.Size(), totalBucketSize, calcPercentage(totalBucketSize, ins.totalNode.size))
	table.Render()
}

func calcPercentage(a, b common.StorageSize) float64 {
	return float64(a) / float64(b) * 100
}

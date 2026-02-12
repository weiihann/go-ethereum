package merkle

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/ethereum/go-ethereum/nomt/core"
)

// childBucket groups stem key-value operations for a single root page child index.
type childBucket struct {
	childIndex uint8
	kvs        []core.StemKeyValue
}

// workerTask describes the work assigned to a single worker goroutine.
type workerTask struct {
	children []childBucket
}

// workerResult holds the output produced by a single worker.
type workerResult struct {
	childPageRoots []childPageRoot
	pages          []UpdatedPage
	err            error
}

// ParallelUpdate applies sorted stem key-value operations to the trie using
// multiple worker goroutines. Each worker processes a disjoint set of root
// page child subtrees (partitioned by the first 6 bits of each stem path).
//
// If numWorkers <= 1 or the batch is small, falls back to single-threaded.
// The pageSetFactory is called once per worker to create independent PageSets.
func ParallelUpdate(
	root core.Node,
	kvs []core.StemKeyValue,
	numWorkers int,
	pageSetFactory func() PageSet,
) Output {
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}
	if len(kvs) == 0 {
		return Output{Root: root}
	}
	if numWorkers <= 1 || len(kvs) < 64 {
		return singleThreadedUpdate(root, kvs, pageSetFactory())
	}

	// Step 1: Partition by child index (first 6 bits).
	buckets := partitionByChildIndex(kvs)

	// Step 2: Assign to workers.
	tasks := assignToWorkers(buckets, numWorkers)
	if len(tasks) == 0 {
		return Output{Root: root}
	}
	if len(tasks) == 1 {
		return singleThreadedUpdate(root, kvs, pageSetFactory())
	}

	// Step 3: Launch workers.
	results := make([]workerResult, len(tasks))
	var wg sync.WaitGroup
	wg.Add(len(tasks))

	for i, task := range tasks {
		go func(idx int, t workerTask) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					results[idx] = workerResult{
						err: fmt.Errorf("worker %d panicked: %v", idx, r),
					}
				}
			}()
			ps := pageSetFactory()
			results[idx] = runWorker(root, t, ps)
		}(i, task)
	}
	wg.Wait()

	// Step 4: Check for errors.
	for _, r := range results {
		if r.err != nil {
			panic(r.err)
		}
	}

	// Step 5: Collect all child page roots and updated pages.
	// ChildPageRoots are already left-to-right ordered because tasks are
	// assigned in ascending child index order and each worker processes
	// children left-to-right.
	var allChildRoots []childPageRoot
	var allPages []UpdatedPage
	for _, r := range results {
		allChildRoots = append(allChildRoots, r.childPageRoots...)
		allPages = append(allPages, r.pages...)
	}

	// Step 6: Root walker places all child roots.
	rootPS := pageSetFactory()
	rootWalker := NewPageWalker(root, nil)
	for _, cpr := range allChildRoots {
		rootWalker.AdvanceAndPlaceNode(rootPS, cpr.Position, cpr.Node)
	}
	rootOut := rootWalker.Conclude()

	// Step 7: Merge all pages.
	allPages = append(allPages, rootOut.Pages...)
	rootOut.Pages = allPages
	return rootOut
}

// singleThreadedUpdate runs the trie update with a single PageWalker.
// This is the fallback for small batches or single-worker configurations.
//
// Uses the same child-index partitioning as the parallel path to ensure
// identical hash results. Without leaf compaction, splitting at depth 1
// vs depth 7 produces different intermediate hashes, so both paths must
// use the same splitting strategy.
func singleThreadedUpdate(
	root core.Node,
	kvs []core.StemKeyValue,
	pageSet PageSet,
) Output {
	walker := NewPageWalker(root, nil)

	buckets := partitionByChildIndex(kvs)
	for childIdx, childKVs := range buckets {
		if len(childKVs) == 0 {
			continue
		}
		var leftKVs, rightKVs []core.StemKeyValue
		for i := range childKVs {
			if (childKVs[i].Stem[0]>>1)&1 == 0 {
				leftKVs = append(leftKVs, childKVs[i])
			} else {
				rightKVs = append(rightKVs, childKVs[i])
			}
		}

		if len(leftKVs) > 0 {
			walker.AdvanceAndReplace(pageSet, childPosition(uint8(childIdx), false), leftKVs)
		}
		if len(rightKVs) > 0 {
			walker.AdvanceAndReplace(pageSet, childPosition(uint8(childIdx), true), rightKVs)
		}
	}

	return walker.Conclude()
}

// partitionByChildIndex buckets sorted SKVs by the first 6 bits of each stem
// path (the root page's child index: 0-63).
func partitionByChildIndex(kvs []core.StemKeyValue) [64][]core.StemKeyValue {
	var buckets [64][]core.StemKeyValue
	for i := range kvs {
		childIdx := kvs[i].Stem[0] >> 2
		buckets[childIdx] = append(buckets[childIdx], kvs[i])
	}
	return buckets
}

// assignToWorkers distributes non-empty child buckets across numWorkers
// contiguous ranges.
func assignToWorkers(
	buckets [64][]core.StemKeyValue,
	numWorkers int,
) []workerTask {
	var nonEmpty []childBucket
	for i, kvs := range buckets {
		if len(kvs) > 0 {
			nonEmpty = append(nonEmpty, childBucket{
				childIndex: uint8(i),
				kvs:        kvs,
			})
		}
	}
	if len(nonEmpty) == 0 {
		return nil
	}
	if numWorkers > len(nonEmpty) {
		numWorkers = len(nonEmpty)
	}

	tasks := make([]workerTask, numWorkers)
	perWorker := len(nonEmpty) / numWorkers
	remainder := len(nonEmpty) % numWorkers

	idx := 0
	for w := range numWorkers {
		count := perWorker
		if w < remainder {
			count++
		}
		tasks[w] = workerTask{children: nonEmpty[idx : idx+count]}
		idx += count
	}
	return tasks
}

// runWorker processes a worker's assigned child subtrees using a PageWalker
// constrained to pages below the root page.
func runWorker(
	root core.Node,
	task workerTask,
	pageSet PageSet,
) workerResult {
	rootPageID := core.RootPageID()
	walker := NewPageWalker(root, &rootPageID)

	for _, child := range task.children {
		var leftKVs, rightKVs []core.StemKeyValue
		for i := range child.kvs {
			if (child.kvs[i].Stem[0]>>1)&1 == 0 {
				leftKVs = append(leftKVs, child.kvs[i])
			} else {
				rightKVs = append(rightKVs, child.kvs[i])
			}
		}

		if len(leftKVs) > 0 {
			walker.AdvanceAndReplace(pageSet, childPosition(child.childIndex, false), leftKVs)
		}
		if len(rightKVs) > 0 {
			walker.AdvanceAndReplace(pageSet, childPosition(child.childIndex, true), rightKVs)
		}
	}

	out := walker.Conclude()
	return workerResult{
		childPageRoots: out.ChildPageRoots,
		pages:          out.Pages,
	}
}

// childPosition creates a TriePosition at depth 7: 6 bits encoding the root
// page's child index (MSB first) plus one additional bit for left/right within
// the child page.
func childPosition(childIndex uint8, rightBit bool) core.TriePosition {
	pos := core.NewTriePosition()
	for b := 5; b >= 0; b-- {
		pos.Down((childIndex>>b)&1 == 1)
	}
	pos.Down(rightBit)
	return pos
}

package bitbox

import (
	"fmt"

	"github.com/ethereum/go-ethereum/nomt/core"
	"github.com/ethereum/go-ethereum/nomt/merkle"
)

// SyncPlan holds the pre-computed work for a sync operation.
type SyncPlan struct {
	walData    []byte
	dataWrites []dataWrite
	syncSeqn   uint32
}

type dataWrite struct {
	bucket uint64
	page   *core.RawPage
}

// BeginSync prepares a sync plan from a set of updated pages. It allocates
// or reuses buckets, builds the WAL, and returns a SyncPlan.
//
// This is Phase 1 of the 3-phase sync protocol.
func (db *DB) BeginSync(
	walPath string,
	syncSeqn uint32,
	updates []merkle.UpdatedPage,
) (*SyncPlan, error) {
	wal := NewWALBuilder()
	writes := make([]dataWrite, 0, len(updates))

	for _, up := range updates {
		if up.Diff.IsCleared() {
			// Page was cleared — tombstone its bucket.
			_, bucket, found, err := db.LoadPage(up.PageID)
			if err != nil {
				return nil, fmt.Errorf("bitbox/sync: load for clear: %w", err)
			}
			if found {
				db.metaMap.Set(bucket, MetaTombstone)
				db.occupied.Add(-1)
				wal.AddClear(bucket)
			}
			continue
		}

		// Encode the PageID into the page data.
		encodedID := up.PageID.Encode()
		up.Page.SetPageIDBytes(encodedID)
		up.Page.SetElidedChildren(up.Page.ElidedChildren())

		// Allocate or reuse a bucket.
		bucket, err := db.StorePage(up.PageID, up.Page)
		if err != nil {
			return nil, fmt.Errorf("bitbox/sync: store page: %w", err)
		}

		// Pack changed nodes from diff.
		changedNodes := up.Diff.PackChangedNodes(up.Page)

		wal.AddUpdate(
			encodedID,
			up.Diff,
			changedNodes,
			up.Page.ElidedChildren(),
			bucket,
		)
		writes = append(writes, dataWrite{bucket: bucket, page: up.Page})
	}

	walData := wal.Finish(syncSeqn)

	return &SyncPlan{
		walData:    walData,
		dataWrites: writes,
		syncSeqn:   syncSeqn,
	}, nil
}

// WriteWAL writes the WAL to disk and fsyncs it.
//
// This is Phase 2 of the 3-phase sync protocol.
func (db *DB) WriteWAL(walPath string, plan *SyncPlan) error {
	return WriteWALFile(walPath, plan.walData)
}

// CommitSync writes dirty HT data + meta pages, fsyncs the HT file, and
// truncates the WAL.
//
// This is Phase 3 of the 3-phase sync protocol.
func (db *DB) CommitSync(walPath string, plan *SyncPlan) error {
	// Write data pages.
	for _, dw := range plan.dataWrites {
		if err := db.writeDataPage(dw.bucket, dw.page); err != nil {
			return fmt.Errorf("bitbox/sync: write data: %w", err)
		}
	}

	// Write dirty meta pages.
	if err := db.FlushMeta(); err != nil {
		return fmt.Errorf("bitbox/sync: flush meta: %w", err)
	}

	// fsync the HT file.
	if err := db.file.Sync(); err != nil {
		return fmt.Errorf("bitbox/sync: fsync HT: %w", err)
	}

	// Truncate WAL — no fsync needed.
	return TruncateWALFile(walPath)
}

// FullSync runs all three phases of the sync protocol.
func (db *DB) FullSync(
	walPath string,
	syncSeqn uint32,
	updates []merkle.UpdatedPage,
) error {
	plan, err := db.BeginSync(walPath, syncSeqn, updates)
	if err != nil {
		return err
	}

	if err := db.WriteWAL(walPath, plan); err != nil {
		return err
	}

	return db.CommitSync(walPath, plan)
}

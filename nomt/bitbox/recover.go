package bitbox

import (
	"fmt"

	"github.com/ethereum/go-ethereum/nomt/core"
)

// Recover replays the WAL file to restore the database to a consistent state.
// Returns the sync sequence number from the WAL, or 0 if no recovery was
// needed.
func (db *DB) Recover(walPath string) (uint32, error) {
	data, err := ReadWALFile(walPath)
	if err != nil {
		return 0, fmt.Errorf("bitbox/recover: %w", err)
	}

	if len(data) == 0 {
		return 0, nil // No recovery needed.
	}

	syncSeqn, entries, err := ReadWAL(data)
	if err != nil {
		return 0, fmt.Errorf("bitbox/recover: parse: %w", err)
	}

	for _, entry := range entries {
		switch entry.Kind {
		case WALEntryClear:
			db.metaMap.Set(entry.ClearBucket, MetaTombstone)

		case WALEntryUpdate:
			// Read the existing data page at this bucket (or use a fresh one).
			page, readErr := db.readDataPage(entry.UpdateBucket)
			if readErr != nil {
				page = new(core.RawPage)
			}

			// Apply the diff: unpack changed nodes into the page.
			entry.Diff.UnpackChangedNodes(entry.ChangedNodes, page)

			// Set elided children and page ID.
			page.SetElidedChildren(entry.ElidedChildren)
			page.SetPageIDBytes(entry.PageID)

			// Write data page.
			if err := db.writeDataPage(entry.UpdateBucket, page); err != nil {
				return 0, fmt.Errorf("bitbox/recover: write page: %w", err)
			}

			// Update meta byte.
			hash := HashPageIDBytes(db.seed, entry.PageID)
			db.metaMap.Set(entry.UpdateBucket, MakeOccupied(hash))
		}
	}

	// Write dirty meta pages.
	if err := db.FlushMeta(); err != nil {
		return 0, fmt.Errorf("bitbox/recover: flush meta: %w", err)
	}

	// fsync HT file.
	if err := db.file.Sync(); err != nil {
		return 0, fmt.Errorf("bitbox/recover: fsync: %w", err)
	}

	// Truncate WAL.
	if err := TruncateWALFile(walPath); err != nil {
		return 0, fmt.Errorf("bitbox/recover: truncate WAL: %w", err)
	}

	return syncSeqn, nil
}

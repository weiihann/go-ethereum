package bitbox

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/ethereum/go-ethereum/nomt/core"
)

// WAL entry type tags.
const (
	walTagStart  byte = 0x01
	walTagClear  byte = 0x02
	walTagUpdate byte = 0x03
	walTagEnd    byte = 0x04
)

// WALEntryKind distinguishes the types of WAL entries.
type WALEntryKind int

const (
	WALEntryClear WALEntryKind = iota
	WALEntryUpdate
)

// WALEntry represents a single entry in the WAL.
type WALEntry struct {
	Kind WALEntryKind

	// For Clear entries:
	ClearBucket uint64

	// For Update entries:
	PageID         [32]byte
	Diff           core.PageDiff
	ChangedNodes   []core.Node
	ElidedChildren uint64
	UpdateBucket   uint64
}

// WALBuilder accumulates WAL entries in memory before serializing.
type WALBuilder struct {
	entries []WALEntry
}

// NewWALBuilder creates an empty WAL builder.
func NewWALBuilder() *WALBuilder {
	return &WALBuilder{
		entries: make([]WALEntry, 0, 64),
	}
}

// AddClear adds a CLEAR entry (tombstone a bucket).
func (b *WALBuilder) AddClear(bucket uint64) {
	b.entries = append(b.entries, WALEntry{
		Kind:        WALEntryClear,
		ClearBucket: bucket,
	})
}

// AddUpdate adds an UPDATE entry.
func (b *WALBuilder) AddUpdate(
	pageID [32]byte,
	diff core.PageDiff,
	changedNodes []core.Node,
	elidedChildren uint64,
	bucket uint64,
) {
	b.entries = append(b.entries, WALEntry{
		Kind:           WALEntryUpdate,
		PageID:         pageID,
		Diff:           diff,
		ChangedNodes:   changedNodes,
		ElidedChildren: elidedChildren,
		UpdateBucket:   bucket,
	})
}

// Finish serializes the WAL with a START and END record, padded to a
// multiple of pageSize.
func (b *WALBuilder) Finish(syncSeqn uint32) []byte {
	// Estimate size: START(5) + entries + END(1).
	estimatedSize := 5 + 1
	for _, e := range b.entries {
		switch e.Kind {
		case WALEntryClear:
			estimatedSize += 1 + 8 // tag + bucket
		case WALEntryUpdate:
			estimatedSize += 1 + 32 + 16 + len(e.ChangedNodes)*32 + 8 + 8
		}
	}

	buf := make([]byte, 0, estimatedSize+pageSize)

	// START: tag(1) + syncSeqn(4)
	buf = append(buf, walTagStart)
	var seqBuf [4]byte
	binary.LittleEndian.PutUint32(seqBuf[:], syncSeqn)
	buf = append(buf, seqBuf[:]...)

	// Entries.
	var u64Buf [8]byte
	for _, e := range b.entries {
		switch e.Kind {
		case WALEntryClear:
			buf = append(buf, walTagClear)
			binary.LittleEndian.PutUint64(u64Buf[:], e.ClearBucket)
			buf = append(buf, u64Buf[:]...)

		case WALEntryUpdate:
			buf = append(buf, walTagUpdate)
			buf = append(buf, e.PageID[:]...)
			encoded := e.Diff.Encode()
			buf = append(buf, encoded[:]...)
			for _, n := range e.ChangedNodes {
				buf = append(buf, n[:]...)
			}
			binary.LittleEndian.PutUint64(u64Buf[:], e.ElidedChildren)
			buf = append(buf, u64Buf[:]...)
			binary.LittleEndian.PutUint64(u64Buf[:], e.UpdateBucket)
			buf = append(buf, u64Buf[:]...)
		}
	}

	// END tag.
	buf = append(buf, walTagEnd)

	// Pad to page boundary.
	if rem := len(buf) % pageSize; rem != 0 {
		padding := make([]byte, pageSize-rem)
		buf = append(buf, padding...)
	}

	return buf
}

// ReadWAL parses a WAL from raw bytes. Returns the sync sequence number
// and the list of entries. Returns an error if the WAL is malformed.
func ReadWAL(data []byte) (uint32, []WALEntry, error) {
	if len(data) == 0 {
		return 0, nil, nil // Empty WAL = no recovery needed.
	}

	pos := 0
	read := func(n int) ([]byte, error) {
		if pos+n > len(data) {
			return nil, fmt.Errorf("bitbox/wal: unexpected EOF at offset %d", pos)
		}
		b := data[pos : pos+n]
		pos += n
		return b, nil
	}

	readByte := func() (byte, error) {
		b, err := read(1)
		if err != nil {
			return 0, err
		}
		return b[0], nil
	}

	readU32 := func() (uint32, error) {
		b, err := read(4)
		if err != nil {
			return 0, err
		}
		return binary.LittleEndian.Uint32(b), nil
	}

	readU64 := func() (uint64, error) {
		b, err := read(8)
		if err != nil {
			return 0, err
		}
		return binary.LittleEndian.Uint64(b), nil
	}

	// Read START.
	tag, err := readByte()
	if err != nil {
		return 0, nil, err
	}
	if tag != walTagStart {
		return 0, nil, fmt.Errorf("bitbox/wal: expected START tag, got 0x%02x", tag)
	}

	syncSeqn, err := readU32()
	if err != nil {
		return 0, nil, err
	}

	var entries []WALEntry

	for {
		tag, err := readByte()
		if err != nil {
			return 0, nil, err
		}

		switch tag {
		case walTagEnd:
			return syncSeqn, entries, nil

		case walTagClear:
			bucket, err := readU64()
			if err != nil {
				return 0, nil, err
			}
			entries = append(entries, WALEntry{
				Kind:        WALEntryClear,
				ClearBucket: bucket,
			})

		case walTagUpdate:
			pidBytes, err := read(32)
			if err != nil {
				return 0, nil, err
			}
			var pageID [32]byte
			copy(pageID[:], pidBytes)

			diffBytes, err := read(16)
			if err != nil {
				return 0, nil, err
			}
			var diffBuf [16]byte
			copy(diffBuf[:], diffBytes)
			diff := core.DecodePageDiff(diffBuf)

			nodeCount := diff.Count()
			nodes := make([]core.Node, nodeCount)
			for i := range nodeCount {
				nodeBytes, err := read(32)
				if err != nil {
					return 0, nil, err
				}
				copy(nodes[i][:], nodeBytes)
			}

			elidedChildren, err := readU64()
			if err != nil {
				return 0, nil, err
			}
			bucket, err := readU64()
			if err != nil {
				return 0, nil, err
			}

			entries = append(entries, WALEntry{
				Kind:           WALEntryUpdate,
				PageID:         pageID,
				Diff:           diff,
				ChangedNodes:   nodes,
				ElidedChildren: elidedChildren,
				UpdateBucket:   bucket,
			})

		default:
			return 0, nil, fmt.Errorf("bitbox/wal: unknown tag 0x%02x at offset %d",
				tag, pos-1)
		}
	}
}

// WriteWALFile writes a WAL to a file, creating or truncating it.
func WriteWALFile(path string, data []byte) error {
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("bitbox/wal: write: %w", err)
	}
	// fsync via re-open.
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("bitbox/wal: open for sync: %w", err)
	}
	defer f.Close()
	return f.Sync()
}

// ReadWALFile reads a WAL file. Returns nil data if the file doesn't exist
// or is empty.
func ReadWALFile(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("bitbox/wal: read: %w", err)
	}
	return data, nil
}

// TruncateWALFile empties the WAL file.
func TruncateWALFile(path string) error {
	return os.Truncate(path, 0)
}

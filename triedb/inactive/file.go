// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package inactive implements the EIP-8188 prototype's "inactive trie database":
// an append-only flat file holding frozen-trie blobs that geth's main chaindb
// references via 17-byte stubs (`0x00 || offset || size`).
//
// The file is opened once per pathdb.Database and shared across all readers.
// Reads are positional (pread) so multiple goroutines can read concurrently
// without locking. Appends are serialized behind a mutex and fsync'd before
// returning the offset — callers writing the corresponding stub into chaindb
// are guaranteed the referenced bytes are durable.
package inactive

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

// File is a handle to the on-disk inactive blob store.
//
// The on-disk layout is a header-less concatenation of opaque blobs. Each blob
// is identified by `(offset, size)` recorded in the chaindb stub that points
// to it. The file format is intentionally minimal — blob structure is the
// concern of triedb/inactive/format.go, not this layer.
type File struct {
	path string
	f    *os.File
	size atomic.Uint64 // current file size, in bytes
	mu   sync.Mutex    // serialises Append; reads do not take the lock
}

// Open opens (or creates, if `create` is true) the inactive file at `path`.
// On open, the existing file size is read and stored so callers can derive
// future append offsets without an extra stat.
func Open(path string, create bool) (*File, error) {
	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE
	}
	f, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open inactive file %q: %w", path, err)
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat inactive file %q: %w", path, err)
	}
	out := &File{path: path, f: f}
	out.size.Store(uint64(stat.Size()))
	return out, nil
}

// Path returns the file path, useful for logging.
func (f *File) Path() string { return f.path }

// Size returns the current file size in bytes. Increases monotonically as
// Append is called. Safe to call concurrently with Append.
func (f *File) Size() uint64 { return f.size.Load() }

// Read returns `size` bytes starting at `offset`. Concurrency-safe; uses
// positional read so the file's seek position is not mutated.
//
// Returns an error if the requested range is out of bounds or partially
// past EOF. The returned slice is a fresh allocation and may be retained.
func (f *File) Read(offset, size uint64) ([]byte, error) {
	if size == 0 {
		return nil, nil
	}
	end := offset + size
	if end < offset { // overflow
		return nil, fmt.Errorf("inactive read range overflows: offset=%d size=%d", offset, size)
	}
	if end > f.size.Load() {
		return nil, fmt.Errorf("inactive read past EOF: offset=%d size=%d file_size=%d",
			offset, size, f.size.Load())
	}
	buf := make([]byte, size)
	if _, err := f.f.ReadAt(buf, int64(offset)); err != nil {
		return nil, fmt.Errorf("inactive read offset=%d size=%d: %w", offset, size, err)
	}
	return buf, nil
}

// Append writes `blob` to the end of the file, fsync's the data, and returns
// the offset at which the blob now lives. After Append returns successfully,
// the bytes are durable on disk — callers can safely record the offset in a
// stub written to chaindb without risking a dangling reference on crash.
//
// Concurrency: Append is serialised. Reads can run concurrently with Append.
func (f *File) Append(blob []byte) (uint64, error) {
	offset, err := f.AppendNoSync(blob)
	if err != nil {
		return 0, err
	}
	if err := f.Sync(); err != nil {
		return 0, err
	}
	return offset, nil
}

// AppendNoSync writes `blob` to the end of the file and returns the offset
// at which it now lives, WITHOUT fsync'ing. The caller is responsible for
// calling Sync() before recording the offset in any persistent reference
// (e.g. a chaindb stub) — otherwise a crash could leave a dangling pointer.
//
// This is the high-throughput path used when many small blobs are written
// in succession and the corresponding stub batch will be committed atomically
// only after a single fsync covers all of them.
//
// Concurrency: serialised behind the same mutex as Append.
func (f *File) AppendNoSync(blob []byte) (uint64, error) {
	if len(blob) == 0 {
		return 0, errors.New("inactive: refuse to append empty blob")
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	offset := f.size.Load()
	n, err := f.f.WriteAt(blob, int64(offset))
	if err != nil {
		return 0, fmt.Errorf("inactive append at offset=%d: %w", offset, err)
	}
	if n != len(blob) {
		return 0, fmt.Errorf("inactive append short write: wrote %d of %d", n, len(blob))
	}
	f.size.Store(offset + uint64(len(blob)))
	return offset, nil
}

// Sync flushes any buffered AppendNoSync writes to disk. Cheap if no dirty
// pages are outstanding. Callers using the AppendNoSync + Sync pattern must
// call Sync before persisting any reference to the appended offsets.
func (f *File) Sync() error {
	if f == nil || f.f == nil {
		return errors.New("inactive: sync on nil/closed file")
	}
	if err := f.f.Sync(); err != nil {
		return fmt.Errorf("inactive fsync: %w", err)
	}
	return nil
}

// Close closes the underlying file. Subsequent Read/Append calls fail.
func (f *File) Close() error {
	if f == nil || f.f == nil {
		return nil
	}
	return f.f.Close()
}

// Truncate discards all blobs and resets the file to zero bytes. Used by
// the EIP-8188 converter at the start of a fresh conversion run to ensure
// no stale blobs remain referenced by stubs that no longer exist.
//
// Concurrency: serialised behind the same mutex as Append. Callers must
// not have outstanding Read calls in-flight when calling Truncate, or those
// reads may observe truncated state.
func (f *File) Truncate() error {
	if f == nil || f.f == nil {
		return errors.New("inactive: truncate on nil/closed file")
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.f.Truncate(0); err != nil {
		return fmt.Errorf("inactive truncate: %w", err)
	}
	if err := f.f.Sync(); err != nil {
		return fmt.Errorf("inactive truncate fsync: %w", err)
	}
	f.size.Store(0)
	return nil
}

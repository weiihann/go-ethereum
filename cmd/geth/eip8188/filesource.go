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

package eip8188

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/ethereum/go-ethereum/common"
)

// fileDiff is the on-disk JSONL wire type accepted by FileSource.
type fileDiff struct {
	Kind    string         `json:"kind"`
	Block   uint64         `json:"block"`
	Address common.Address `json:"address"`
	Slot    common.Hash    `json:"slot,omitempty"`
}

// FileSource streams diffs from a JSONL file. Each line is a fileDiff object.
// kind is "account" or "storage". After construction the file is read once
// per call; ARGMAX dedup is the caller's responsibility (the E2E test emits
// already-deduped fixtures).
type FileSource struct {
	path string

	mu      sync.Mutex
	lastErr error
}

// NewFileSource constructs a FileSource backed by the JSONL file at path.
func NewFileSource(path string) *FileSource {
	return &FileSource{path: path}
}

// Err returns the first fatal stream error and is part of the Source interface.
func (s *FileSource) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastErr
}

func (s *FileSource) setErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastErr == nil {
		s.lastErr = err
	}
}

// AccountDiffs streams account-kind rows from the file whose block is in
// [startBlock, endBlock]. endBlock == 0 is treated as unbounded.
func (s *FileSource) AccountDiffs(ctx context.Context, startBlock, endBlock uint64) (<-chan AccountDiff, error) {
	out := make(chan AccountDiff)
	go func() {
		defer close(out)
		err := s.stream(ctx, startBlock, endBlock, "account", func(d fileDiff) bool {
			select {
			case out <- AccountDiff{Address: d.Address, Block: d.Block}:
				return true
			case <-ctx.Done():
				return false
			}
		})
		if err != nil && ctx.Err() == nil {
			s.setErr(err)
		}
	}()
	return out, nil
}

// StorageDiffs streams storage-kind rows from the file whose block is in
// [startBlock, endBlock].
func (s *FileSource) StorageDiffs(ctx context.Context, startBlock, endBlock uint64) (<-chan StorageDiff, error) {
	out := make(chan StorageDiff)
	go func() {
		defer close(out)
		err := s.stream(ctx, startBlock, endBlock, "storage", func(d fileDiff) bool {
			select {
			case out <- StorageDiff{Address: d.Address, Slot: d.Slot, Block: d.Block}:
				return true
			case <-ctx.Done():
				return false
			}
		})
		if err != nil && ctx.Err() == nil {
			s.setErr(err)
		}
	}()
	return out, nil
}

// Close is a no-op for FileSource — each stream opens and closes its own handle.
func (s *FileSource) Close() error { return nil }

func (s *FileSource) stream(ctx context.Context, startBlock, endBlock uint64, kind string, emit func(fileDiff) bool) error {
	f, err := os.Open(s.path)
	if err != nil {
		return fmt.Errorf("open %s: %w", s.path, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	// Storage slots can include generous hex payloads; raise the per-line buffer.
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var d fileDiff
		if err := json.Unmarshal(line, &d); err != nil {
			return fmt.Errorf("parse line %d: %w", lineNo, err)
		}
		if d.Kind != kind {
			continue
		}
		if d.Block < startBlock {
			continue
		}
		if endBlock > 0 && d.Block > endBlock {
			continue
		}
		if !emit(d) {
			return ctx.Err()
		}
	}
	if err := scanner.Err(); err != nil && err != io.EOF {
		return fmt.Errorf("scan %s: %w", s.path, err)
	}
	return nil
}

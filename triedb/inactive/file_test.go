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

package inactive

import (
	"bytes"
	"path/filepath"
	"sync"
	"testing"
)

func TestOpenCreatesEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inactive.bin")
	f, err := Open(path, true)
	if err != nil {
		t.Fatalf("Open(create): %v", err)
	}
	defer f.Close()

	if got := f.Size(); got != 0 {
		t.Errorf("Size on fresh file = %d, want 0", got)
	}
}

func TestOpenMissingNonCreateErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inactive.bin")
	if _, err := Open(path, false); err == nil {
		t.Errorf("Open(create=false) on missing file: want error, got nil")
	}
}

func TestOpenExisting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inactive.bin")
	{
		f, err := Open(path, true)
		if err != nil {
			t.Fatalf("Open(create): %v", err)
		}
		if _, err := f.Append([]byte("hello world")); err != nil {
			t.Fatalf("Append: %v", err)
		}
		f.Close()
	}
	// Re-open; size should be preserved.
	f, err := Open(path, false)
	if err != nil {
		t.Fatalf("Open(existing): %v", err)
	}
	defer f.Close()

	if got, want := f.Size(), uint64(len("hello world")); got != want {
		t.Errorf("Size after re-open = %d, want %d", got, want)
	}
}

func TestAppendReadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inactive.bin")
	f, err := Open(path, true)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	blobs := [][]byte{
		[]byte("one"),
		[]byte("twotwo"),
		bytes.Repeat([]byte{0xab}, 4096),
		{0xff},
	}
	offsets := make([]uint64, len(blobs))
	for i, b := range blobs {
		off, err := f.Append(b)
		if err != nil {
			t.Fatalf("Append #%d: %v", i, err)
		}
		offsets[i] = off
	}

	for i, b := range blobs {
		got, err := f.Read(offsets[i], uint64(len(b)))
		if err != nil {
			t.Fatalf("Read #%d at offset %d: %v", i, offsets[i], err)
		}
		if !bytes.Equal(got, b) {
			t.Errorf("Read #%d: got %x, want %x", i, got, b)
		}
	}
}

func TestAppendEmptyRejected(t *testing.T) {
	f, err := Open(filepath.Join(t.TempDir(), "inactive.bin"), true)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()
	if _, err := f.Append(nil); err == nil {
		t.Errorf("Append(nil): want error, got nil")
	}
	if _, err := f.Append([]byte{}); err == nil {
		t.Errorf("Append(empty): want error, got nil")
	}
}

func TestReadOutOfBounds(t *testing.T) {
	f, err := Open(filepath.Join(t.TempDir(), "inactive.bin"), true)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()
	if _, err := f.Append([]byte("hello")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	cases := []struct {
		name         string
		offset, size uint64
	}{
		{"past_end_zero_offset", 0, 6},
		{"past_end_partial", 3, 10},
		{"past_end_at_eof", 5, 1},
		{"overflow", 1<<63 + 1, 1 << 63},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := f.Read(tc.offset, tc.size); err == nil {
				t.Errorf("Read(%d, %d) on 5-byte file: want error, got nil", tc.offset, tc.size)
			}
		})
	}
}

func TestReadZeroSize(t *testing.T) {
	f, err := Open(filepath.Join(t.TempDir(), "inactive.bin"), true)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()
	got, err := f.Read(0, 0)
	if err != nil {
		t.Errorf("Read(0,0): unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("Read(0,0) = %x, want nil", got)
	}
}

// TestConcurrentReads exercises the positional-read invariant: many
// goroutines reading at different offsets should all observe their own bytes
// without interference.
func TestConcurrentReads(t *testing.T) {
	f, err := Open(filepath.Join(t.TempDir(), "inactive.bin"), true)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	const n = 32
	offsets := make([]uint64, n)
	for i := range n {
		blob := bytes.Repeat([]byte{byte(i)}, 64)
		off, err := f.Append(blob)
		if err != nil {
			t.Fatalf("Append #%d: %v", i, err)
		}
		offsets[i] = off
	}

	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got, err := f.Read(offsets[i], 64)
			if err != nil {
				errCh <- err
				return
			}
			want := bytes.Repeat([]byte{byte(i)}, 64)
			if !bytes.Equal(got, want) {
				errCh <- &concurrentReadErr{i: i, got: got, want: want}
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

type concurrentReadErr struct {
	i         int
	got, want []byte
}

func (e *concurrentReadErr) Error() string {
	return "concurrent read returned wrong bytes"
}

func TestSizeMonotonic(t *testing.T) {
	f, err := Open(filepath.Join(t.TempDir(), "inactive.bin"), true)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	prev := f.Size()
	for i := range 5 {
		blob := bytes.Repeat([]byte{byte(i)}, i+1)
		if _, err := f.Append(blob); err != nil {
			t.Fatalf("Append: %v", err)
		}
		cur := f.Size()
		if cur <= prev {
			t.Errorf("Size did not grow: prev=%d, cur=%d", prev, cur)
		}
		prev = cur
	}
}

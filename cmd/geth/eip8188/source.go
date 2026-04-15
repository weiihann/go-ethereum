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
	"context"

	"github.com/ethereum/go-ethereum/common"
)

// AccountDiff is a record that an account was written (balance / nonce / code
// / storage) at a given block. The Source is expected to have already reduced
// to one row per address holding the max block at which it was written.
type AccountDiff struct {
	Address common.Address
	Block   uint64
}

// StorageDiff is a record that a specific storage slot on an account was
// written (to a non-zero value) at a given block. Zero-clears must be
// filtered at the Source; the injector trusts its input.
type StorageDiff struct {
	Address common.Address
	Slot    common.Hash
	Block   uint64
}

// Source provides the stream of most-recent writes for a given block range.
// Implementations must emit at most one row per key (ARGMAX over block at
// the source layer) so the injector does not need to dedupe on its own.
type Source interface {
	// AccountDiffs streams one AccountDiff per address whose most-recent write
	// fell in [startBlock, endBlock].
	AccountDiffs(ctx context.Context, startBlock, endBlock uint64) (<-chan AccountDiff, error)

	// StorageDiffs streams one StorageDiff per (address, slot) whose
	// most-recent non-zero write fell in [startBlock, endBlock].
	StorageDiffs(ctx context.Context, startBlock, endBlock uint64) (<-chan StorageDiff, error)

	// Close releases any underlying connections. Safe to call multiple times.
	Close() error
}

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

// Package eip8188 implements the prototype last-written-period injector.
//
// The injector backfills EIP-8188 period metadata into an existing geth
// path-based chaindata directory by reading post-fork state diffs from an
// external Source (ClickHouse in production, files in tests) and rewriting
// account and storage snapshot records in place.
//
// The state root is preserved byte-for-byte: only the snapshot layer is
// touched, never trie leaves. Callers must not let geth regenerate the
// snapshot after injection or the periods are lost.
package eip8188

import "math"

// DefaultBlocksPerPeriod is the reference period length from EIP-8188 —
// approximately six months at a 12 second slot time.
const DefaultBlocksPerPeriod uint64 = 1_314_000

// ComputePeriod derives the EIP-8188 period for a given block number.
// Blocks before the fork and zero-length periods saturate at zero. Periods
// that would exceed uint32 are clamped — the prototype is not expected to
// operate on horizons that long.
func ComputePeriod(block, forkBlock, blocksPerPeriod uint64) uint32 {
	if block < forkBlock || blocksPerPeriod == 0 {
		return 0
	}
	p := (block - forkBlock) / blocksPerPeriod
	if p > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(p)
}

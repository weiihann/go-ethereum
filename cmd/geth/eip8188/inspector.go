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
	"bytes"
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
)

// Snapshot key prefixes mirror core/rawdb/schema.go; hard-coded here to
// avoid a circular dependency between cmd/geth and core/rawdb on a prototype.
var (
	snapshotAccountPrefix = []byte("a")
	snapshotStoragePrefix = []byte("o")
)

// PeriodReport is the JSON document emitted by the inspect-periods CLI.
// Field names are snake_case so the bintrie-benchmarks shell harness can
// consume them without translation.
type PeriodReport struct {
	TotalAccounts       uint64 `json:"total_accounts"`
	AccountsWithPeriod  uint64 `json:"accounts_with_period"`
	MaxAccountPeriod    uint32 `json:"max_account_period"`
	TotalStorageSlots   uint64 `json:"total_storage_slots"`
	StorageWithPeriod   uint64 `json:"storage_with_period"`
	MaxStoragePeriod    uint32 `json:"max_storage_period"`
	AccountDecodeErrors uint64 `json:"account_decode_errors"`
	StorageDecodeErrors uint64 `json:"storage_decode_errors"`
}

// Inspect scans the snapshot keyspace and tallies per-record periods.
// It runs to completion or until ctx is cancelled, iterating disk order.
func Inspect(ctx context.Context, db ethdb.Iteratee) (PeriodReport, error) {
	var report PeriodReport

	if err := inspectAccounts(ctx, db, &report); err != nil {
		return report, fmt.Errorf("inspect accounts: %w", err)
	}
	if err := inspectStorage(ctx, db, &report); err != nil {
		return report, fmt.Errorf("inspect storage: %w", err)
	}
	return report, nil
}

func inspectAccounts(ctx context.Context, db ethdb.Iteratee, report *PeriodReport) error {
	it := db.NewIterator(snapshotAccountPrefix, nil)
	defer it.Release()

	for it.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		key := it.Key()
		// The snapshot account key is prefix+hash (33 bytes). Other keys that
		// happen to share the "a" prefix (e.g. longer keys from neighbouring
		// keyspaces) are filtered by the length check.
		if len(key) != len(snapshotAccountPrefix)+common.HashLength {
			continue
		}
		if !bytes.HasPrefix(key, snapshotAccountPrefix) {
			continue
		}
		report.TotalAccounts++

		var account types.SlimAccount
		if err := rlp.DecodeBytes(it.Value(), &account); err != nil {
			report.AccountDecodeErrors++
			continue
		}
		if account.LastWrittenPeriod > 0 {
			report.AccountsWithPeriod++
			if account.LastWrittenPeriod > report.MaxAccountPeriod {
				report.MaxAccountPeriod = account.LastWrittenPeriod
			}
		}
	}
	return it.Error()
}

func inspectStorage(ctx context.Context, db ethdb.Iteratee, report *PeriodReport) error {
	it := db.NewIterator(snapshotStoragePrefix, nil)
	defer it.Release()

	for it.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		key := it.Key()
		if len(key) != len(snapshotStoragePrefix)+2*common.HashLength {
			continue
		}
		if !bytes.HasPrefix(key, snapshotStoragePrefix) {
			continue
		}
		report.TotalStorageSlots++

		_, period, err := types.DecodeStorageSnapshotValue(it.Value())
		if err != nil {
			report.StorageDecodeErrors++
			continue
		}
		if period > 0 {
			report.StorageWithPeriod++
			if period > report.MaxStoragePeriod {
				report.MaxStoragePeriod = period
			}
		}
	}
	return it.Error()
}

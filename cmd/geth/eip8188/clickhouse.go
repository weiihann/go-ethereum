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
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
)

// Default xatu-cbt connection for the prototype; flags on the CLI override.
const (
	DefaultClickHouseHost     = "asyuki.tailccb236.ts.net"
	DefaultClickHousePort     = 8123
	DefaultClickHouseUser     = "default"
	DefaultClickHousePassword = ""
	DefaultClickHouseDatabase = "default"
)

// ClickHouseConfig describes the xatu-cbt ClickHouse instance to query.
type ClickHouseConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

// ClickHouseSource queries xatu-cbt's canonical_execution_* tables for the
// most-recent write per account and per (account, slot) in a block range.
// It connects over the HTTP interface so it works against the tailnet host
// without TLS bring-up.
type ClickHouseSource struct {
	conn driver.Conn
}

// NewClickHouseSource opens a connection to ClickHouse. Caller must Close.
func NewClickHouseSource(ctx context.Context, cfg ClickHouseConfig) (*ClickHouseSource, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Protocol: clickhouse.HTTP,
		Addr:     []string{fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.User,
			Password: cfg.Password,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	return &ClickHouseSource{conn: conn}, nil
}

// AccountDiffs queries each canonical_execution_* diff table, unions them,
// and returns one row per address holding the max block at which it was
// written. "Account writes" per EIP-8188 include balance changes, nonce
// increments, storage mutations (indirectly tag the account), and contract
// creation events.
func (s *ClickHouseSource) AccountDiffs(ctx context.Context, startBlock, endBlock uint64) (<-chan AccountDiff, error) {
	const query = `
		SELECT lower(address) AS addr, max(block_number) AS block
		FROM (
			SELECT address, block_number FROM canonical_execution_balance_diffs
				WHERE block_number >= ? AND block_number <= ?
			UNION ALL
			SELECT address, block_number FROM canonical_execution_nonce_diffs
				WHERE block_number >= ? AND block_number <= ?
			UNION ALL
			SELECT address, block_number FROM canonical_execution_storage_diffs
				WHERE block_number >= ? AND block_number <= ?
			UNION ALL
			SELECT contract_address AS address, block_number FROM canonical_execution_contracts
				WHERE block_number >= ? AND block_number <= ?
		)
		GROUP BY addr
	`
	rows, err := s.conn.Query(ctx, query,
		startBlock, endBlock,
		startBlock, endBlock,
		startBlock, endBlock,
		startBlock, endBlock,
	)
	if err != nil {
		return nil, fmt.Errorf("account diffs query: %w", err)
	}
	out := make(chan AccountDiff, 128)
	go func() {
		defer close(out)
		defer rows.Close()
		for rows.Next() {
			var (
				addr  string
				block uint64
			)
			if err := rows.Scan(&addr, &block); err != nil {
				log.Error("scan account diff", "err", err)
				return
			}
			select {
			case out <- AccountDiff{
				Address: common.HexToAddress(addr),
				Block:   block,
			}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// StorageDiffs queries canonical_execution_storage_diffs, filters out
// zero-clears at the source, and returns one row per (address, slot) holding
// the max block at which it was written to a non-zero value.
func (s *ClickHouseSource) StorageDiffs(ctx context.Context, startBlock, endBlock uint64) (<-chan StorageDiff, error) {
	const query = `
		SELECT lower(address) AS addr, lower(slot) AS slot_key, max(block_number) AS block
		FROM canonical_execution_storage_diffs
		WHERE block_number >= ? AND block_number <= ?
		  AND to_value != '0x0' AND to_value != '0x00' AND to_value != '0'
		GROUP BY addr, slot_key
	`
	rows, err := s.conn.Query(ctx, query, startBlock, endBlock)
	if err != nil {
		return nil, fmt.Errorf("storage diffs query: %w", err)
	}
	out := make(chan StorageDiff, 128)
	go func() {
		defer close(out)
		defer rows.Close()
		for rows.Next() {
			var (
				addr  string
				slot  string
				block uint64
			)
			if err := rows.Scan(&addr, &slot, &block); err != nil {
				log.Error("scan storage diff", "err", err)
				return
			}
			select {
			case out <- StorageDiff{
				Address: common.HexToAddress(addr),
				Slot:    common.HexToHash(slot),
				Block:   block,
			}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// Close releases the ClickHouse connection.
func (s *ClickHouseSource) Close() error { return s.conn.Close() }

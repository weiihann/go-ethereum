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
	"sync"

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

// DefaultClickHouseQueryBatch is how many blocks each ARGMAX query covers.
// Mainnet runs span ~2.6M blocks; one query over the whole range produces
// huge intermediate aggregates (especially against canonical_execution_storage_diffs)
// and can blow ClickHouse memory budgets or time out the HTTP read. Splitting
// keeps any single query bounded.
const DefaultClickHouseQueryBatch uint64 = 500_000

// ClickHouseConfig describes the xatu-cbt ClickHouse instance to query.
type ClickHouseConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string

	// QueryBatch overrides DefaultClickHouseQueryBatch when non-zero.
	QueryBatch uint64
}

// ClickHouseSource queries xatu-cbt's canonical_execution_* tables for the
// most-recent write per account and per (account, slot) in a block range.
// It connects over the HTTP interface so it works against the tailnet host
// without TLS bring-up.
type ClickHouseSource struct {
	conn       driver.Conn
	queryBatch uint64

	mu      sync.Mutex
	lastErr error
}

// Err returns the first fatal stream error and is part of the Source interface.
func (s *ClickHouseSource) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastErr
}

// setErr records the first fatal stream error. Subsequent calls are ignored
// so the original cause survives.
func (s *ClickHouseSource) setErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastErr == nil {
		s.lastErr = err
	}
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
	queryBatch := cfg.QueryBatch
	if queryBatch == 0 {
		queryBatch = DefaultClickHouseQueryBatch
	}
	return &ClickHouseSource{conn: conn, queryBatch: queryBatch}, nil
}

// batchRanges yields [lo, hi] inclusive sub-ranges of [startBlock, endBlock]
// each at most s.queryBatch wide, ordered oldest-first.
func (s *ClickHouseSource) batchRanges(startBlock, endBlock uint64) [][2]uint64 {
	if s.queryBatch == 0 || startBlock > endBlock {
		return [][2]uint64{{startBlock, endBlock}}
	}
	out := make([][2]uint64, 0, (endBlock-startBlock)/s.queryBatch+1)
	for lo := startBlock; lo <= endBlock; {
		hi := lo + s.queryBatch - 1
		if hi > endBlock {
			hi = endBlock
		}
		out = append(out, [2]uint64{lo, hi})
		if hi == endBlock {
			break
		}
		lo = hi + 1
	}
	return out
}

// AccountDiffs queries each canonical_execution_* diff table, unions them,
// and returns one row per address holding the max block at which it was
// written *within each batch*. Caller must use monotonic update semantics
// across batches; the injector does. "Account writes" per EIP-8188 include
// balance changes, nonce increments, storage mutations (indirectly tag the
// account), and contract creation events.
func (s *ClickHouseSource) AccountDiffs(ctx context.Context, startBlock, endBlock uint64) (<-chan AccountDiff, error) {
	out := make(chan AccountDiff, 128)
	batches := s.batchRanges(startBlock, endBlock)
	go func() {
		defer close(out)
		for i, br := range batches {
			log.Info("clickhouse account batch starting",
				"batch", fmt.Sprintf("%d/%d", i+1, len(batches)),
				"lo", br[0], "hi", br[1])
			emitted, err := s.streamAccountBatch(ctx, br[0], br[1], out)
			if err != nil {
				s.setErr(err)
				log.Error("clickhouse account batch", "lo", br[0], "hi", br[1], "err", err)
				return
			}
			log.Info("clickhouse account batch finished",
				"batch", fmt.Sprintf("%d/%d", i+1, len(batches)),
				"lo", br[0], "hi", br[1], "emitted", emitted)
			if ctx.Err() != nil {
				return
			}
		}
	}()
	return out, nil
}

// streamAccountBatch issues one ARGMAX query for a single block range and
// emits its rows to out. Returns the row count for observability.
func (s *ClickHouseSource) streamAccountBatch(ctx context.Context, lo, hi uint64, out chan<- AccountDiff) (uint64, error) {
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
	rows, err := s.conn.Query(ctx, query, lo, hi, lo, hi, lo, hi, lo, hi)
	if err != nil {
		return 0, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()
	var emitted uint64
	for rows.Next() {
		var (
			addr  string
			block uint64
		)
		if err := rows.Scan(&addr, &block); err != nil {
			return emitted, fmt.Errorf("scan: %w", err)
		}
		select {
		case out <- AccountDiff{
			Address: common.HexToAddress(addr),
			Block:   block,
		}:
			emitted++
		case <-ctx.Done():
			return emitted, ctx.Err()
		}
	}
	if err := rows.Err(); err != nil {
		return emitted, fmt.Errorf("rows: %w", err)
	}
	return emitted, nil
}

// StorageDiffs queries canonical_execution_storage_diffs, filters out
// zero-clears at the source, and returns one row per (address, slot) holding
// the max block at which it was written to a non-zero value *within each
// batch*. Caller must use monotonic update semantics across batches.
func (s *ClickHouseSource) StorageDiffs(ctx context.Context, startBlock, endBlock uint64) (<-chan StorageDiff, error) {
	out := make(chan StorageDiff, 128)
	batches := s.batchRanges(startBlock, endBlock)
	go func() {
		defer close(out)
		for i, br := range batches {
			log.Info("clickhouse storage batch starting",
				"batch", fmt.Sprintf("%d/%d", i+1, len(batches)),
				"lo", br[0], "hi", br[1])
			emitted, err := s.streamStorageBatch(ctx, br[0], br[1], out)
			if err != nil {
				s.setErr(err)
				log.Error("clickhouse storage batch", "lo", br[0], "hi", br[1], "err", err)
				return
			}
			log.Info("clickhouse storage batch finished",
				"batch", fmt.Sprintf("%d/%d", i+1, len(batches)),
				"lo", br[0], "hi", br[1], "emitted", emitted)
			if ctx.Err() != nil {
				return
			}
		}
	}()
	return out, nil
}

// streamStorageBatch issues one ARGMAX query for a single block range and
// emits its (address, slot) rows to out.
func (s *ClickHouseSource) streamStorageBatch(ctx context.Context, lo, hi uint64, out chan<- StorageDiff) (uint64, error) {
	const query = `
		SELECT lower(address) AS addr, lower(slot) AS slot_key, max(block_number) AS block
		FROM canonical_execution_storage_diffs
		WHERE block_number >= ? AND block_number <= ?
		  AND to_value != '0x0' AND to_value != '0x00' AND to_value != '0'
		  AND to_value != '0x0000000000000000000000000000000000000000000000000000000000000000'
		GROUP BY addr, slot_key
	`
	rows, err := s.conn.Query(ctx, query, lo, hi)
	if err != nil {
		return 0, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()
	var emitted uint64
	for rows.Next() {
		// canonical_execution_storage_diffs.block_number is UInt32 in the
		// source schema; the accounts query unions across multiple tables
		// so the union widens to UInt64, but this single-table query keeps
		// the original width. Scan must match exactly or clickhouse-go
		// errors at row 0.
		var (
			addr  string
			slot  string
			block uint32
		)
		if err := rows.Scan(&addr, &slot, &block); err != nil {
			return emitted, fmt.Errorf("scan: %w", err)
		}
		select {
		case out <- StorageDiff{
			Address: common.HexToAddress(addr),
			Slot:    common.HexToHash(slot),
			Block:   uint64(block),
		}:
			emitted++
		case <-ctx.Done():
			return emitted, ctx.Err()
		}
	}
	if err := rows.Err(); err != nil {
		return emitted, fmt.Errorf("rows: %w", err)
	}
	return emitted, nil
}

// Close releases the ClickHouse connection.
func (s *ClickHouseSource) Close() error { return s.conn.Close() }

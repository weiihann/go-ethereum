# EIP-8188 prototype — work summary

Branch: `feat/eip-8188/inject`

This document summarises **what we built and what we learned**. For *how it
works* (architecture, on-disk formats, code map), see
[`eip8188-v2-handoff.md`](./eip8188-v2-handoff.md). For the quantitative
end-state measurements, see
[`../eip8188-mainnet-runs/stats-20260501-0553/04-synthesis.md`](../eip8188-mainnet-runs/stats-20260501-0553/04-synthesis.md).

## What this prototype is

A research vehicle for EIP-8188 (state expiry) that physically separates "frozen"
state from "live" state into two stores while preserving consensus state-root
hashes and read transparency.

```
chaindb (live, hot, pebble)        inactive.bin (frozen, cold, append-only)
───────────────────────────         ─────────────────────────────────────────
standard MPT nodes (RLP)            v2 blobs: 16 B header + post-order DFS
+ 17-byte stubs pointing into ──→   each blob = one originally-stubbed subtree
+ hybrid nodes (live RLP + inline
  metadata for still-cold children)
```

Goal: measure the read/write cost shape of the "separate file" variant to
inform a production design.

## Phases

### 1. Build (prior sessions)

| Component | Commit |
|---|---|
| Period injector (offline backfill from ClickHouse) | `b9c6d7b7f` |
| Inactive-subtree identifier | `d4b692504` |
| Inactive database v2 (lazy materialisation) | `3d903e684` |
| Handoff documentation | `5bc590b9a` |

The v2 design replaced an earlier v1 that fully-materialised any touched stub
into chaindb. v2's lazy approach materialises only the touched path, leaving
untouched siblings as expired references inside hybrid parent nodes.

### 2. Mainnet validation (this session and immediately prior)

Imported 2,628,700 mainnet blocks (head 19,999,256 → 22,627,956) on top of
EIP-8188-converted state. Pipeline:

1. `db inject-periods` — backfill period markers into snapshot accounts/slots from a ClickHouse state-diff stream. ~1h32m.
2. `db inspect-periods` — sanity check (max period == 2 as expected from the period math). ~6m.
3. `db convert-inactive --scope both` — identify inactive subtrees, serialise to `inactive.bin`, write 17-byte stubs in chaindb, delete original interior trie nodes. ~hours.
4. `db compact` — reclaim pebble tombstones from the convert deletes.
5. `geth import` — import 2.6 M blocks. Reads cross stub boundaries; writes trigger lazy materialisation; commits emit hybrid parent nodes.
6. `db count-trienode-kinds` — verify hybrid count went from 0 to >0 (lazy mat actually fired during import).

LVM thin snapshots gated each phase so any failure was revertible without re-syncing 19 M blocks.

**Result:** 0 merkle root errors, 0 EIP-8188 invariant violations across all 2.6 M blocks.

### 3. Pipeline-stage measurement (this session)

Captured trie composition at three reachable LVM snapshot states (post-import, post-convert-both, post-inject) plus a synthesised vanilla baseline. Two small tool extensions enabled byte-level reporting (`2c78254ce`). Headline numbers in [the synthesis report](../eip8188-mainnet-runs/stats-20260501-0553/04-synthesis.md).

## Bugs caught during validation (and how)

### Identifier double-counting (`384d5dd7e`)

When a fully-inactive parent `N` was emitted as a single InactiveSubtree, the algorithm was *also* inheriting its inner candidates upward — so the same subtree got emitted twice. Manifested at scale as 20 M `Unexpected trie node` errors during import + a pebble SIGSEGV under load. Fix: drop `popped.candidates` when popped is itself emittable.

### Delete-collapse on `*expiredNode` survivor (`62e081ccd`, `7fd7ea811`)

`trie.delete` collapses a fullNode with one remaining child into a shortNode. The collapse path called `t.resolve(child)` to inspect the survivor, but `resolve` returned `*expiredNode` for stubbed children unchanged. The shortNode-detection branch then failed and produced an invalid MPT (`shortNode{[pos], *expiredNode}`). Manifested as a state root mismatch at block 19,999,259. Fix: `resolve()` now fully materialises `*expiredNode` after `resolveAndTrack` so the merge branch fires correctly.

### convert-inactive fsync per blob (`879609325`)

`inactive.File.Append` was issuing one `fsync` per appended blob. On NVMe + LVM thin snapshot COW, this saturated the disk at 99% util on only 2.7 MB/s of writes. Original ETA was 16 days for the full convert. Fix: added `AppendNoSync` + `Sync` methods, aligned fsync with pebble batch boundaries (one fsync per ~12,500 subtrees instead of per subtree). 500x speedup; convert finished in a few hours.

### Inactive file not auto-attached during import (`99309f61b`)

`MakeChain` (the import path) wasn't setting `TrieInactiveFile` so reads through stubs hit a missing archive resolver. Fix: set the flag in `MakeChain` analogously to `MakeTrieDatabase`.

### Snapshot byte-tally missed keys (this session)

The `inspect-periods` byte tally I added initially counted only `len(it.Value())`. Snapshot keys are 33 B (account) / 65 B (storage), and storage keys alone account for 75 GB of data — the original measurement undercounted by 5x for storage. Fix: split the tally into key vs value contributions (this is also semantically clearer since period markers only affect values).

## Measurement headlines

| | Stage 3: post-inject | Stage 2: post-convert-both | Stage 1: post-import |
|---|---:|---:|---:|
| chaindb trie bytes | 148.14 GB | 32.68 GB | 66.89 GB |
| inactive.bin | — | 162.39 GB | 162.39 GB |
| **trie storage total** | **148.14 GB** | **195.07 GB** | **229.28 GB** |

- Conversion: chaindb trie -78% (148 → 33 GB), inactive.bin +162 GB. **Net total disk +47 GB** at conversion time.
- Lazy materialisation during 2.6 M blocks: chaindb trie +34 GB, inactive.bin unchanged. ~80% of original stubs survived the entire import (cold state stayed cold).
- Period-injection overhead: <0.5% of snapshot bytes (~300 MB on a 101 GB snapshot base).
- Storage subtree compression: ~7.7 trie keys collapse into 1 stub (account subtrees average 2.0 keys/stub).

**Verdict:** EIP-8188 trades ~30–50 GB more total disk for a ~70% reduction in working-set chaindb bytes. The cold inactive.bin can live on slower or remote storage. The hot chaindb shrinks dramatically.

## Bench machine handoff

`/mnt/disk1/eip8188-bench/post-convert-both/` holds a 689 GB rsync of the post-convert datadir (head 19,999,256, periods + stubs + 162 GB inactive.bin) for import-benchmark replication on a different machine. The 92 GB block stream `/mnt/experiment/19999256_22627956.gz` is *not* included.

## Known open issues

- **Prefetcher concurrency panic.** During import, the parallel speculative `statePrefetcher` occasionally panicked on stack underflow (`opSwap1` from a prefetch, stack[15] of length 15) when accessing state derived from a `*expiredNode`. The auto-retry wrapper masked it (each retry got further) and we reached the target head, but the underlying race in copying state containing expired nodes during parallel execution is unresolved.
- **No true vanilla baseline.** The pre-inject LVM snapshot was removed earlier in the experiment to free COW space, so the "vanilla" column in the synthesis is a computed estimate (subtract period-marker overhead from post-inject). Re-syncing from genesis would give exact numbers but takes days.
- **Hybrid encoding heavier than expected.** Average hybrid node is ~185 B (account) / ~235 B (storage), versus ~17 B for stubs and ~78–128 B for standard RLP. ~2.5x the size of a comparable standard branch node. Worth a focused look if we want to reduce hot-path bytes further.

## Code reference (where to look)

| Concern | Location |
|---|---|
| CLI entry points | `cmd/geth/dbcmd_eip8188.go` |
| Pipeline (inject / inspect / identify / convert) | `cmd/geth/eip8188/{injector,inspector,identifier,converter}.go` |
| v2 inactive blob format | `triedb/inactive/{format,file}.go` |
| Read / write / commit paths | `trie/{expired_node,inactive_resolve,inactive_lazy,hybrid_codec}.go` |
| Pathdb hash-check skip for stub/hybrid bytes | `triedb/pathdb/reader.go` |
| Detailed architecture | `docs/eip8188-v2-handoff.md` |
| Measurement synthesis | `eip8188-mainnet-runs/stats-20260501-0553/04-synthesis.md` |

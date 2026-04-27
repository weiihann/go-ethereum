# EIP-8188 Inactive Database — Prototype Handoff

Branch: `feat/eip-8188/inject` · Status: prototype, dev-mode tested, **not yet exercised on mainnet**

## Purpose

EIP-8188 is a state-expiry research proposal: state that hasn't been written to
in a long time gets "frozen" so it doesn't bloat the live state database. This
branch is a research prototype exploring **how to physically separate the
frozen state from the live state** while preserving:

1. **Read transparency** — accessing a frozen account/slot still works.
2. **Write efficiency** — re-touching a frozen account doesn't require
   re-inflating its entire enclosing subtree.
3. **Hash invariance** — the post-fork state root machinery is unchanged;
   freezing is a storage-layout transformation, not a consensus change.

The end goal is to inform whether a production EIP-8188 should keep its frozen
state in the same database engine (pebble) as the live state, or split it
into a separate file optimised for cold reads. This prototype builds the
"separate file" variant and measures the read/write cost shape.

## Status (what's done in this branch)

| Layer | Status |
|---|---|
| Period injection (offline backfill) | merged earlier on this branch |
| Inactive-subtree identification | merged earlier on this branch |
| **v1** inactive database (full-subtree materialisation) | superseded — v2 replaces it |
| **v2** inactive database (lazy materialisation) | this commit |
| Mainnet-scale validation | **not done** — handoff |

Tests passing:
- `go test ./trie/... ./triedb/... ./cmd/geth/eip8188/... ./core/state/...`
- `bash bintrie-benchmarks/eip8188-test/run.sh` (8 steps)

## Architecture

### Two-tier storage

```
chaindb (live, hot)              inactive.bin (frozen, cold)
─────────────────────             ────────────────────────────
"A" + path → trie node            append-only blob store
"O" + owner + path → trie node    multiple v2 blobs concatenated
                                   each blob = 16B header + post-order DFS body
```

### Three flavours of chaindb value

A trie-node value's first byte classifies it:

| Marker | Kind | Length | Meaning |
|---|---|---|---|
| `0xc0+` | Standard MPT RLP | varies | Live trie node (unchanged from upstream) |
| `0x00` | Primary stub | 17 B | `marker + blobOffset(8) + rootInBlob(4) + rootSize(4)`. Decoded into `*expiredNode` pointing at the blob's root entry |
| `0x01` | Hybrid node | varies | `marker + standardRLP + count + blobOffset + [stubEntry]*`. A partially-materialised parent: standard RLP for hash invariance, inline metadata for off-path *expiredNode children |

`pathdb/reader.go` skips its keccak verification on `0x00` and `0x01` (their
bytes deliberately don't match the parent's hashNode reference).

### v2 blob format (`triedb/inactive/format.go`)

```
header (16 B)
  version=2 | reserved | rootOffset(4) | rootSize(4)

body — nodes in post-order DFS (children before parents)
  tag(1) + body
    fullNode body  = 17 child slots (children[0..15] + value)
    shortNode body = keyLen(2) + key bytes + 1 child slot

child slot — first byte = kind:
  0 empty
  1 hashed-ref    (39 B: hash[32] + relOffset[4] + size[2])
  2 embedded-ref  (7 B:  relOffset[4] + size[2])
  3 inline-value  (variable)
```

The hash carried in slot kind 1 is what makes lazy materialisation possible:
the materialiser can substitute a sibling with `*expiredNode{hash}`, and that
node's `encode()` re-emits the original 32-byte hashNode bytes — preserving
the parent's MPT hash byte-for-byte.

### Read path

`Trie.Get(k)` hits `*expiredNode` → `navigateInactive` → `navigateNodeByReader`:
- Reads only one node entry per descent step from the inactive file
  (O(depth) bytes, not O(blob)).
- Follows children via `blobOffset + slot.relOffset`.

### Write path (lazy materialisation)

`Trie.Insert/Delete` hits `*expiredNode` → `materialiseLazyPath`:

1. Walk the blob along the modified key suffix.
2. **On-path child**: recurse.
3. **Off-path hashed-ref sibling**: substitute with
   `*expiredNode{blobOffset, nodeFileOffset, size, hash}` — stays a reference
   into the blob.
4. **Off-path embedded-ref sibling**: must fully materialise (the parent's
   RLP inlines it, and an inline element can't be substituted with a hash
   without changing the parent's hash). Embedded children are <32 B, so this
   is bounded.
5. **Inline-value**: keep as `valueNode`.

The result is a partial subtree: live nodes along the modified path,
`*expiredNode` references at the off-path siblings.

### Commit path (hybrid nodes)

`committer.commit()` preserves `*expiredNode` children in place (instead of
collapsing to hashNode like normal clean nodes). Then `committer.store()`
calls `nodeStorageBytes(n)`:

- If `n` has no `*expiredNode` children → standard RLP.
- If `n` has any `*expiredNode` children → `assembleHybridBytes(n)`:
  - `0x01` marker
  - Standard RLP of `n` (each `*expiredNode` encodes as its 32-byte hash →
    identical to the pre-conversion form → parent hash unchanged)
  - Trailing metadata: `stubCount, blobOffset,
    [(childIdx, nodeOffsetInBlob, nodeSize)]*`

Reading later: `decodeNodeUnsafe` dispatches `0x01` → `decodeHybrid`, which
decodes the standard RLP into a normal node tree, then patches each named
child slot to replace its hashNode with `*expiredNode{...}`.

**Cost**: per modification, one chaindb entry per level along the modified
path — O(depth), typically 5–10 entries on mainnet. No per-sibling write
amplification.

### Converter (`cmd/geth/eip8188/converter.go`)

Offline `geth db convert-inactive`:
1. **Clean slate**: truncate `inactive.bin`, sweep chaindb deleting any
   `0x00`/`0x01` value.
2. `Identify` walks the trie and emits maximal all-inactive subtree roots.
3. For each: materialise from chaindb, encode v2 blob, append to file (fsync),
   write 17-byte primary stub at the subtree's chaindb path, delete interior
   trie-node keys.

## Key files

| File | Role |
|---|---|
| `triedb/inactive/format.go` | v2 blob format spec, `IsStub`/`IsHybrid`/`IsStubOrHybrid` |
| `triedb/inactive/file.go` | `inactive.File` (Open/Read/Append/Truncate) |
| `trie/expired_node.go` | `*expiredNode` struct, `encode()` emits hashNode RLP, `decodeStub`, `EncodeStub` |
| `trie/inactive_resolve.go` | v2 encoder (`EncodeInactiveBlob`), `navigateInactive`, `MaterialiseLiveSubtree` |
| `trie/inactive_lazy.go` | `materialiseLazyPath`, `fullyMaterialiseByReader` |
| `trie/hybrid_codec.go` | `assembleHybridBytes`, `decodeHybrid` |
| `trie/committer.go` | preserves `*expiredNode` children, calls `nodeStorageBytes` |
| `trie/node.go` | `decodeNodeUnsafe` dispatches on first byte (0x00 / 0x01 / 0xc0+) |
| `triedb/pathdb/reader.go` | hash-check skip extended to `IsStubOrHybrid` |
| `cmd/geth/eip8188/converter.go` | `Convert`, `prepareCleanSlate`, `convertOne` |
| `cmd/geth/eip8188/identifier.go` | walks trie, emits inactive subtree roots |
| `cmd/geth/eip8188/injector.go` | period backfill (earlier work) |
| `cmd/geth/dbcmd_eip8188.go` | CLI: `db inject-periods`, `inspect-periods`, `identify-inactive`, `convert-inactive`, `count-trienode-kinds` |

## How to run / verify

### Build

```bash
cd /path/to/go-ethereum
go build -o build/bin/geth-prototype ./cmd/geth
```

### Unit + integration tests

```bash
go test ./trie/... ./triedb/... ./cmd/geth/eip8188/... ./core/state/...
```

Two key tests demonstrate the v2 design:
- `trie.TestLazyMaterialiseProducesHybrid` — controlled trie test:
  build a multi-leaf subtree, mount as `*expiredNode`, modify one leaf,
  assert the commit nodeset contains a hybrid (0x01) entry.
- `eip8188.TestLazyMaterialiseAfterConvert` — full pipeline: build state,
  convert, modify a converted account, verify hybrid entries appear.

### End-to-end (dev chain)

`bash bintrie-benchmarks/eip8188-test/run.sh` (8 steps):
1. Build geth
2. Spin up dev mode, send spamoor traffic
3. Inject periods from JSONL fixture
4. Verify periods via `inspect-periods`
5. `identify-inactive` — walk trie, emit subtrees
6. `convert-inactive` — move subtrees to inactive.bin, write stubs
7. Send more spamoor traffic, run `count-trienode-kinds`, assert ≥1 hybrid

Last successful run (from this conversation): 17 hybrids appeared in the
account trie after 30 follow-up transfers.

### CLI cheatsheet

```bash
# Inspect period stats from snapshot
geth --datadir DD db inspect-periods --json

# Inspect chaindb classification by first byte
geth --datadir DD db count-trienode-kinds --json
# → {"account_trie":{"stubs":N,"hybrids":N,"standard_rlp":N,...},"storage_trie":{...}}

# Convert all inactive subtrees offline
geth --datadir DD db convert-inactive \
  --fork-block 0 --blocks-per-period 3 \
  --current-period 3 --inactive-min-age 3 \
  --scope account \
  --inactive-file DD/geth/chaindata/inactive.bin
```

## Mainnet handoff — things to actually try

1. **Convert a small mainnet snapshot end-to-end.** Pick a recent state root,
   run inject-periods (you'll need real period fixtures — currently the
   workflow uses ClickHouse via `--source clickhouse`), then convert. Watch:
   - Conversion wall time vs. trie size.
   - inactive.bin size growth (compare to chaindb byte savings).
   - Whether any subtree fails to materialise (e.g., decoding errors,
     missing nodes due to pathdb pruning).

2. **Verify hash invariance at scale.** Before/after convert, compute the
   state root via `geth db root` (or similar). Must be byte-identical. Any
   drift is a bug — the standard RLP component of hybrids and the v2 blob
   encoding are designed to preserve every keccak-32-byte child reference.

3. **Measure the read cost.** Inactive accounts read via the snapshot are
   fast (snapshot doesn't go through the trie), but storage proofs and
   `eth_getProof` go through the trie. With many sub-stubs accumulating,
   how does proof generation perform? `Trie.getNode` currently **errors
   out** at `*expiredNode` — proofs across an inactive boundary are
   unsupported. Worth fixing before any production use.

4. **Measure write cost.** Send a series of transactions touching converted
   accounts; observe `count-trienode-kinds` after each block. Each modified
   path should add ~5–10 hybrid entries. If the count grows by far more,
   the lazy materialiser is over-expanding (bug) or the trie shape is
   pathological.

5. **Re-modification across nested hybrids.** A second write to a nearby
   key whose path crosses an existing hybrid should produce a deeper
   hybrid. The `decodeHybrid` → `*expiredNode` patching path needs to
   work transparently. The unit test covers it for one level; mainnet
   workloads will exercise multiple.

## Known sharp edges & open questions

### Sharp edges (would-bite-on-mainnet)

- **Identifier double-counting.** With a mixed root, the identifier emits
  BOTH a parent subtree AND its child subtrees as candidates (see the
  test that observed 70 emitted subtrees from 64 accounts). The converter
  processes them in order, and stubbing a parent makes subsequent child
  conversions fail with "*expiredNode at..." (because `MaterialiseLiveSubtree`
  rejects already-stubbed paths). This currently surfaces as logged warnings
  + `ConversionErrors` increments, but no data corruption. Worth fixing
  the identifier to dedupe before processing mainnet-scale tries.

- **Embedded-ref full-materialisation cost in lazy mat.** Off-path
  embedded children are always fully expanded. On a path with deep embedded
  subtrees this could blow up. In practice mainnet embeds are tiny shortNodes
  with single-leaf values, so the cost is bounded — but worth measuring.

- **shortNode-mismatch fallback.** When a write's path diverges from a
  shortNode's key inside an inactive subtree, the materialiser falls back
  to full materialisation. This is correct but expensive for new keys
  inserted into deep inactive paths. Could be optimised by handling the
  divergence in-place (split the shortNode into a fullNode without
  materialising the unchanged side).

- **Orphaned blob regions.** Lazy materialisation creates sub-stubs that
  point into different parts of the blob. As writes accumulate, parts of
  the blob become unreachable. There's no compaction. Long-running nodes
  will see inactive.bin grow monotonically. v2 design assumes periodic
  re-conversion to reclaim space — not implemented.

- **Convert + diff-layer interaction.** `prepareCleanSlate` runs before
  the trie iterator; if pathdb has dirty diff layers above the chosen
  state root, the converter's view may be inconsistent. The CLI calls
  `flushDiffLayers` first (see `dbConvertInactive`), but this assumes the
  node was shut down cleanly. A crashed pathdb might leave the journal
  partially loaded.

### Open design questions

- **Should hybrid nodes be merged back into stubs once all their
  *expiredNode children get re-modified?** Currently a hybrid stays a
  hybrid even if all its sub-stubs become live. The `nodeStorageBytes`
  check is "any *expiredNode children → hybrid; else RLP", so this is
  self-healing — once all sub-stubs are live, the next commit emits
  pure RLP. Worth verifying with a long-running test.

- **Should the inactive file be sharded by period?** The plan defers this.
  A single growing file is simpler, but periodic shards would let us drop
  oldest-period data wholesale without compaction.

- **eth_getProof support.** Currently `Trie.getNode` errors at the
  `*expiredNode` boundary. A production EIP-8188 must support proofs.
  This requires materialising the path at proof time and emitting proof
  nodes for the materialised section + a sentinel for the inactive
  remainder.

## References

- Original plan file (preserved for context):
  `~/.claude/plans/sequential-sparking-phoenix.md` on the original machine.
- Earlier commits on this branch establish injector + identifier:
  `b9c6d7b7f` (period injector), `d4b692504` (identifier).
- Bintrie benchmark scripts: `bintrie-benchmarks/eip8188-test/run.sh`
  (separate repo on `eip8188` branch).

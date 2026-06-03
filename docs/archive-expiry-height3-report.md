# Archive-expiry height-3 inactive move-out — mainnet footprint report

Moving fully-inactive **height-3** subtrees of the Merkle-Patricia state trie out of
chaindb into an external append-only file (`nodearchive`) in the archive-expiry
"expired node" format, and measuring the resulting on-disk footprint against a compacted
vanilla baseline.

- **Trie:** hex MPT (account + per-contract storage), Ethereum mainnet, head 19,999,256.
- **Format:** archive-expiry leaves-only records (gballet/archival-command), gated by
  EIP-8188 inactivity periods.
- **Result:** chaindb −54.54 GB, archive +43.09 GB → **net −11.45 GB (−4.55%)** of the
  251.75 GB compacted baseline. 77,002,180 subtrees moved out, `errors=0`.

---

## 1. Summary

Each subtree of **height exactly 3** (measured from leaves; ≤ 16² = 256 leaves) whose
*every* leaf is inactive is removed from chaindb: its leaves are appended to `nodearchive`
as RLP records, its root key is overwritten with a 17-byte stub pointing at those records,
and its interior nodes are deleted. The original interior structure is not stored — it is
rebuilt on read. This trades disk for recompute-on-access.

| | value |
|---|---|
| subtrees moved out | 77,002,180 (account 6,283,805 + storage 70,718,375) |
| interior+leaf nodes deleted | 661,010,044 |
| `nodearchive` size | 43.09 GB |
| stubs written | 77,002,180 × 17 B = 1.31 GB |
| **net on-disk vs vanilla** | **−11.45 GB (−4.55%)** |
| errors / snapshot-mismatches | 0 / 0 |

---

## 2. The archive design — what is stored

**Selection unit.** A subtree rooted at a node of height exactly 3, where height is
counted from the leaves (a leaf node is height 1, a branch directly above leaves is
height 2, a branch above that is height 3). In a hex trie such a subtree spans at most
16 × 16 = 256 leaves. A subtree is moved out only if **all** of its leaves are inactive —
`currentPeriod − leafPeriod ≥ 2`, where each leaf's `LastWrittenPeriod` is read from the
snapshot (periods were injected by the EIP-8188 pipeline; they live in the snapshot, not
the trie).

**What lands in `nodearchive`.** For each moved-out subtree, a contiguous block of
RLP-encoded `Record{Path, Value}` — **one record per leaf; interior branch/extension
nodes are not stored at all.**

- `Path` — the hex-nibble key from the *subtree root* down to the leaf, terminated with
  `0x10` (one nibble per byte). It is relative to the subtree root, not the full 32-byte
  key.
- `Value` — the leaf's value bytes verbatim: the RLP `StateAccount` for an account leaf,
  or the RLP storage-slot value for a storage leaf.

Byte example — a leaf at relative nibbles `a,4` holding value `0x2a`:

```
Record{Path: [0a 04 10], Value: [2a]}
  → RLP: c5 83 0a 04 10 2a        (6 bytes: c5=list(5) · 83=3-byte string · 2a=self-encoding byte)
```

Records for one subtree are concatenated; the stub's `(offset, size)` brackets exactly
that block. The reader streams records out of the block until `size` is consumed.

**Reconstruction on read.** Load the block's records and re-insert each `(Path, Value)`
pair into a fresh, initially-empty MPT (`archiveRecordsToNode` → `insertTrieNode`).
Because the same key/value set has a unique canonical MPT, the rebuilt subtree is
byte-identical to the original and its hash equals the original root hash. This identity
is the built-in integrity check (used at write time — see §7). Interior structure is
recomputed, never stored — that is the whole space saving, bounded by height-3 so a single
reconstruction touches ≤ 256 leaves.

Code: `trie/archive/{archive.go, writer.go}` (format + append-only writer),
`trie/archive_records.go` (record collection, stub encoding, reconstruction).

---

## 3. The stub in pebbledb

When a subtree is moved out, its **root key** is overwritten in place and its interior
keys are deleted. Keys follow geth's path-scheme layout: `"A" + path` for the account
trie, `"O" + ownerHash + path` for a storage trie.

**Stub format — 17 bytes** (`EncodeExpiredNodeBlob`):

```
byte[0]     = 0x00                    marker (a valid MPT node's first byte is ≥ 0xc0, so 0x00 is unambiguous)
byte[1..9]  = offset  (uint64, big-endian)   start of this subtree's record block in nodearchive
byte[9..17] = size    (uint64, big-endian)   byte length of that block
```

**Measured stub counts and size:**

| | stubs | stub bytes | bytes/stub |
|---|---|---|---|
| account | 6,283,805 | 106,824,685 | 17.0 |
| storage | 70,718,375 | 1,202,212,375 | 17.0 |
| **total** | **77,002,180** | **1,309,037,060 (1.31 GB)** | 17.0 |

Every stub is exactly 17 bytes, so the stub keyspace adds back ~1.31 GB of values to
chaindb (plus their keys) against the much larger interior+leaf data removed.

---

## 4. Raw triedb node count and size, before vs after

Measured with `count-trienode-kinds`, which walks every trie-node entry under the `A`
(account) and `O` (storage) pebble prefixes and buckets each by its first byte:

- **normal node** — an ordinary MPT node (branch / extension / leaf), first byte ≥ `0xc0`
  (the tool labels this `standard_rlp`). The only kind present in a vanilla trie.
- **stub** — a 17-byte expired-node marker, first byte `0x00` (§3), created by the move-out.
- hybrids / other — 0 throughout (the archive-expiry format has no hybrid node).

So **`total keys = normal nodes + stubs`**. In the vanilla trie there are no stubs, so
`total keys == normal nodes`; after the move-out they diverge. *Value-bytes* below are
logical — the sum of node value lengths, keys excluded (contrast the physical `du` in §5).

| trie | metric | vanilla (before) | after height-3 | delta |
|---|---|---|---|---|
| account | total keys | 334,723,255 | 291,732,630 | −42,990,625 |
| | — normal nodes | 334,723,255 | 285,448,825 | −49,274,430 |
| | — stubs | 0 | 6,283,805 | +6,283,805 |
| | value-bytes | 38.81 GB | 33.71 GB | −5.10 GB |
| storage | total keys | 1,560,703,684 | 942,684,265 | −618,019,419 |
| | — normal nodes | 1,560,703,684 | 871,965,890 | −688,737,794 |
| | — stubs | 0 | 70,718,375 | +70,718,375 |
| | value-bytes | 109.33 GB | 65.14 GB | −44.18 GB |
| **total** | **total keys** | **1,895,426,939** | **1,234,416,895** | **−661,010,044** |
| | — normal nodes | 1,895,426,939 | 1,157,414,715 | −738,012,224 |
| | — stubs | 0 | 77,002,180 | +77,002,180 |
| | **value-bytes** | **148.13 GB** | **98.86 GB** | **−49.28 GB** |

**Reconcile.** 738,012,224 normal nodes left the trie. Of those, 77,002,180 were subtree
roots **overwritten in place** by stubs (same key, new 17-byte value) and the remaining
661,010,044 were interior+leaf nodes **deleted**. So `total keys` falls by exactly the
661,010,044 deletions (= the converter's `nodes-deleted`), while `normal nodes` falls by
the full 738,012,224. The 49.28 GB of logical value-bytes removed is what the
`nodearchive` (43.09 GB) re-stores in leaves-only form.

---

## 5. Physical on-disk footprint

Compacted pebble SSTs only, ancient freezer excluded (`du -sb --exclude=ancient`), taken
after `db compact` so delete-tombstones are flushed.

| | bytes | GB |
|---|---|---|
| vanilla chaindb baseline (compacted) | 251,754,955,428 | 251.75 |
| chaindb after move-out (compacted) | 197,217,880,771 | 197.22 |
| → chaindb reduction | −54,537,074,657 | −54.54 |
| `nodearchive` | 43,086,318,884 | +43.09 |
| **net total on disk vs vanilla** | **−11,450,755,773** | **−11.45 (−4.55%)** |

The snapshot keyspace (~100 GB, carrying the injected periods) is identical before and
after — the move-out only touches trie nodes — so it cancels in the delta; the −54.54 GB
chaindb change is purely the trie effect. Note the §4 value-bytes (−49.28 GB) are logical
(values only) while this `du` is physical (keys + values + SST overhead), so the two
reductions differ.

---

## 6. End-to-end pipeline (as run)

1. **Isolate.** `ae-work` = a writable LVM thin snapshot of the pristine periods-injected
   datadir (`post-inject`, head 19,999,256), mounted at `/mnt/experiment`.
2. **Baseline.** `geth db compact`; record `du` (251.75 GB) and `count-trienode-kinds`
   (vanilla). Snapshot `ae-vanilla` as a revert point.
3. **Move out.**
   ```
   geth db convert-inactive --format archive --subtree-height 3 \
       --inactive-min-age 2 --scope both --skip-clean-slate \
       --fork-block 17371256 --blocks-per-period 1314000
   ```
   (fork-block + blocks-per-period yield current-period 2.) Streaming: EIP-8188's
   `Identify` walks the trie alongside the period snapshot and invokes an emit callback
   per fully-inactive height-3 subtree; the callback materialises the subtree, writes its
   leaf records to `nodearchive`, stages the 17-byte stub and interior deletes into a
   pebble batch. The archive file is fsync'd once **per batch flush** (not per subtree —
   essential at 77 M subtrees). Wall time ~2h51m.
4. **Measure.** `geth db compact` (flush tombstones); record `du` chaindb + `nodearchive`
   and `count-trienode-kinds` (after).

Tooling on branch `feat/archive-expiry/footprint`. Run artifacts (logs, JSON counts,
byte tallies) are in the go-ethereum repo at
`eip8188-mainnet-runs/ae-footprint-20260603-h3/`.

---

## 7. Correctness

- **Per-subtree hash invariance.** Before any node is deleted, the converter reconstructs
  the subtree from the records it is about to write and checks that the reconstructed hash
  equals the hash the iterator identified. A mismatch aborts that subtree (leaving it
  intact) rather than corrupting state. The full run reported `errors=0` and
  `snapshot-mismatches=0`.
- **Height correctness.** `max-subtree-leaves=73` over the whole run (≤ 256 ⇒ genuine
  height-3). The height counting is also pinned by a deterministic unit test,
  `trie/height_probe_test.go`, which caught an earlier off-by-one (geth's `NodeIterator`
  surfaces a leaf as a `shortNode` step plus a `valueNode` step, so naively counting the
  value added a level and selected height-2 subtrees). On the hex MPT, height-3 means
  ≤ 256 leaves (16²); 8 leaves would be the binary-trie variant, which is not used here.
- **Not exercised on this branch:** end-to-end read-back / resurrection (open the archived
  datadir and resolve expired nodes through the trie). The archive-expiry *read*
  integration was deliberately not ported here because its `expiredNode` type collides
  with the branch's existing EIP-8188 stub; integrity therefore rests on the inline
  write-time hash check above.

---

## 8. Code reference

| area | path |
|---|---|
| archive format + append-only writer | `trie/archive/archive.go`, `trie/archive/writer.go` |
| leaf-record collection, stub encode, reconstruction | `trie/archive_records.go` |
| height-3 inactivity gate (identifier) | `cmd/geth/eip8188/identifier.go` |
| `--format archive` move-out branch | `cmd/geth/eip8188/converter.go` |
| CLI flags (`--format`, `--subtree-height`, `--archive-file`) | `cmd/geth/dbcmd_eip8188.go` |
| height-counting regression test | `trie/height_probe_test.go` |

Branch `feat/archive-expiry/footprint` (worktree `/mnt/disk0/repos/go-ethereum-archive`),
off `feat/eip-8188/inject`. The archive format and reconstruction are ported from
`gballet/archival-command`; the inactivity gate and streaming converter are EIP-8188's.

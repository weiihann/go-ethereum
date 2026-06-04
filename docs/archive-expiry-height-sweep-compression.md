# Archive-expiry move-out: subtree-height sweep + archive compression (MPT, mainnet)

Follow-up to `docs/archive-expiry-height3-report.md`. Goal: push the inactive
move-out toward the **Pareto frontier** — shrink chaindb *and* keep the external
`nodearchive` small. Two orthogonal levers are measured here:

1. **Subtree height** — the granularity of what we move out (height from leaves;
   a fully-inactive subtree of height `N` has ≤ 16^(N-1) leaves). Sweeps the
   chaindb-reduction ↔ archive-size trade.
2. **Archive compression** — the `nodearchive` is raw RLP today, while chaindb
   pebble SSTs are already snappy-compressed, so the archive is the only
   uncompressed side of the ledger.

All runs: mainnet head 19,999,256, `--inactive-min-age 2 --scope both`, on an LVM
thin snapshot of the periods-injected datadir; compacted vanilla baseline =
**251.75 GB** chaindb (pebble SSTs, ancient freezer excluded). Net =
`chaindb_after + archive − baseline`.

## Headline frontier (heights 2–5)

| height | chaindb reduction | raw archive | **chunked archive** | raw net | **chunked net** | subtrees | max leaves |
|---:|---:|---:|---:|---:|---:|---:|---:|
| **2** | **−66.93 GB** | 63.35 GB | **31.39 GB** | −3.58 GB | **−35.54 GB** | 295.1 M | 14 |
| 3 | −54.54 GB | 43.09 GB | 22.59 GB | −11.45 GB | −31.95 GB | 77.0 M | 73 |
| 4 | −41.60 GB | 30.45 GB | 16.44 GB | −11.15 GB | −25.16 GB | 17.5 M | 295 |
| 5 | −40.69 GB | 29.30 GB | 16.01 GB | −11.39 GB | −24.68 GB | 3.44 M | 1118 |

Chunked archive = 1 MB frames, zstd-9 (the realistic compressed on-disk size; see
§Compression). `errors = 0` on every run.

## Reading the frontier

- **Deeper is worse.** Going from height-3 to 4/5, the all-inactive gate qualifies
  far fewer subtrees (a 4096-leaf account region is essentially never 100% cold), so
  chaindb reduction drops from −54.5 GB to ~−41 GB. The smaller archive doesn't make
  up for it. Heights 4 and 5 **converge** (nearly identical chaindb reduction, archive,
  and net) — both collapse onto the same deep cold regions, with height-5 just packing
  them into 5× fewer, larger subtrees (3.4 M vs 17.5 M).
- **Raw net is flat (~−11 GB) across 3–5;** the differences live almost entirely in the
  archive, which compression then shrinks.
- **Compression makes the shallow end win.** zstd halves every archive (~50–55%), so a
  shallower height's *larger* archive penalty mostly evaporates while its *larger*
  chaindb reduction stays. The chunked net therefore peaks at the shallow end of the
  sweep: **height-2 (−35.54 GB) > height-3 (−31.95) ≫ height-4 (−25.16) ≈ height-5
  (−24.68).** Height-2 also gets the best ratio (49.6%) because it carries far more
  account records (EOAs share the same codeHash/storageRoot, which compress well), and it
  frees the most from chaindb (−66.93 GB), so it wins on every axis once compressed.
  Height-2 is the floor: height-1 would move individual leaves with no interior to drop,
  which goes net-negative.

**Conclusion: with a compressed archive, height-2 is the measured sweet spot — net
−35.54 GB (≈ 14% of the 251.75 GB baseline), ~9.9× the uncompressed height-2 saving
(−3.58 GB).** Raw, height-3 looked best; compression flips that, because it absorbs
height-2's much larger archive (63 GB → 31 GB) while keeping its much larger chaindb
reduction (−66.93 GB). Going deeper than 3 is counterproductive, and the shallow lever
bottoms out at height-2 (height-1 has no interior to drop, so it goes net-negative).

## Compression: why chunked

The archive is a sequence of per-subtree blocks (a height-3 block ≈ 560 B). Compression
ratio is dominated by the **size of the unit compressed together**, measured on the
height-3 archive (43.09 GB raw):

| compression unit | archive size | reduction |
|---|---:|---:|
| per-block ~256 B | 98.7% | ~1% |
| per-block ~560 B (one height-3 subtree) | ~70–85% | ~15–30% |
| per-block 4 KB | 65.5% | 35% |
| per-block 16 KB | 59.3% | 41% |
| **chunked, 1 MB frames (zstd-9)** | **52.4%** | **48%** |
| whole-file (zstd-9 / zstd-19) | 51.0% / 47.9% | 49% / 52% |

Findings:

- **Per-block compression is weak** — a single subtree's block is too small for zstd to
  find redundancy. It improves with deeper heights (bigger blocks) but never reaches the
  whole-file ratio.
- **A shared dictionary does not help.** Training a 110 KB zstd dictionary on sampled
  record blocks and compressing each block with it moved the ratio by ≤ 4 points (and
  sometimes worse). The storage values that dominate the archive are high-entropy
  (slot values, 32-byte hashes), so there's little cross-record redundancy for a
  dictionary to capture.
- **Chunked compression captures the gain.** Batching ~1 MB of consecutive subtree blocks
  into one zstd frame lands at 52.4% — within ~1.5 points of whole-file — while still
  allowing a single subtree to be resurrected by decompressing only its one frame.
  Because the archive holds *inactive* state, reads are rare, so the per-read cost of
  decompressing one ~1 MB frame is acceptable.

### Stub / format impact of chunked

In a real chunked archive the stub's *meaning* changes but **not its 17-byte size**:

```
raw:     [0x00 | fileOffset:8       | size:8 ]
chunked: [0x00 | chunkFileOffset:8  | offsetInChunk:4 | size:4 ]
```

So chaindb footprint is unchanged; only the archive file is compressed, plus a tiny
chunk-offset table (~0.4 MB for 77 M subtrees). The figures above are **measurements**
of the chunked archive size (the raw archive compressed in 1 MB frames) on the
unmodified stubs, which is faithful precisely because the stub stays 17 bytes — the
chaindb side is identical for raw vs chunked. A production chunked writer/reader is a
small follow-on (batch blocks → frame, reinterpret the stub fields, decompress one
frame on read).

## Method

Per height N: revert the work volume to the compacted vanilla snapshot →
`db convert-inactive --format archive --subtree-height N --skip-clean-slate
--fork-block 17371256 --blocks-per-period 1314000` (→ current-period 2) →
`db compact` → `du --exclude=ancient` chaindb + `du` raw archive +
chunked-compress measure (1 MB/zstd-9) + `count-trienode-kinds`. Per-subtree
hash-invariance is checked before any delete (so `errors=0` means every moved subtree
reconstructs to its original root). Height correctness: `max-subtree-leaves ≤ 16^(N-1)`
plus `trie/height_probe_test.go`.

Run artifacts: `eip8188-mainnet-runs/ae-footprint-20260603-h{3,4,5}/`.
Branch `feat/archive-expiry/footprint` (worktree `/mnt/disk0/repos/go-ethereum-archive`).

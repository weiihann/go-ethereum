# EIP-8188 prototype — pipeline-stage data synthesis

**Run:** `stats-20260501-0553`
**Range:** mainnet head 19,999,256 → 22,627,956
**Stages measured:** post-inject, post-convert-both, post-import
**Synth baseline:** vanilla (computed from post-inject by subtracting period-marker overhead)

## Trie composition

All sizes are chaindb on-disk bytes; counts are unique trie-node keys (stubs collapse a subtree into one key).

### Account trie

| Kind | Stage 3: post-inject | Stage 2: post-convert-both | Stage 1: post-import |
|---|---:|---:|---:|
| standard RLP — count | 334,723,255 | 67,333,944 | 165,567,866 |
| standard RLP — bytes | 38.81 GB | 10.71 GB | 21.26 GB |
| stubs — count | 0 | 135,744,840 | 107,598,711 |
| stubs — bytes | 0 | 2.31 GB | 1.83 GB |
| hybrids — count | 0 | 0 | 13,671,487 |
| hybrids — bytes | 0 | 0 | 2.53 GB |
| **total — count** | **334,723,255** | **203,078,784** | **286,838,064** |
| **total — bytes** | **38.81 GB** | **13.01 GB** | **25.63 GB** |

### Storage trie

| Kind | Stage 3: post-inject | Stage 2: post-convert-both | Stage 1: post-import |
|---|---:|---:|---:|
| standard RLP — count | 1,560,703,684 | 171,150,053 | 444,675,068 |
| standard RLP — bytes | 109.33 GB | 16.60 GB | 34.58 GB |
| stubs — count | 0 | 180,529,497 | 136,960,198 |
| stubs — bytes | 0 | 3.07 GB | 2.33 GB |
| hybrids — count | 0 | 0 | 18,488,661 |
| hybrids — bytes | 0 | 0 | 4.36 GB |
| **total — count** | **1,560,703,684** | **351,679,550** | **600,123,927** |
| **total — bytes** | **109.33 GB** | **19.67 GB** | **41.26 GB** |

### inactive.bin

| | Stage 3: post-inject | Stage 2: post-convert-both | Stage 1: post-import |
|---|---:|---:|---:|
| inactive.bin size | absent | 162.39 GB | 162.39 GB |

(unchanged stage 2 → 1 — lazy materialisation reads from the file but never writes new bytes)

## Snapshot bytes — periods overhead

Bytes split into key vs value contributions because period markers only modify value-encoding bytes — keys (33 B per account, 65 B per storage slot) are unaffected.

| | Stage 3: post-inject (measured) | Synth vanilla (computed) |
|---|---:|---:|
| account_snapshot_key_bytes | 7.58 GB | 7.58 GB (unchanged) |
| account_snapshot_value_bytes | 3.77 GB | ~3.72–3.74 GB |
| storage_snapshot_key_bytes | 69.74 GB | 69.74 GB (unchanged) |
| storage_snapshot_value_bytes | 13.32 GB | ~13.08–13.16 GB |
| **total snapshot bytes** | **101.38 GB** | **~101.06–101.18 GB** |

Synthesis assumptions (per-record overhead when `LastWrittenPeriod > 0`, value bytes only):
- Accounts: ~1–2 bytes each (uint32 period byte + occasional list-length-prefix expansion)
- Storage slots: ~2–3 bytes each (list framing replaces byte-string framing + period byte)
- 32,470,464 accounts × 1.5 B avg ≈ 49 MB
- 102,886,481 slots × 2.5 B avg ≈ 257 MB
- **Total period overhead ≈ 200–320 MB (~0.2–0.3% of total snapshot bytes)**

**Why the snapshot is so big:** 1.15 B storage records × 65 B keys (1-byte prefix + 32-byte addrHash + 32-byte slotHash) = 75 GB of keys alone. The actual value content is just 14 GB. Storage keys dominate the snapshot footprint by ~5x over their values. Note these are *raw* logical bytes — pebble applies key-prefix compression at the SST level, so on-disk size is somewhat less.

## Headline deltas

### Period injection cost (vanilla → post-inject)

- Trie: byte-identical (periods live entirely in the snapshot keyspace, never touch trie nodes)
- Snapshot: ~+200–320 MB on a base of ~101 GB — well under 0.5%
- **Verdict: period injection is essentially free.** (Per-record overhead is 1–3 bytes only when `LastWrittenPeriod > 0`; period-zero records encode byte-identically to legacy.)

### Conversion impact (post-inject → post-convert-both)

| | Account | Storage | Combined |
|---|---:|---:|---:|
| chaindb trie before | 38.81 GB | 109.33 GB | 148.14 GB |
| chaindb trie after | 13.01 GB | 19.67 GB | 32.68 GB |
| chaindb trie delta | -25.80 GB | -89.66 GB | **-115.46 GB** |
| inactive.bin delta | — | — | **+162.39 GB** |
| **Net total disk delta** | | | **+46.93 GB** |

Trie-key fan-in (stub compression ratio):

| | Account | Storage |
|---|---:|---:|
| keys removed | 267,389,311 | 1,389,553,631 |
| stubs written | 135,744,840 | 180,529,497 |
| avg subtree size | **1.97 keys/stub** | **7.70 keys/stub** |

Storage subtrees are ~4x denser than account subtrees — the convert-inactive identification step finds bigger inactive subtrees in storage tries (which makes intuitive sense: dormant contracts have whole untouched storage tries while account-trie inactivity is more piecewise).

### Lazy-materialisation cost (post-convert-both → post-import, 2.6M block import)

| | Account | Storage | Combined |
|---|---:|---:|---:|
| stubs touched (became hybrids) | 28,145,896 | 43,569,299 | 71,715,195 |
| hybrids appeared | 13,671,487 | 18,488,661 | 32,160,148 |
| new standard RLP nodes | +98,233,922 | +273,525,015 | +371,758,937 |
| chaindb trie delta | +12.62 GB | +21.59 GB | **+34.21 GB** |
| inactive.bin delta | — | — | **0 GB** |

Observations:
- Hybrids:stubs-touched ratio ≈ 0.45 — each touched stub generated less than 1 hybrid on average. This means lazy mat sometimes promotes a stub directly to standard RLP (when the touched subtree is fully unfolded) without leaving a hybrid intermediate. Sometimes one hybrid covers multiple touched stubs sharing an ancestor.
- Stub fan-out for new RLP: each touched stub produced ~5.2 standard RLP descendants on average (372M new RLP / 71.7M touched stubs). Roughly the average subtree depth × branching factor.
- 80% of original stubs survived the entire import (107.6M / 135.7M for account, 137.0M / 180.5M for storage) — validates the EIP-8188 design assumption that *most* state stays inactive across active development.

### End-to-end EIP-8188 vs vanilla (estimated)

We can't directly measure vanilla post-import (no pre-inject snapshot), but a reasoned estimate:

| | Estimated vanilla (post-import) | Measured EIP-8188 (post-import) | Delta |
|---|---:|---:|---:|
| chaindb trie bytes | ~180–200 GB | 66.89 GB | **-113 to -133 GB** |
| inactive.bin | 0 | 162.39 GB | **+162 GB** |
| Net total disk | ~180–200 GB | 229.28 GB | **+29 to +49 GB** |

Vanilla estimate methodology: stage 3 trie size (148.1 GB) + an import-period addition equivalent to what we measured at EIP-8188 (34 GB) plus a correction for paths that would have been new RLP but were lazy-materialised here.

**Verdict:** EIP-8188 trades ~30–50 GB more total disk for a ~70% reduction in chaindb (working set) bytes. The cold inactive.bin can live on slower or remote storage; the hot chaindb shrinks dramatically.

## Operational observations

- **inactive.bin grew exactly once (during convert) and never again.** Lazy materialisation only reads from it — every byte added during the original convert ran. This means inactive.bin is content-addressable, append-only, and a perfect candidate for offload to cold storage.
- **Hybrid encoding is heavier than expected.** Average hybrid is 185 B (account) / 235 B (storage) vs ~17 B for stubs and ~78–128 B for standard RLP. Hybrids carry both the live MPT structure inline AND the residual cold-child metadata, so they're a fat composite. ~2.5x the size of a comparable standard branch node.
- **Chaindb hot path more than halved.** Pre-EIP-8188 trie chaindb at block 19,999,256 was 148 GB. Post-import (head 22.6M) chaindb trie is 67 GB — even after 2.6M blocks of new state. This is the headline practical win.

## Files captured

- `01-post-import/counts.json` — extended count-trienode-kinds at post-import
- `01-post-import/inactive.bin.stat` — inactive.bin size at post-import
- `02-post-convert-rsync.log` — rsync log of datadir → /mnt/disk1/eip8188-bench/post-convert-both/
- `02-post-convert-both/counts.json` — counts at post-convert-both
- `02-post-convert-both/inactive.bin.stat`
- `03-post-inject/counts.json` — counts at post-inject
- `03-post-inject/periods.json` — extended inspect-periods (with snapshot byte totals)
- `03-post-inject/inactive.bin.absent` — confirms no inactive.bin at post-inject
- `04-synthesis.md` — this file

## Bench machine handoff

`/mnt/disk1/eip8188-bench/post-convert-both/` contains a bit-exact rsync of the post-convert-both datadir (head 19,999,256, periods + stubs + 162 GB inactive.bin). 689 GB on disk.

Block stream `/mnt/experiment/19999256_22627956.gz` (92 GB) is *not* included — transfer separately if the benchmark machine needs it.

# Shrinking Ethereum state: period-injection + inactive move-out, end to end

This report walks through a three-step experiment on a real Ethereum mainnet
node, in plain terms, with the measured numbers at each step. It is written for a
reader who is **not** already familiar with this work.

- **Step 1 — Baseline:** the node as it normally is.
- **Step 2 — Period injection:** tag every piece of state with *when it was last
  used*, so we can tell what is "cold".
- **Step 3 — Move inactive subtrees out:** physically move the cold parts out of
  the main database into a separate file, shrinking the part the node has to keep hot.

All measurements are on the same datadir: mainnet at block height **19,999,256**.

---

## Background: how a node stores "the state"

The **state** is every account balance/nonce and every contract's storage. A
go-ethereum node keeps it in **two** structures inside its key-value database
(pebble):

```
   Ethereum state @ block 19,999,256
   ┌────────────────────────────────────────────────────────────────┐
   │  (1) MERKLE-PATRICIA TRIE  — the authenticated tree.            │
   │      A tree of "nodes" (branches + leaves) whose root hash is   │
   │      the block's stateRoot. Proves the state is correct.        │
   │                                                                 │
   │            account trie root                                    │
   │                  │                                              │
   │            ┌─────┴─────┐         (each contract account also    │
   │         branch       branch       points to its own storage     │
   │         /    \        /   \        trie, same shape)            │
   │      leaf   branch  leaf  leaf                                  │
   │             /    \                                              │
   │          leaf    leaf   ← leaves hold the actual account/slot   │
   │                                                                 │
   │  (2) SNAPSHOT — a flat key→value copy of just the leaves.       │
   │      No tree walk needed; used for fast reads.                  │
   │        accountHash → account                                    │
   │        accountHash+slotHash → slot value                        │
   └────────────────────────────────────────────────────────────────┘
```

Two facts drive everything below:

1. The **trie** is big and dominated by interior structure; the **snapshot** is big
   and dominated by its long keys (a storage key is `1 + 32 + 32 = 65` bytes).
2. Most of the state is **cold** — never touched in a long time. If we knew which
   parts were cold, we could move them out of the hot database.

"Periods" are simply **time windows** (~6 months each, here `1,314,000` blocks).
A leaf is **inactive** if it hasn't been written for at least `minAge` periods.

---

## Step 1 — Baseline (no period information)

The node as-is. Sizes are *logical* bytes (sum of record contents) unless noted;
the physical on-disk compacted chaindb is ~251.75 GB.

| component | count | size |
|---|---:|---:|
| trie nodes (account) | 334.7 M | 38.81 GB |
| trie nodes (storage) | 1,560.7 M | 109.33 GB |
| **trie total** | **1,895.4 M nodes** | **148.13 GB** |
| snapshot — account (keys / values) | — | 7.58 / 3.77 GB |
| snapshot — storage (keys / values) | — | 69.74 / 13.32 GB |
| **snapshot total** | ~1.15 B records | **101.38 GB** |

At this point the node has **no idea** which leaves are cold — there's no
timestamp on anything. That's what Step 2 adds.

---

## Step 2 — Period injection (tag state with *when it was last used*)

### What it does
For every account and storage slot, record the **most recent period** in which it
was written, as a number called `LastWrittenPeriod`. This is written **only into
the snapshot**; the trie is never touched.

### Where the timestamps come from
An external **source** (a database of historical state-access "diffs" — which
address/slot changed at which block) streams `(address-or-slot, block)` pairs. The
injector converts each block to a period and stamps the snapshot record.

```
   source of access history            injector                   snapshot (pebble)
   ┌──────────────────────┐    (addr, block)   ┌───────────┐   read record → set period → write
   │ diff: addr A @ block  ├──────────────────►│ period =   │   ┌───────────────────────────────┐
   │ diff: slot S @ block  │                    │  (block −  │   │ accountHash → [account, PERIOD]│
   │ …                     │                    │  forkBlk)/ │──►│ slotHash    → [value,   PERIOD]│
   └──────────────────────┘                     │  perLen   │   └───────────────────────────────┘
                                                 └───────────┘   (monotonic: never lower a period)
```

- `ComputePeriod(block) = (block − forkBlock) / blocksPerPeriod`. Here
  `forkBlock = 17,371,256`, `blocksPerPeriod = 1,314,000`, so the head sits in
  **period 2**.
- The update is **monotonic** — it only ever raises a record's period — so the
  source may emit diffs in any order, in batches, with retries.

### How the period is stored (and why it's nearly free)
The period is an **optional** field appended to the record. When a record's period
is `0` (never written in the tracked range), the encoding is **byte-identical** to
a legacy record — so only *recently-written* records grow at all.

```
   account snapshot record:
     before:  RLP[ nonce, balance, storageRoot, codeHash ]
     after :  RLP[ nonce, balance, storageRoot, codeHash, period ]   (+~1–2 bytes)

   storage slot record:
     before:  RLP("value bytes")                       ← plain string
     after :  RLP[ "value bytes", period ]             ← 2-item list (+~2–3 bytes)
```

### Cost (Step 1 → Step 2)

| | trie | snapshot |
|---|---|---|
| change | **none** (byte-identical) | **+~0.2–0.3 GB** (on ~32.5 M accounts + recently-written slots) |
| as % | 0% | **< 0.5%** of the 101 GB snapshot |

**Verdict: period injection is essentially free**, and it's the enabler for Step 3 —
now every leaf carries enough information to decide whether it's cold.

---

## Step 3 — Move inactive subtrees out

### The idea
Find chunks of the trie whose leaves are **all** inactive, move their leaf data
into an external file (`nodearchive`), and replace the whole chunk in the main
database with a tiny **17-byte pointer** ("stub"). The chunk's interior nodes are
**deleted** — they're cheap to recompute from the leaves when (rarely) needed.

### What gets moved: a "height-3 subtree"
We move subtrees of **height 3** (the leaves are 3 levels below the subtree root)
whose every leaf is inactive. Height-3 caps how much must be rebuilt on a read.

```
   BEFORE (in the main trie / chaindb):        AFTER:

        N  (subtree root, height 3)               N  →  17-byte stub  ──┐
       / \                                                              │
   branch branch     ← interior nodes            (interior + leaves     │
   /  \    /  \         (deleted)                  removed from chaindb) │
 leaf leaf leaf leaf  ← all inactive                                    ▼
                                                  nodearchive (separate file):
                                                  ┌────────────────────────────┐
                                                  │ [leafRecord][leafRecord]…   │
                                                  └────────────────────────────┘
```

### What is stored where

- **In chaindb (the stub, 17 bytes):** `[0x00 marker | fileOffset:8 | size:8]`.
  `0x00` can't collide with a real trie node (those start at `0xc0+`). The offset
  and size bracket this subtree's records in the archive file.
- **In `nodearchive` (leaves only):** for each leaf, one record
  `RLP[ pathToLeaf, leafValue ]`. **No interior nodes are stored** — only the
  leaves and their relative paths.
- **On read (resurrection):** load the records, re-insert each `(path, value)` into
  a fresh mini-trie; the rebuilt subtree is identical to the original, and its hash
  matches the stub's expected root (a built-in integrity check).

```
   read of an expired leaf:
     stub (offset,size) → read records from nodearchive → rebuild subtree → return value
                                                          (cheap: ≤256 leaves)
```

### How the cold subtrees are found
A single streaming pass walks the trie and the period-stamped snapshot together.
For each height-3 node it asks "are *all* my leaves inactive?" (using the periods
from Step 2: inactive ⇔ `currentPeriod − leafPeriod ≥ 2`). If yes, it's moved; the
move is verified (rebuilt hash == original) **before** anything is deleted.

### Result (Step 2 → Step 3)

| | trie (chaindb) | snapshot | new file |
|---|---|---|---|
| trie nodes | 1,895.4 M → **1,234.4 M** (−661 M, −35%) | unchanged | — |
| of which stubs | 0 → **77.0 M** (17 B each = 1.31 GB) | — | — |
| `nodearchive` | — | — | **43.09 GB** raw |

**Physical on-disk footprint** (compacted pebble SSTs, ancient freezer excluded):

```
                         chaindb        + archive     = total      vs baseline
   baseline (Step 2)     251.75 GB        —             251.75 GB    —
   after move-out        197.22 GB        43.09 GB      240.31 GB    −11.45 GB
   …with the archive
   compressed (zstd)     197.22 GB        22.59 GB      219.81 GB    −31.95 GB
```

The archive is compressed in ~1 MB chunks (one zstd frame per chunk + a small
offset table), which roughly halves it while still letting a single subtree be
resurrected by decompressing just its one chunk. (Compression is the single biggest
lever; per-leaf or per-subtree compression doesn't work — the blocks are too small.)

---

## End-to-end summary

```
   STEP 1 (baseline)         STEP 2 (period inject)        STEP 3 (move inactive out)
   ─────────────────         ──────────────────────        ──────────────────────────
   trie     148.1 GB   ──►   trie     148.1 GB   (same) ──► trie      ~99 GB  + 77 M stubs
   snapshot 101.4 GB   ──►   snapshot 101.6 GB  (+0.3)  ──► snapshot  101.6 GB (same)
   ── no timestamps ──       ── every leaf knows its ──     nodearchive 43 GB raw
                                last-used period             (22.6 GB compressed)
```

| | Step 1 baseline | Step 2 post-inject | Step 3 post-move-out (height-3) |
|---|---|---|---|
| trie nodes | 1,895.4 M | 1,895.4 M | **1,234.4 M** (−35%) |
| snapshot | 101.38 GB | ~101.6 GB (+<0.5%) | ~101.6 GB |
| external archive | — | — | 43.09 GB raw / **22.59 GB** zstd |
| chaindb (physical, compacted) | 251.75 GB | 251.75 GB | **197.22 GB** (−54.5) |
| **net total disk vs baseline** | — | ~+0.3 GB | **−11.45 GB raw / −31.95 GB compressed** |

**Takeaways**

- **Inject is free and orthogonal.** It only stamps the snapshot (~+0.3 GB), never
  the trie, and gives every leaf a "last used" period — the key that unlocks Step 3.
- **The move-out shrinks the hot database** by deleting cold interior nodes and
  relocating cold leaves: chaindb drops 54.5 GB; the leaves land in an external,
  cold-storage-friendly `nodearchive`.
- **Net on-disk shrinks** even counting the archive: −11.45 GB raw, and **−31.95 GB
  (~13%)** once the archive is chunk-compressed — at the cost of recomputing a small
  subtree on the rare read of expired state.

*Methodology:* run on an LVM thin-snapshot of the injected datadir; sizes are
`du` of compacted pebble SSTs (deletes become tombstones until compaction, and the
constant `ancient/` freezer is excluded). Every moved subtree is hash-verified
before deletion, so `errors = 0` means each one reconstructs exactly. Snapshot/
period figures are from the earlier injection run on this same datadir; trie and
move-out figures are from the height-3 run in `eip8188-mainnet-runs/`.

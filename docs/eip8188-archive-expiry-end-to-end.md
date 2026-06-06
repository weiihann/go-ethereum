# How much cold state can EIP-8188 move out?

We ran a three-step experiment on a real mainnet go-ethereum (geth) node to see how much disk you save
by pulling "cold" state out of the hot database. This writes up what each step does
and what it actually measured.

The three steps:

- **Baseline.** The normal geth node.
- **Period injection.** Tag every account and slot with when it was last used, so we
  can tell what is cold.
- **Move inactive state out.** Pull the cold parts out of the main database into a
  flat file, shrinking what the node has to keep hot.

Everything below is measured on one data directory: mainnet at block 19,999,256.

---

## How geth stores the state

The state is every account (balance, nonce, code, storage root) plus every contract's
storage slots. geth keeps it twice inside its key-value store (PebbleDB).

**1. The Merkle-Patricia Trie (MPT).** The authenticated tree whose root hash is the
block's state root.

```
   account trie  (root hash = stateRoot)
            |
         (branch)                 
        /        \
  (extension)   (branch)          
      |          /     \
   (branch)   (leaf)  (leaf)      
    /    \
 (leaf) (leaf)

   every contract account's storage is its own trie of the same shape.
```

**2. The snapshot.** A flat key-to-value copy of just the leaves, so a read does not
have to walk the tree.

```
   accountHash             -> account
   accountHash + slotHash  -> slot value
```

---

## Step 1: baseline

The node at block 19,999,256. Sizes are logical bytes (sum of record contents) unless noted. The
physical on-disk compacted PebbleDB is about 251.75 GB.

| component | count | size |
|---|---:|---:|
| trie nodes (account) | 334.7 M | 38.81 GB |
| trie nodes (storage) | 1,560.7 M | 109.33 GB |
| **trie total** | **1,895.4 M nodes** | **148.13 GB** |
| snapshot, account (keys / values) | - | 7.58 / 3.77 GB |
| snapshot, storage (keys / values) | - | 69.74 / 13.32 GB |
| **snapshot total** | ~1.15 B records | **101.38 GB** |

At this point the node has no way to tell which leaves are cold. Nothing carries a
timestamp. That is what step 2 fixes.

---

## Step 2: period injection

**What it does.** For each account and slot, store the most recent period it was
written. This goes only into the snapshot. A "period" is just a time window, here 1,314,000 blocks (about six months). A leaf is inactive if it has not been written for at least `minAge` periods (here it's 2, so 1-year inactivity in total).

**Where the timestamps come from.** An external source (a database of historical
access "diffs", meaning which address or slot changed at which block) streams
`(key, block)` pairs. The injector turns each block into a period and stamps the
matching snapshot record. In this experiment, we used [Xatu](https://github.com/ethpandaops/xatu) as the primary data source.

```
    access-history source                     injector             snapshot (pebble)

   +---------------------+  (key, block)   +-------------+   read -> set period -> write
   | addr A wrote @ blk  | --------------> | period =    |   +---------------------------+
   | slot S wrote @ blk  |                 | (blk - fork)| ->| accountHash -> [acct, P]  |
   | ...                 |                 |   / perLen  |   | slotHash    -> [value, P] |
   +---------------------+                 +-------------+   +---------------------------+
                                                              (only ever raises a period)
```

`ComputePeriod(block) = (block - forkBlock) / blocksPerPeriod`. With `forkBlock`
17,371,256 and `blocksPerPeriod` 1,314,000, the head sits in period 2. The update only
ever raises a record's period, so the source can emit diffs in any order, in batches,
with retries.

**How it is stored, and why it barely costs anything.** The period is an optional
trailing RLP field. When it is 0 (the record was never written in the tracked range)
the bytes are identical to a legacy record, so only recently-written records grow.

```
   account record:
     before:  RLP[ nonce, balance, storageRoot, codeHash ]
     after:   RLP[ nonce, balance, storageRoot, codeHash, period ]   (+1 to 2 bytes)

   storage slot record:
     before:  RLP("value")                a plain string
     after:   RLP[ "value", period ]      a 2-item list  (+2 to 3 bytes)
```

**Cost (step 1 to step 2).**

| | trie | snapshot |
|---|---|---|
| change | none, byte-identical | +0.2 to 0.3 GB (on ~32.5 M accounts plus recently-written slots) |
| as % | 0% | under 0.5% of the 101 GB snapshot |

---

## Step 3: move inactive state out

Now that every leaf carries a last-used period (step 2), we can take the cold parts of
the state out of the main database and put them in a flat file, leaving the node
a smaller hot working set. There are two ways to do this, and the gap between them is the
point of this section.

### The naive approach: move every cold node, as-is

The obvious move is to take every inactive trie node out into a flat file, with
interior branches and extensions included. A 17-byte stub replaces each moved subtree
root in the main database, and a read follows the stub into the flat file where the
original nodes are waiting.

This empties most of the cold state out of the hot database, so the main database shrinks
a lot. The catch is the flat file. Copying the full tree structure verbatim makes it
enormous, larger than the space freed back in the main database, so the total on disk
goes up rather than down. The numbers are in the comparison at the end of this section.

### The subtree approach: move fully-inactive subtrees, leaves only

**The idea.** Find chunks of the trie whose leaves are all inactive, write their leaf
data to a flat file (`nodearchive`), and replace the whole chunk with a 17-byte
pointer (a "stub"). The interior nodes get deleted. They are cheap to rebuild from the
leaves on the rare read.

**What gets moved, and the height param.** We move subtrees of a fixed height whose
every leaf is inactive. Height is counted from the leaves, so a leaf node is height 1, a
branch directly above leaves is height 2, and the height-3 root in the picture below sits
two levels above its leaves (up to 16^(N-1) leaves under it). A deeper subtree holds more
leaves, so it bounds how much you rebuild, but a deeper subtree is much less
likely to be *entirely* cold. We swept heights 2, 3, 4 and 5.

```
   BEFORE (in PebbleDB)                  AFTER

        N (subtree root, height 3)       N  ->  17-byte stub  --+
       / \                                                      |
  (branch)(branch)   interior nodes      subtree gone from      |
   / \     / \         (deleted)         PebbleDB                v
 leaf leaf leaf leaf  (all inactive)            nodearchive (flat file)
                                                +------------------------+
                                                | [leaf][leaf][leaf] ... |
                                                +------------------------+
```

**What is stored where.**

- The stub, 17 bytes in PebbleDB: `[0x00 marker | fileOffset:8 | size:8]`. A real trie
  node's first byte is `0xc0` or higher, so `0x00` can never be mistaken for one. The
  offset and size bracket this subtree's records in the file.
- The archive, leaves only: one RLP record per leaf, `[pathToLeaf, leafValue]`. No
  interior nodes, just the leaves and their relative paths.
- Reading it back: load the records, re-insert each `(path, value)` into a fresh
  mini-trie. The rebuilt subtree is identical to the original, and its hash has to
  match the one the stub expects, which doubles as a corruption check.

```
   write of an expired leaf:
     stub(offset, size) -> read records -> rebuild subtree -> modify path
                                           (cheap: at most 256 leaves)
```

**How the cold subtrees are found.** One streaming pass walks the trie and the
period-stamped snapshot side by side. At each candidate node it checks whether all
leaves under it are inactive (`currentPeriod - leafPeriod >= 2`, using the periods from
step 2). If they are, it moves the subtree, and the rebuilt-hash check runs before
anything is deleted.

**Results, sweeping the height.** Shallower subtrees qualify more often, because a small
group is more likely to be entirely cold, so more moves out, but they leave a bigger raw
archive. Deeper subtrees move out far less. The snapshot does not change at any height.
Net is `PebbleDB_after + archive - 251.75 GB baseline`, and the "zstd" column is the
chunk-compressed archive (explained below).

| height | trie nodes after | subtrees moved (stubs) | PebbleDB reduction | archive raw | archive zstd | net raw | **net zstd** |
|---:|---:|---:|---:|---:|---:|---:|---:|
| **2** | 1,159.3 M | 295.1 M | **-66.93 GB (-26.6%)** | 63.35 GB | 31.39 GB | -3.58 (-1.4%) | **-35.54 (-14.1%)** |
| 3 | 1,234.4 M | 77.0 M | -54.54 GB (-21.7%) | 43.09 GB | 22.59 GB | -11.45 (-4.5%) | -31.95 (-12.7%) |
| 4 | 1,380.6 M | 17.5 M | -41.60 GB (-16.5%) | 30.45 GB | 16.44 GB | -11.15 (-4.4%) | -25.16 (-10.0%) |
| 5 | 1,389.7 M | 3.44 M | -40.69 GB (-16.2%) | 29.30 GB | 16.01 GB | -11.39 (-4.5%) | -24.68 (-9.8%) |

The answer flips depending on whether you compress the archive. On the raw archive,
height 3 is best (-11.45), because height 2's archive is too big to pay for itself. But
the archive is exactly the thing we compress, and once we do, height 2 takes the lead.
Height 2 has the largest PebbleDB reduction (-66.93 GB), and it carries far more account
records, which compress well, so it gets the best ratio too (49.6%). That lands the best
net by a clear margin, -35.54 GB. Going the other way, heights 4 and 5 move out far less,
only 17.5 M and 3.4 M subtrees qualify against 295 M at height 2, and they converge on the
same deep cold regions, so they net worse. This is the best we can do *if* every moved chunk
must be exactly one height. Relaxing that constraint does much better, which we come to after
the compression detail below.

Physical footprint at height 2 (compacted pebble SSTs, ancient freezer excluded):

```
                       PebbleDB     + archive    = total       vs baseline
   baseline            251.75 GB     -            251.75 GB     -
   after move-out      184.83 GB     63.35 GB     248.18 GB     -3.58 GB (-1.4%)
   archive compressed  184.83 GB     31.39 GB     216.22 GB     -35.54 GB (-14.1%)
```

The archive is compressed in roughly 1 MB chunks, one zstd frame per chunk plus a
small offset table, which about halves it while still letting you resurrect a single
subtree by decompressing just its chunk. Compression turned out to be the biggest lever
by far. Two things that did not work: compressing each leaf or each subtree on its own,
because the blocks are too small for zstd to find anything, and a shared dictionary
trained on sample records, which moved the number by a couple of points and sometimes
made it worse. You have to compress many subtrees together.

### Up to a height, not exactly that height

Everything above made each moved chunk exactly N levels tall. That quietly wastes coverage.
If a cold clump is only 2 levels tall but sits under a parent that also has a hot leaf, the
height-3 run skips it, and those cold leaves stay in the hot database.

So we relaxed the rule. Instead of exactly N levels, we take the largest fully cold chunk we
can find but never let it grow past N levels, and we skip the tiniest ones, the lone cold
leaves, because moving a single leaf costs a 17 byte stub plus an archive record while saving
no interior, a net loss. In the tool this is two knobs, a minimum height (we set it to 2) and
a maximum height (the cap, which we sweep).

The floor of 2 means every cap setting moves the same leaves. The cap only changes how those
leaves are grouped. A cold region three levels tall becomes one chunk instead of up to sixteen
smaller ones. Same data relocated, but fewer 17 byte stubs left behind, more interior nodes
deleted, and larger archive blocks that compress a little better.

Sweeping the cap from 2 to 5, floor fixed at 2:

| cap (floor 2) | PebbleDB reduction | archive raw | archive zstd | net raw | net zstd | stubs | max leaves rebuilt |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 2 | -66.93 GB | 63.35 GB | 31.39 GB | -3.58 | -35.54 (-14.1%) | 295.1 M | 16 |
| 3 | -95.86 GB | 82.96 GB | 41.58 GB | -12.90 | **-54.28 (-21.6%)** | 239.6 M | 73 |
| 4 | -107.37 GB | 89.19 GB | 44.79 GB | -18.18 | -62.58 (-24.9%) | 158.4 M | 295 |
| 5 | -110.03 GB | 89.98 GB | 45.16 GB | -20.05 | -64.87 (-25.8%) | 116.4 M | 1118 |

The cap-2 row is the same setting as exactly height 2 above, and it reproduced that run to
the byte, which is how we know the new code is correct.

This is a different league. The old best was -35.54 GB at height 2. Just moving the cap to 3
lands at -54.28 GB, an extra 18.7 GB, because we keep the cold height-2 coverage that exactly
height 3 used to throw away and we delete far more interior on top of it. The gains then
shrink each step, -18.7 then -8.3 then -2.3 GB, while the worst case rebuild grows from 16
leaves to over a thousand. Cap 3 is the knee, the most saving for the least read cost. Cap 4
buys another 8 GB if rebuilding a few hundred leaves on a cold read is acceptable. Cap 5 is
barely worth it.

### Naive vs subtree

Side by side on the same datadir, with logical (value-byte) figures so the two are
measured the same way:

| | naive (every cold node) | subtree (height 3) |
|---|---:|---:|
| granularity | maximal, every inactive node | fully-inactive height-3 subtrees |
| stubs written | 316.3 M | 77.0 M |
| nodes moved out | ~1.66 B | 661 M |
| stored in the flat file | full subtree structure | leaves only, interior rebuilt on read |
| trie value bytes | 148.14 -> 32.68 GB (-115.46) | 148.13 -> 98.86 GB (-49.28) |
| flat file | **162.39 GB** | **43.09 GB** raw / **22.59 GB** zstd |
| net (trie delta + flat file) | **+46.93 GB (+18.6%)** | **-6.19 GB (-2.5%)** raw / **-26.69 GB (-10.6%)** zstd |

The main point is the flat file. The naive approach keeps the full structure, which
costs 162 GB, close to four times the leaves-only archive and over seven times the
compressed one. It moves about 2.5x as many nodes, so its main-database saving is larger,
but the flat file outgrows that saving and total disk goes *up* by about 47 GB. The
subtree approach drops the intermediate nodes nd rebuilds it on read, so its flat file stays small
and total disk comes *down*.

---

## End to end summary

```
   STEP 1 baseline          STEP 2 period inject        STEP 3 move inactive out
   ---------------          --------------------        ------------------------
   trie     148.1 GB  --->  trie     148.1 GB  (same) -> trie     ~59 GB + 240 M stubs
   snapshot 101.4 GB  --->  snapshot 101.6 GB  (+0.3) -> snapshot 101.6 GB (same)

   no timestamps            every leaf has its          nodearchive 83 GB raw
                            last-used period            (42 GB compressed)
```

| | Step 1 baseline | Step 2 post-inject | Step 3 post-move-out (floor 2 / cap 3, the knee of the sweep) |
|---|---|---|---|
| trie nodes | 1,895.4 M | 1,895.4 M | **788.0 M** (-58%) |
| snapshot | 101.38 GB | ~101.6 GB (+under 0.5%) | ~101.6 GB |
| external archive | - | - | 82.96 GB raw / **41.58 GB** zstd |
| PebbleDB (physical, compacted) | 251.75 GB | 251.75 GB | **155.89 GB** (-95.9 GB, -38.1%) |
| **net total disk vs baseline** | - | ~+0.3 GB (+0.1%) | **-12.90 GB (-5.1%) raw / -54.28 GB (-21.6%) compressed** |

What we take from this:

- The move-out shrinks the hot database by deleting cold interior nodes and relocating
  cold leaves. With the floor-2 / cap-3 rule PebbleDB drops 95.9 GB and the leaves land in
  a flat file you can park on cheaper storage.
- Total on-disk still shrinks after counting the archive: -12.90 GB raw, and -54.28 GB
  (about 22%) with the chunked compression. The price is recomputing a small subtree on
  the rare read of expired state, at most 73 leaves at this cap.
- How we group the moved leaves matters more than which exact height we pick. Requiring
  each chunk to be exactly N levels throws away cold regions that are not that exact height.
  Taking the largest cold chunk up to a cap instead, with a floor of 2, keeps that coverage
  and consolidates it, which is why cap 3 (-54.28 GB) beats the best exactly-height result
  (-35.54 GB) by 18.7 GB. Going deeper helps with sharply diminishing returns, cap 4 reaches
  -62.58 GB and cap 5 -64.87 GB, while the worst case rebuild grows past a thousand leaves.

## Open Questions

**Performance under real workloads.** Everything above is a static footprint
measurement. What it does not measure is the runtime cost of the design, and that cost
differs sharply between the naive and subtree approaches. The naive approach serves a
moved node with a single read into the side file. The subtree approach pays a rebuild on
every hit: read the leaf records, rebuild the subtree in memory,
and hash-check the result before returning. That is cheap per hit at a shallow cap (at most
73 leaves at floor-2 / cap-3) but it grows with the cap, to about 300 leaves at cap 4 and over
a thousand at cap 5, and it happens every time execution touches an expired subtree. This is
the main reason the deeper caps are not obviously better despite saving more disk.

So the footprint winner is not automatically the workload winner. A design that saves the
most disk can still lose if a common access pattern keeps reaching into cold subtrees and
paying the rebuild. The experiment we have not run yet is to replay real mainnet blocks,
plus some adversarial patterns that deliberately touch expired state, against an expired
datadir and measure: resurrections per block, rebuild latency, the resulting read
amplification, and tail latency on the unlucky reads. Compression adds a second layer
here, since a hit on a compressed chunk also pays a zstd decompress of that chunk. Hence,
the numbers presented in this document should solely serve as references for storage footprint, and not performance.

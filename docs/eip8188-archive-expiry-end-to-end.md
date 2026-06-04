# How much cold state can EIP-8188 actually move out?

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
block's stateRoot.

```
   account trie  (root hash = stateRoot)
            |
         (branch)                 16 slots, one per hex nibble of the key
        /        \
  (extension)   (branch)          extension = shared key-prefix -> one child
      |          /     \
   (branch)   (leaf)  (leaf)      leaf = key tail + value
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

The node as-is. Sizes are logical bytes (sum of record contents) unless noted. The
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
every leaf is inactive. The picture below uses height 3 (leaves three levels under the
root). Height is configured. A deeper subtree holds more leaves (up to 16^(N-1)), so it
bounds how much you rebuild on a read, but a deeper subtree is much less likely to be
*entirely* cold. We swept heights 3, 4 and 5.

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
   read of an expired leaf:
     stub(offset, size) -> read records -> rebuild subtree -> return value
                                           (cheap: at most 256 leaves)
```

**How the cold subtrees are found.** One streaming pass walks the trie and the
period-stamped snapshot side by side. At each candidate node it checks whether all
leaves under it are inactive (`currentPeriod - leafPeriod >= 2`, using the periods from
step 2). If they are, it moves the subtree, and the rebuilt-hash check runs before
anything is deleted.

**Results, sweeping the height.** Going deeper trades coverage for compressibility. A
height-4 account region covers about 4,096 leaves and is basically never 100% cold, so
far fewer subtrees qualify and less moves out, but each archive block is bigger and
compresses better. The snapshot does not change at any height. Net is
`PebbleDB_after + archive - 251.75 GB baseline`. The "zstd" archive column is
chunk-compressed (explained below).

| height | trie nodes after | subtrees moved (stubs) | PebbleDB reduction | archive raw | archive zstd | net raw | **net zstd** |
|---:|---:|---:|---:|---:|---:|---:|---:|
| **3** | 1,234.4 M | **77.0 M** | **-54.5 GB** | 43.09 GB | 22.59 GB | -11.45 | **-31.95** |
| 4 | 1,380.6 M | 17.5 M | -41.6 GB | 30.45 GB | 16.44 GB | -11.15 | -25.16 |
| 5 | 1,389.7 M | 3.44 M | -40.7 GB | 29.30 GB | 16.01 GB | -11.39 | -24.68 |

Height 3 wins, and the gap grows once you compress the archive. Heights 4 and 5 pull
far less out of PebbleDB, only about 17.5 M and 3.4 M subtrees qualify against 77 M at
height 3, and their smaller archives do not make up for it. Heights 4 and 5 basically
land in the same place, hitting the same deep cold regions just packed into fewer,
bigger subtrees. Deeper is worse. If anything the way to push further is shallower, not
deeper, which honestly was not what we expected going in.

Physical footprint at height 3 (compacted pebble SSTs, ancient freezer excluded):

```
                       PebbleDB     + archive    = total       vs baseline
   baseline            251.75 GB     -            251.75 GB     -
   after move-out      197.22 GB     43.09 GB     240.31 GB     -11.45 GB
   archive compressed  197.22 GB     22.59 GB     219.81 GB     -31.95 GB
```

The archive is compressed in roughly 1 MB chunks, one zstd frame per chunk plus a
small offset table, which about halves it while still letting you resurrect a single
subtree by decompressing just its chunk. Compression turned out to be the biggest lever
by far. Two things that did not work: compressing each leaf or each subtree on its own,
because the blocks are too small for zstd to find anything, and a shared dictionary
trained on sample records, which moved the number by a couple of points and sometimes
made it worse. You have to compress many subtrees together.

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
| net (trie delta + flat file) | **+46.93 GB** | **-6.19 GB** raw / **-26.69 GB** zstd |

The flat file is the whole story. The naive approach keeps the full structure, which
costs 162 GB, close to four times the leaves-only archive and over seven times the
compressed one. It moves about 2.5x as many nodes, so its main-database saving is larger,
but the flat file outgrows that saving and total disk goes *up* by about 47 GB. The
subtree approach drops the interior and rebuilds it on read, so its flat file stays small
and total disk comes *down*.

The takeaway that shaped the design is simple. Do not relocate interior nodes. Drop them
and rebuild on read. That one change is what turns a net disk increase into a net
decrease.

---

## End to end

```
   STEP 1 baseline          STEP 2 period inject        STEP 3 move inactive out
   ---------------          --------------------        ------------------------
   trie     148.1 GB  --->  trie     148.1 GB  (same) -> trie     ~99 GB + 77 M stubs
   snapshot 101.4 GB  --->  snapshot 101.6 GB  (+0.3) -> snapshot 101.6 GB (same)

   no timestamps            every leaf has its          nodearchive 43 GB raw
                            last-used period            (22.6 GB compressed)
```

| | Step 1 baseline | Step 2 post-inject | Step 3 post-move-out (height 3, best of the 3 to 5 sweep) |
|---|---|---|---|
| trie nodes | 1,895.4 M | 1,895.4 M | **1,234.4 M** (-35%) |
| snapshot | 101.38 GB | ~101.6 GB (+under 0.5%) | ~101.6 GB |
| external archive | - | - | 43.09 GB raw / **22.59 GB** zstd |
| PebbleDB (physical, compacted) | 251.75 GB | 251.75 GB | **197.22 GB** (-54.5) |
| **net total disk vs baseline** | - | ~+0.3 GB | **-11.45 GB raw / -31.95 GB compressed** |

What we take from this:

- Injection is free and it stays out of the way. It only touches the snapshot
  (about +0.3 GB), never the trie, and it is the thing that lets step 3 tell cold from
  hot.
- The move-out shrinks the hot database by deleting cold interior nodes and relocating
  cold leaves. PebbleDB drops 54.5 GB and the leaves land in a flat file you can park on
  cheaper storage.
- Total on-disk still shrinks after counting the archive: -11.45 GB raw, and -31.95 GB
  (about 13%) with the chunked compression. The price is recomputing a small subtree on
  the rare read of expired state.
- Height 3 came out of a 3-to-5 sweep. Deeper subtrees qualify far less often, so they
  move out less and net worse, about -25 GB at heights 4 and 5 against -32 GB at
  height 3.

## Open Questions

**Read performance under real workloads.** Everything above is a static footprint
measurement. What it does not measure is the runtime cost of the design, and that cost
differs sharply between the naive and subtree approaches. The naive approach serves a
moved node with a single read into the side file. The subtree approach pays a rebuild on
every hit: read the leaf records, re-insert each `(path, value)` into a fresh mini-trie,
and hash-check the result before returning. That is cheap per hit (at most 256 leaves)
but it is not free, and it happens every time execution touches an expired subtree.

So the footprint winner is not automatically the workload winner. A design that saves the
most disk can still lose if a common access pattern keeps reaching into cold subtrees and
paying the rebuild. The experiment we have not run yet is to replay real mainnet blocks,
plus some adversarial patterns that deliberately touch expired state, against an expired
datadir and measure: resurrections per block, rebuild latency, the resulting read
amplification, and tail latency on the unlucky reads. Compression adds a second layer
here, since a hit on a compressed chunk also pays a zstd decompress of that chunk. Until
those numbers exist, the -32 GB is a storage result only, not a performance one.

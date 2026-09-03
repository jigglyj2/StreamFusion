---
title: LIMIT
description: Acceleration coverage and fallback behavior for Flink SQL LIMIT.
sidebar:
  order: 13
---

**Current status:** Accelerated for Flink streaming global constant `LIMIT` and `OFFSET`.

## SQL example

```sql
SELECT *
FROM bid
LIMIT 100 OFFSET 10;
```

## Acceleration and fallback

StreamFusion replaces Flink's `StreamExecLimit`, represented by an unordered, all-in-one
`ROW_NUMBER` range. The range must have a constant upper bound and must not expose a rank column.
Other unordered rank shapes fall back with an EXPLAIN reason. An ordered finite range is handled by
[Top-N](../top-n/); a full `ORDER BY` remains on Flink.

The payload may contain every Flink logical type supported by the Arrow boundary, including nested
arrays, maps, multisets, and rows. Append, update, and retract changelogs use the strategy selected
by Flink. Retract mode retains rows so deleting a visible row can reveal the next row in arrival
order. LIMIT is timer-free. State TTL, when configured by Flink, uses the shared lazy-expiration
contract; the non-expiring append-only fast path is enabled only when that cannot change semantics.

## Implementation

The Java planner keeps Flink's singleton exchange and lowers the limit to the versioned Top-N
protobuf node with no sort keys. Rust computes the singleton state's Flink key group and accesses
the selected memory or direct native RocksDB backend without a state JNI round trip. Append-only
LIMIT stores a versioned count rather than payload rows. Once a non-expiring append-only limit is
full, the operator stops state reads/writes and Arrow C Data calls while continuing to count and
validate logical input rows. Retraction and TTL plans remain on the general state path.

Memory and RocksDB share canonical key-group savepoints. The operator supports aligned and
unaligned checkpoints, cross-backend restore, and incremental RocksDB checkpoints with reusable
SST handles. The empty global key still uses Flink's key-group assignment and canonical state
format, although Flink's required singleton exchange keeps a global LIMIT at parallelism one.
Native state, scratch, RocksDB cache/write buffers, and Arrow transfers are admitted through Flink
managed memory.

Metrics use the same Flink Rank surface as the selected strategy: `topn.invalidTopSize` is present
for every plan; append/update strategies also expose `topn.cache.hitRate` and `topn.cache.size`.
Standard I/O counters count logical rows even after saturation. The `StreamFusion` subgroup reports
logical changelog counts, state batches and groups, comparator calls, invalid retractions,
managed-memory use, and checkpoint/restore activity.

See the [Flink 2.3 LIMIT documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/limit/).

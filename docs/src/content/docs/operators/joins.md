---
title: Joins
description: Acceleration coverage and fallback behavior for Flink SQL Joins.
sidebar:
  order: 9
---

**Current status:** Partially accelerated for synchronous regular streaming equi-joins.

## SQL example

```sql
SELECT b.bidder, b.price, p.name
FROM bid AS b
JOIN person AS p ON b.bidder = p.id;
```

## Acceleration and fallback

StreamFusion currently accelerates Flink's synchronous regular streaming `INNER`, `LEFT`,
`RIGHT`, `FULL`, `SEMI`, and `ANTI` equi-joins when both sides use non-unique multiset state.
Rows and join keys may use any Arrow-representable Flink scalar or nested logical type. The native
operator accepts the complete insert/update-before/update-after/delete changelog, retains
duplicates, applies Flink's per-key null filtering, and reproduces Flink's null-padding and
association-count transitions.

The containing plan falls back to Flink with an EXPLAIN reason when a regular join has a non-equi
condition, state TTL, mini-batch execution, asynchronous state, changelog-state wrapping, or a
planner-provided unique/upsert key. Interval, temporal, lookup, and bounded batch joins are also
still Flink-owned. These are explicit unimplemented shapes, not approximations of their semantics.

## Implementation

The planner replaces an eligible `StreamExecJoin` with a distinct StreamFusion exec node and sends
a versioned protobuf join contract to Rust. Each input crosses a native Arrow exchange edge; the
join itself receives Arrow batches and returns Arrow batches without a RowData loop or per-record
JNI call.

Rust stores an ordered multiset for both input sides under a Flink-compatible key group. One input
batch performs one distinct batched state read and one atomic batched write. The same opaque state
contract runs on managed native memory or direct native RocksDB. Canonical key-group snapshots move
between those backends and across parallelism, while ordinary RocksDB checkpoints use the shared
incremental-SST lifecycle. Aligned and unaligned checkpoints preserve both join state and the two
input watermark frontiers. Regular joins do not register timers; watermarks are coalesced and
forwarded using the two-input Flink rule.

Deterministic native transition tests cover every join type, duplicates, retractions, null keys,
rescaling, and memory-to-RocksDB restoration. The generated SQL parity suite currently uses INNER
and SEMI joins because their result changelog is invariant to the test harness's independent
two-input source scheduling; outer and anti transition ordering is checked with controlled native
input order.

See the [Flink 2.3 Joins documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/joins/).

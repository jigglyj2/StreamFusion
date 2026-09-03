---
title: Joins
description: Acceleration coverage and fallback behavior for Flink SQL Joins.
sidebar:
  order: 9
---

**Current status:** Partially accelerated for synchronous regular and time-bounded streaming
equi-joins.

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

Flink `StreamExecIntervalJoin` plans are accelerated for constant row-time or processing-time
bounds and `INNER`, `LEFT`, `RIGHT`, or `FULL` joins. Both sides retain timestamp-ordered
multisets in native keyed state. Native event-time or processing-time cleanup timers delay outer
null rows until no future match can arrive, and retractions reverse both joined and previously
emitted outer rows using Flink-compatible association counts. Keys and stored rows have the same
complete Arrow-representable scalar and nested type coverage as regular joins.

The containing plan falls back to Flink with an EXPLAIN reason when a regular join has a non-equi
condition, state TTL, mini-batch execution, asynchronous state, changelog-state wrapping, or a
planner-provided unique/upsert key. Interval joins additionally fall back for a residual non-equi
condition, non-constant bounds, semi/anti join modes, mini-batching, asynchronous state, or
changelog-state wrapping. Temporal, lookup, and bounded batch joins remain Flink-owned. These are
explicit unimplemented shapes, not approximations of their semantics. In particular, the current
Flink plans for official Nexmark q4 and q9 use an expiry-column residual condition on a regular
join; they do not satisfy the constant-bound interval-join contract.

## Implementation

The planner replaces an eligible `StreamExecJoin` with a distinct StreamFusion exec node and sends
a versioned protobuf join contract to Rust. Each input crosses a native Arrow exchange edge; the
join itself receives Arrow batches and returns Arrow batches without a RowData loop or per-record
JNI call.

Rust stores an ordered multiset for both input sides under a Flink-compatible key group. One input
batch performs one distinct batched state read and one atomic batched write. The same opaque state
contract runs on managed native memory or direct native RocksDB. Canonical key-group snapshots move
between those backends and across parallelism, while ordinary RocksDB checkpoints use the shared
incremental-SST lifecycle. Aligned and unaligned checkpoints preserve join state and the two input
watermark frontiers. Regular joins do not register timers. Interval joins use the shared native
timer service and materialize dirty timer groups into canonical keyed state at snapshot boundaries,
avoiding repeated whole-group timer serialization during normal batch processing. Both variants
coalesce and forward watermarks using Flink's two-input rule.

Deterministic native transition tests cover every join type, duplicates, retractions, null keys,
rescaling, and memory-to-RocksDB restoration. Interval coverage additionally exercises every
supported logical type as both a key and stored value, pending event-time and processing-time
timers, aligned and unaligned checkpoints, canonical cross-backend savepoints, and incremental
RocksDB restore. The generated SQL parity suite uses INNER and SEMI regular joins because their
result changelog is invariant to the test harness's independent two-input source scheduling;
outer and anti transition ordering is checked with controlled native input order. A deterministic
constant-bound interval-join workload compares the complete Flink and StreamFusion changelogs on
both state backends.

See the [Flink 2.3 Joins documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/joins/).

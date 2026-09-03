---
title: OVER aggregation
description: Acceleration coverage and fallback behavior for Flink SQL OVER aggregation.
sidebar:
  order: 8
---

**Current status:** Partially accelerated. Streaming non-time `ROWS` and `RANGE` frames from
`UNBOUNDED PRECEDING` through `CURRENT ROW` run natively when the query uses one ascending order
key and supported aggregates.

**Future acceleration target:** Yes.

## SQL example

```sql
SELECT bidder, price,
  SUM(price) OVER (
    PARTITION BY bidder ORDER BY auction
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM bid;
```

## Acceleration and fallback

The current native path supports `COUNT`, `SUM`, `MIN`, and `MAX`, including Flink's internal
`$SUM0` rewrite. It accepts the complete insert/update/delete changelog. `RANGE` peers receive the
same aggregate value, while `ROWS` peers retain deterministic input order. All partition-key and
row payload types representable by the Arrow row codec are retained as opaque Arrow rows; complex
partition keys are pre-encoded with Flink's binary-row hash contract before the native boundary.

State is available through both StreamFusion's managed in-memory backend and direct native
RocksDB backend. Both use Flink key groups, batched multi-get/write calls, canonical cross-backend
key-group snapshots, rescaling, aligned and unaligned recovery, and incremental RocksDB native
checkpoints. Native allocations are charged to the operator's Flink managed-memory reservation.

The operator publishes Flink's `numOfIdsNotFound` and `numOfSortKeysNotFound` counters plus the
standard logical-record I/O metrics. StreamFusion state, checkpoint, recovery, allocation, and
native batch diagnostics remain in the explicitly named StreamFusion metric subgroup.

Time-attribute frames, bounded preceding frames, descending or multiple order keys, aggregate
functions beyond the set above, mini-batch mode, async state, changelog-state wrapping, and state
TTL still cause an explicit whole-plan fallback. These are not documented as accelerated until
their parity and recovery suites pass.

## Implementation

The Java planner keeps the Flink node available for fallback and sends a versioned protobuf plan
to a dedicated Rust physical operator. Arrow batches cross the JVM boundary only at the native
plan edge. The operator uses ordered native keyed state and Arrow aggregate kernels because
DataFusion alone does not provide Flink's incremental changelog and peer behavior.

## Local performance evidence

On the September 3, 2026 local release/native-CPU `over-aggregate` RowData Nexmark workload,
three alternating fresh-JVM forks processed 100,000 deterministic events at parallelism one.
In-memory Flink and StreamFusion medians were 20,908 and 20,759 events/s respectively, or 99.3%
throughput parity; elapsed-time ranges were 4.677–4.930s and 4.791–4.875s. RocksDB medians were
16,887 and 19,843 events/s, a 17.5% StreamFusion gain, with elapsed ranges of 5.879–6.037s and
5.017–5.175s. Every run emitted the same 92,000-row changelog with SHA-256
`9cdcca648552898214b792466466ca07ccf3af86de0663a881d9930e38036144`, and every StreamFusion
fork reported seven native OVER batches. Profiler-instrumented timings were excluded.

Separate 600,000-event mixed JVM/native CPU profiles used non-safepoint sampling, DWARF/frame-
pointer unwinding, JFR, collapsed stacks, and differential flame graphs on both backends. After
resuming append-heavy updates from the prior emitted prefix, full-partition recomputation was
0.11% of samples, ordered-suffix recomputation was 0.42–0.74%, and the complete native OVER
processor was 2.1–2.4%. The required source RowData-to-Arrow boundary remained larger at
4.5–5.5%. Separate Java and native allocation profiles found no dominant OVER state-loop
allocation; all retained state and scratch buffers are also covered by the managed-memory denial
and release tests. These are local diagnostic results, not portable performance claims.

See the [Flink 2.3 OVER aggregation documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/over-agg/).

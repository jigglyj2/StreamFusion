---
title: OVER aggregation
description: Acceleration coverage and fallback behavior for Flink SQL OVER aggregation.
sidebar:
  order: 8
---

**Current status:** Partially accelerated. Streaming non-time `ROWS` and `RANGE` frames from
`UNBOUNDED PRECEDING` through `CURRENT ROW` run natively. Processing-time and event-time frames
also accept a constant bounded `PRECEDING` boundary. All forms require one ascending order key and
supported aggregates.

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

The current native path supports `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`, including Flink's internal
`$SUM0` rewrite. Flink lowers SQL `AVG` to its typed sum/count accumulator plus an output projection;
both stages remain inside the accelerated Arrow plan for every numeric input family. It accepts the
complete insert/update/delete changelog. `RANGE` peers receive the
same aggregate value, while `ROWS` peers retain deterministic input order. All partition-key and
row payload types representable by the Arrow row codec are retained as opaque Arrow rows; complex
partition keys are pre-encoded with Flink's binary-row hash contract before the native boundary.
For an event-time order key, rows are buffered in timestamp and input order until the watermark
fires the corresponding native timer. `ROWS` advances one row at a time and `RANGE` emits all peers
with the same aggregate value. Rows arriving at or behind the current watermark are dropped with
Flink-compatible late-record accounting. A bounded `ROWS` frame retains only its required row
suffix; bounded `RANGE` uses the inclusive millisecond distance represented by Flink's window
boundary. Large watermarks drain due timers into managed 8,192-row Arrow batches so a terminal
watermark cannot require one unbounded output allocation.

For processing time, the planner recognizes Flink's `Calc -> Exchange -> OVER -> Calc` shape,
removes the unobservable synthetic `PROCTIME()` field, and keeps supported lower filters and
projections as native Calcs. The native kernel uses arrival order, matching Flink's processing-time
function for both `ROWS` and `RANGE`. Flink SQL only plans this operator for append-only input. An
unbounded frame therefore uses one compact accumulator per partition, while a bounded `ROWS` frame
keeps only the aggregate contributions in its rolling suffix. Bounded processing-time `RANGE`
groups rows that observe the same millisecond and emits those peers from a native processing-time
timer. The native changelog representation remains available for directly tested retractions and
older row-history savepoints are upgraded by replaying their contributions once during the first
post-restore batch.

State is available through both StreamFusion's managed in-memory backend and direct native
RocksDB backend. Both use Flink key groups, batched multi-get/write calls, canonical cross-backend
key-group snapshots, rescaling, aligned and unaligned recovery, and incremental RocksDB native
checkpoints. Native allocations are charged to the operator's Flink managed-memory reservation.
Event and processing-time timers plus the current watermark are included in canonical snapshots,
including pending-timer redistribution during scale-out and scale-in recovery. Dirty timer groups
are materialized into keyed state only at canonical or native RocksDB checkpoint boundaries; the
runtime does not rewrite an ever-growing timer snapshot on every input batch. Event-time OVER
keeps one active earliest-pending timer per partition and advances that partition through the
current watermark when it fires. The third canonical state-codec version uses variable-width
sequence and aggregate encodings; restore coverage retains compatibility with the first two
fixed-width versions.

The non-time operator publishes Flink's `numOfIdsNotFound` and `numOfSortKeysNotFound` counters;
the event-time operator publishes `numLateRecordsDropped`. Both publish the standard logical-record
I/O metrics. StreamFusion state, checkpoint, recovery, allocation, pending event-time and
processing-time timer, and native batch diagnostics remain in the explicitly named StreamFusion
metric subgroup.

If a query selects, filters on, or otherwise observes the synthetic processing-time field, the plan
still falls back explicitly because removing that value would change semantics. Following frames,
non-constant frame boundaries, descending or multiple order keys, aggregate functions beyond the set
above, mini-batch mode, async state, changelog-state wrapping, and state TTL also fall back.
These are not documented as accelerated until their end-to-end parity and recovery suites pass.

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

The true event-time `over-aggregate-event-time` workload uses the bid rowtime attribute rather than
casting it to a regular timestamp. On the same release/native-CPU machine, three alternating fresh
JVM forks over 100,000 events produced in-memory medians of 20,934 events/s for Flink and 20,642
events/s for StreamFusion (98.6% parity). RocksDB medians were 16,778 and 19,813 events/s, an 18.1%
StreamFusion gain. Every run emitted the same 92,000-row changelog with the SHA-256 above, and each
StreamFusion run reported seven native OVER batches.

Mixed JVM/native CPU profiles covered longer event-time runs on both backends. The complete native
OVER input and watermark paths accounted for 3.5% or less of samples; timer registration was about
1%, and state encoding, decoding, key encoding, timer snapshots, and individual RocksDB operations
were each below 0.4%. Java allocation profiles were led by result RowData materialization and the
benchmark sink. Native samples showed expected row retention and timer registration, but no
corresponding CPU hot path. The implementation therefore retains the shared timer service and
canonical state format instead of introducing an OVER-specific benchmark shortcut. Profiler-
instrumented timings were excluded from the throughput results.

The `over-aggregate-processing-time` workload exercises the real Nexmark bid filter and nested-row
projection below `PROCTIME()`. Three alternating separate-JVM release/native-CPU forks over two
million events at parallelism one produced in-memory medians of 175,965 events/s for Flink and
185,511 events/s for StreamFusion, a 5.4% gain; elapsed ranges were 10.677–11.500s and
10.398–10.833s. RocksDB medians were 151,893 and 152,452 events/s respectively (0.4% gain), with
wider elapsed ranges of 12.813–14.811s and 10.767–15.856s. Every fork emitted 1,840,000 rows with
SHA-256 `667f3fe5bdcc5417f2aed194286f9ecfd983a245ce08a2e316b3f579705b2e55`; StreamFusion
reported 124–125 native OVER batches and 373–377 native Calc batches.

Separate three-million-event mixed JVM/native profiles retained JFR, collapsed stacks, flame
graphs, and differential flame graphs for both engines and backends. The complete native OVER call
tree accounted for 15.8% of memory and 19.1% of RocksDB CPU samples, with no dominant leaf; direct
native RocksDB work was 4.2%, compared with 14.5% in the Flink RocksDB profile. The source
RowData-to-Arrow boundary was about 7%. Java and native allocation profiles show the expected
output-row/Arrow materialization rather than retained historical rows; all state, scratch, and
RocksDB allocations remain governed by Flink managed memory. Profiler-instrumented timings were
excluded from the benchmark results.

The bounded-frame workloads were measured over one million deterministic events in three
alternating fresh-JVM forks. For `ROWS BETWEEN 100 PRECEDING AND CURRENT ROW`, the in-memory
medians were 125,804 events/s for Flink and 125,718 events/s for StreamFusion (99.9% parity), with
elapsed ranges of 7.870–7.992s and 7.867–8.053s. RocksDB medians were 55,406 and 120,289 events/s,
a 2.17x StreamFusion gain, with elapsed ranges of 17.709–18.231s and 8.193–8.337s. Every run
emitted the same 920,000 rows with SHA-256
`66d6a3d626e845ebd5f0e05af7a4c8b44e099284000de995b309699f011fd037`.

For the inclusive ten-second event-time `RANGE`, in-memory medians were 116,211 events/s for Flink
and 111,770 events/s for StreamFusion (96.2% parity), with elapsed ranges of 8.122–8.760s and
8.780–9.052s. RocksDB medians were 35,446 and 108,597 events/s, a 3.06x StreamFusion gain. The
RocksDB elapsed ranges were 20.363–30.669s and 8.879–19.310s; the unusually wide ranges are
retained because the local storage showed substantial run-to-run variance. Every run emitted the
same 920,000 rows with SHA-256
`6efa4c099108291ada48a6bde539fffd227870454a8702cf4e19de8e5ea12108`, and all StreamFusion forks
reported native OVER and Calc batches.

Separate 500,000-event diagnostic runs captured mixed JVM/native CPU, Java allocation, and native
allocation JFRs plus collapsed stacks and differential flame graphs. The native OVER call tree was
4.9% of in-memory and 5.6% of RocksDB CPU samples; timer work was 1.9–2.0%, and direct native
RocksDB work was 1.5%. Required RowData/Arrow edges plus JNI were approximately 8–9% in this
operator-isolation topology. Java allocation volume fell from 4.55 GB to 2.70 GB in memory and from
30.06 GB to 2.71 GB on RocksDB. Native allocation samples show retained event rows and timers, all
of which remain governed by Flink managed-memory admission and release. Profiler-instrumented
timings were excluded from throughput results.

See the [Flink 2.3 OVER aggregation documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/over-agg/).

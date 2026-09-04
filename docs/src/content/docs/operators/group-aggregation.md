---
title: Group aggregation
description: Acceleration coverage and fallback behavior for Flink SQL Group aggregation.
sidebar:
  order: 6
---

**Current status:** Partially accelerated for timer-free keyed and global streaming aggregates,
including grouping sets, `ROLLUP`, and `CUBE`.

## SQL example

```sql
SELECT bidder, COUNT(*) AS bids, SUM(price) AS spend,
       MIN(price) AS minimum_price, MAX(price) AS maximum_price
FROM bid
GROUP BY bidder;
```

## Acceleration and fallback

StreamFusion accelerates keyed `StreamExecGroupAggregate` plans containing `COUNT(*)`,
single-input `COUNT`, and the following aggregate/type combinations. Every listed call also
supports SQL `FILTER (WHERE ...)`; null filter predicates are false.

| Aggregate | Supported Flink SQL types |
| --- | --- |
| `SUM` | `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`, `DECIMAL` |
| `AVG` | `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`, `DECIMAL` |
| `MIN`, `MAX` | All `SUM` types plus `BOOLEAN`, `CHAR`, `VARCHAR`, `DATE`, `TIME`, `TIMESTAMP`, and `TIMESTAMP_LTZ` |

`COUNT(DISTINCT value)` supports the same scalar types as `MIN`/`MAX`, and
`SUM(DISTINCT value)` and `AVG(DISTINCT value)` support the numeric types above. Distinct values
are counted in native state, so duplicate
inserts and retractions change the result only at first/last membership boundaries. `DISTINCT`
and `FILTER` may be combined.

Input may be insert-only or a Flink changelog. Changelog output preserves Flink's per-record
`INSERT`, `UPDATE_BEFORE`, `UPDATE_AFTER`, and `DELETE` behavior byte-for-byte; unchanged aggregate
values do not produce a spurious update. Retraction parity covers null values, duplicate extrema,
removing the current extremum, deletes against absent state, IEEE-754 NaN and signed zero, and
deleting the final row of a group. Integer and decimal arithmetic uses the planned Flink result
type, including its overflow behavior.

Global aggregates use Flink's required singleton exchange and the canonical zero-field
`BinaryRowData` state key. The native batch path materializes that key once rather than hashing one
identical allocation per input row. Flink's `StreamExecExpand` is accelerated for grouping sets,
`ROLLUP`, and `CUBE`; the expanded grouping fields may widen from non-null to nullable without
changing their logical type. Generated parity cases cover scalar binary, decimal, date, and
timestamp keys as well as opaque array and row keys through that nullable boundary.

One- and two-phase count-triggered mini-batch aggregation are accelerated. Flink's split-DISTINCT
three-stage shape is also accelerated for the aggregate/type combinations above. It remains three
observable physical stages—native local aggregate, native stateful incremental aggregate, and
native global aggregate—with the two planned exchanges retained between them. The incremental
stage keeps counted DISTINCT membership and retractable extrema by partial key, merges local opaque
deltas, and emits one opaque net accumulator delta per final key. Ordinary COUNT/SUM/AVG and
append-only extrema remain bundle-local, so they do not create keyed-state reads or tombstones.
Duplicate values and extrema replacements therefore remain correct across bundle boundaries,
including after retractions and recovery.

The two-phase shape is
lowered as distinct native local aggregate, native exchange, and native global aggregate plan
nodes. The local stage is state-free and emits grouping columns plus one opaque, versioned native
accumulator; neither the exchange nor Java interprets that accumulator. The global stage merges
the partial deltas into the same canonical keyed state used by one-phase aggregation. StreamFusion
preserves Flink's exact bundle boundaries even when one Arrow input batch crosses several
boundaries, buffers opaque group keys and accumulator deltas in managed native memory, and emits at
most one aggregate-level change per key when a bundle is finished. Bundles finish at their
configured count, before a watermark or checkpoint, and at bounded input completion.
Processing-time and row-time mini-batch assigners remain Arrow control operators and never
transpose the payload back to rows. Local bundle output follows Flink's Java `HashMap` bucket
iteration order because that order can affect the receiving global bundle boundary and therefore
the observable changelog. Native `Expand` emits projection results in Flink's input-row-major
order; projection-major union batches are not used because they would alter local and incremental
bundle boundaries.

State TTL, async state, Flink's changelog-state wrapper, multi-column `DISTINCT`,
ordered or approximate aggregates, `IGNORE NULLS`, unsupported aggregate functions, and
unsupported aggregate value types fall back with a specific EXPLAIN reason. Flink's internal
`SUM0` call uses the same accumulator as `SUM` here: a group has no output after its last row is
retracted, so an empty accumulator value is not observable in this physical operator.
Flink may instead retain `AVG` as a physical aggregate call. That path uses Flink's two-buffer
contract: a wrapping `BIGINT`, `DOUBLE`, or `DECIMAL(38, input_scale)` sum and a `BIGINT` non-null
count. Decimal results apply Flink's precision-38 `HALF_UP` division and final-scale rounding.
Both ordinary and counted-distinct AVG state support filters and retractions.

Grouping keys use the same Flink `BinaryRowData` bytes and key-group assignment as native
deduplication. Scalar keys are encoded directly in Rust; keys that need Flink's complex internal
encoding are supplied as opaque bytes. Java never interprets native state keys or accumulator
values.

## State and recovery

The aggregate uses the shared backend-neutral native keyed-state interface:

- `HashMapStateBackend` stores opaque values in `ahash`/`hashbrown` maps split by key group.
- `EmbeddedRocksDBStateBackend` talks to the separately packaged RocksDB component through its
  versioned native ABI. The immediate path performs one distinct-key multi-get and one `WriteBatch`
  per Arrow input batch, including deletes. Mini-batch stages avoid empty backend calls and batch
  missing-key reads and mutations at exact Flink bundle boundaries.

Both backends use the same versioned canonical key-group snapshot format. Accumulator payload
version 6 adds sparse neutral-accumulator tags while continuing to read versions 1–5; version 5
added ordinary and counted-distinct AVG sum/count state, version 4 added counted `DISTINCT` sets,
and version 3 introduced typed boolean,
floating-point, string, temporal, and nullable decimal-overflow state.
Canonical savepoints are tested across all four source/target
backend pairs and redistribute key groups during both 1-to-N and N-to-1 rescaling. Regular RocksDB
checkpoints use incremental Flink keyed-state handles, reuse completed immutable SST files, survive
Flink checkpoint-metadata serialization, and restore into native RocksDB.

Global aggregate recovery is independently tested for all four memory/RocksDB source-to-target
backend pairs with canonical savepoints and with both aligned and unaligned checkpoints. Global
state remains singleton state; keyed and grouping-set aggregates retain the normal Flink key-group
rescaling contract. The two-phase global operator uses that identical snapshot/checkpoint path;
canonical native global state is also round-tripped between the memory and RocksDB processors. The
local stage flushes before the checkpoint pre-barrier and has no persistent state of its own. The
stateful incremental stage uses the same canonical raw-keyed snapshot and direct RocksDB ABI as
the global stage. Native tests cover duplicate/retraction restoration after key-group rescaling,
memory-to-RocksDB restoration, batched state reads and writes, state-free ordinary split branches,
and managed memory admission. Its pending bundle is flushed before checkpoint snapshotting by the
shared group-aggregate operator lifecycle.

The aggregate operator has no timers in the immediate, two-phase, or split-DISTINCT
count-triggered mini-batch shapes.
A pending mini-batch is finished before the checkpoint pre-barrier hook, so aligned and unaligned
checkpoints snapshot the same canonical aggregate state; Flink's channel-state machinery owns
messages still in flight and the native state does not need a second message sequence log. The
processing-time mini-batch assigner uses Flink processing timers only to advance its batch
watermark. With aligned checkpoints, exchange rows are gathered into one Arrow frame per non-empty
destination. With unaligned checkpoints, exchange frames retain a single key group so Flink can
redistribute captured channel state during rescaling without splitting a frame.

Arrow buffers, in-memory state, aggregate scratch/output storage, RocksDB cache and write buffers,
and restore readers are admitted through the operator's existing Flink managed-memory allowance.
Used, peak, and limit gauges are exposed under the operator's StreamFusion metric group. The same
group reports logical processing/changelog counts and the native processor's actual batched state
reads and writes,
checkpoint kind/bytes/duration/failures, incremental upload and SST-reuse bytes, and restore
bytes/duration/failures.

Retractable `MIN` and `MAX` keep counted ordered values so deleting the current extremum reveals the
next one. Insert-only extrema use a single scalar instead. The immediate batch path deduplicates
keys with `ahash`, decodes each touched accumulator once, and applies every row in input order. The
mini-batch path performs backend multi-get calls only for missing keys and one mutation batch per
completed bundle, while preserving Flink `HashMap` iteration order inside the bundle. This follows the keyed
aggregate-group/cache shape used by RisingWave and Arroyo's Arrow incremental aggregates while
retaining Flink's immediate or mini-batch changelog contract as planned.

The RowData Nexmark `group-aggregate`, `global-aggregate`, `grouping-sets`, and
`incremental-group-aggregate` harnesses compare Flink and StreamFusion in separate JVMs for both
state backends. Benchmark builds use the Rust release profile and the build machine's native CPU
feature set. The harness records elapsed time, input throughput, native calculation batches, and
native aggregate batches so exchange fragmentation and JNI call amplification remain visible.

The published measurements below predate the split-DISTINCT incremental executor and therefore do
not claim performance results for that three-stage shape. Its dedicated alternating-fork memory
and RocksDB measurement and mixed-stack profiles have not yet been published.

On the September 4, 2026 local one-million-event two-phase run at parallelism four and bundle size
5,000, three alternating fresh-JVM forks produced in-memory medians of 184,336 events/s for Flink
and 182,799 events/s for StreamFusion (99.2% parity). RocksDB medians were 170,047 and 171,154
events/s respectively, a 0.7% StreamFusion gain. Elapsed ranges were 5.394–5.501s and
5.389–6.416s in memory, and 5.844–8.641s and 5.825–5.874s on RocksDB; the isolated slow fork on
each side is retained as observed variance. Every StreamFusion fork reported acceleration and
nonzero native Calc and aggregate batches. Exact changelog parity is established by deterministic
SQL and recovery tests because the one-second benchmark checkpoint can finish a bundle at a
different input position in separate wall-clock runs, changing valid intermediate updates without
changing final keyed results.

Mixed JVM/native CPU profiles on two-million-event forks retained JFR, collapsed stacks, per-engine
flame graphs, and differential flame graphs. The StreamFusion memory profile attributed 3.2% of
samples to the local aggregate, 3.1% to the global aggregate, 1.1% to Arrow C import/export, 7.4%
to the required source RowData-to-Arrow boundary, and 4.1% to the keyed exchange. Direct native
RocksDB occupied 0.6% of StreamFusion samples versus 2.0% for Flink's RocksDB path, so additional
bundle-boundary state-call coalescing was not justified by this profile. Sampled Java allocation
volume was 5.23/5.26 GB for StreamFusion memory/RocksDB versus 14.48/14.61 GB for Flink. Separate
native-allocation profiles attributed 63 MB in memory and 77 MB on RocksDB to the two native
aggregate stages over two million events; JVM compiler arenas, Arrow boundary buffers, and exchange
buffers were larger. Profiler timings were excluded from throughput results.

On the September 3, 2026 local one-million-event run, three alternating fresh-JVM forks gave global
aggregation in-memory medians of 107,827 events/s for Flink and 103,550 events/s for StreamFusion
(96.0% parity). RocksDB medians were 99,061 and 106,810 events/s respectively, a 7.8% StreamFusion
gain. Elapsed ranges were 9.177–9.689s and 9.458–9.854s in memory, and 9.844–10.893s and
9.324–10.166s on RocksDB. Every fork emitted 1,839,999 changelog rows with SHA-256
`def58ec236efbd1b8d4230f25681e86ef79a487155cd47791631558c0d9d299a`.

Grouping sets on the same event count produced in-memory medians of 75,963 events/s for Flink and
78,222 events/s for StreamFusion, a 3.0% gain. RocksDB medians were 57,959 and 71,869 events/s, a
24.0% gain; the StreamFusion RocksDB elapsed range of 13.473–17.165s is retained because storage
variance was visible. All forks emitted 3,650,083 rows with SHA-256
`25375393fce85edf36dd09d91a045ff75af3a0470896ff1d242ec447058a86ea`.

Mixed JVM/native CPU profiles used non-safepoint Java sampling, DWARF/frame-pointer unwinding, JFR,
collapsed stacks, flame graphs, and differential flame graphs. The complete native global
aggregate path was 1.2–1.3% of CPU samples after removing per-row empty-key hashing; grouping sets
placed 2.2–2.3% in aggregation, about 6% in its native Expand stage, and 7–8% in Arrow/RowData
boundaries. Direct RocksDB was 2.7% of the grouping-sets profile. At 500,000 events, sampled Java
allocation volume was 3.02/2.97 GB for StreamFusion global aggregation versus 4.82/4.98 GB for Flink
on memory/RocksDB, and 4.04/4.17 GB versus 6.73/7.24 GB for grouping sets. Native allocations were
led by required Arrow output and transport buffers; direct RocksDB allocations were not dominant.
Profiler timings were excluded from throughput results.

See the [Flink 2.3 Group aggregation documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/group-agg/).

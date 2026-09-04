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
| `MIN`, `MAX` | All `SUM` types plus `BOOLEAN`, `CHAR`, `VARCHAR`, `DATE`, `TIME`, `TIMESTAMP`, and `TIMESTAMP_LTZ` |

`COUNT(DISTINCT value)` supports the same scalar types as `MIN`/`MAX`, and `SUM(DISTINCT value)`
supports the numeric `SUM` types. Distinct values are counted in native state, so duplicate
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

State TTL, mini-batching, async state, Flink's changelog-state wrapper, multi-column `DISTINCT`,
ordered or approximate aggregates, `IGNORE NULLS`, unsupported aggregate functions, and
unsupported aggregate value types fall back with a specific EXPLAIN reason. Flink's internal
`SUM0` call uses the same accumulator as `SUM` here: a group has no output after its last row is
retracted, so an empty accumulator value is not observable in this physical operator.

Grouping keys use the same Flink `BinaryRowData` bytes and key-group assignment as native
deduplication. Scalar keys are encoded directly in Rust; keys that need Flink's complex internal
encoding are supplied as opaque bytes. Java never interprets native state keys or accumulator
values.

## State and recovery

The aggregate uses the shared backend-neutral native keyed-state interface:

- `HashMapStateBackend` stores opaque values in `ahash`/`hashbrown` maps split by key group.
- `EmbeddedRocksDBStateBackend` talks to the separately packaged RocksDB component through its
  versioned native ABI. Each Arrow input batch performs one distinct-key multi-get and one
  `WriteBatch`, including deletes.

Both backends use the same versioned canonical key-group snapshot format. Accumulator payload
version 4 adds counted `DISTINCT` sets while continuing to read versions 1–3; version 3 introduced
typed boolean, floating-point, string, temporal, and nullable decimal-overflow state.
Canonical savepoints are tested across all four source/target
backend pairs and redistribute key groups during both 1-to-N and N-to-1 rescaling. Regular RocksDB
checkpoints use incremental Flink keyed-state handles, reuse completed immutable SST files, survive
Flink checkpoint-metadata serialization, and restore into native RocksDB.

Global aggregate recovery is independently tested for all four memory/RocksDB source-to-target
backend pairs with canonical savepoints and with both aligned and unaligned checkpoints. Global
state remains singleton state; keyed and grouping-set aggregates retain the normal Flink key-group
rescaling contract.

The operator has no timers in this supported shape. A batch is applied synchronously before the
mailbox processes a checkpoint barrier. Aligned and unaligned checkpoints therefore snapshot the
same native operator state, while Flink's channel-state machinery owns messages still in flight;
the native state does not need a second message sequence log. With aligned checkpoints, exchange
rows are gathered into one Arrow frame per non-empty destination. With unaligned checkpoints,
exchange frames retain a single key group so Flink can redistribute captured channel state during
rescaling without splitting a frame.

Arrow buffers, in-memory state, aggregate scratch/output storage, RocksDB cache and write buffers,
and restore readers are admitted through the operator's existing Flink managed-memory allowance.
Used, peak, and limit gauges are exposed under the operator's StreamFusion metric group. The same
group reports logical processing/changelog counts, one batched state read and write per input batch,
checkpoint kind/bytes/duration/failures, incremental upload and SST-reuse bytes, and restore
bytes/duration/failures.

Retractable `MIN` and `MAX` keep counted ordered values so deleting the current extremum reveals the
next one. Insert-only extrema use a single scalar instead. The batch path deduplicates keys with
`ahash`, decodes each touched accumulator once, and applies every row in input order. This is close
to RisingWave's keyed aggregate-group/cache design and Arroyo's Arrow incremental aggregate, while
emitting immediately to preserve Flink's non-mini-batch changelog contract.

The RowData Nexmark `group-aggregate`, `global-aggregate`, and `grouping-sets` harnesses compare
Flink and StreamFusion in separate JVMs for both state backends. Benchmark builds use the Rust
release profile and the build machine's native CPU feature set. The harness records elapsed time,
input throughput, native calculation batches, and native aggregate batches so exchange
fragmentation and JNI call amplification remain visible.

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

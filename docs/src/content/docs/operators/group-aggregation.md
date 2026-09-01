---
title: Group aggregation
description: Acceleration coverage and fallback behavior for Flink SQL Group aggregation.
sidebar:
  order: 6
---

**Current status:** Partially accelerated for timer-free keyed streaming aggregates.

## SQL example

```sql
SELECT bidder, COUNT(*) AS bids, SUM(price) AS spend,
       MIN(price) AS minimum_price, MAX(price) AS maximum_price
FROM bid
GROUP BY bidder;
```

## Acceleration and fallback

StreamFusion accelerates keyed `StreamExecGroupAggregate` plans containing `COUNT(*)`,
single-input `COUNT`, and integer or decimal `SUM`, `MIN`, and `MAX`. Input may be insert-only or a
Flink changelog. Changelog output preserves Flink's per-record `INSERT`, `UPDATE_BEFORE`,
`UPDATE_AFTER`, and `DELETE` behavior byte-for-byte; unchanged aggregate values do not produce a
spurious update.

The initial implementation requires at least one grouping field. Global aggregation, state TTL,
mini-batching, async state, Flink's changelog-state wrapper, `DISTINCT`, `FILTER`, ordered or
approximate aggregates, `IGNORE NULLS`, unsupported aggregate functions and value types, and
grouping sets/`ROLLUP`/`CUBE` fall back with a specific EXPLAIN reason. `SUM0` also falls back rather
than being approximated as SQL `SUM`.

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

Both backends use the same versioned canonical key-group snapshot format. Canonical savepoints can
restore across memory and RocksDB and redistribute key groups during 1-to-N or N-to-1 rescaling.
Regular RocksDB checkpoints use incremental Flink keyed-state handles and reuse completed immutable
SST files.

The operator has no timers in this supported shape. A batch is applied synchronously before the
mailbox processes a checkpoint barrier. Aligned and unaligned checkpoints therefore snapshot the
same native operator state, while Flink's channel-state machinery owns messages still in flight;
the native state does not need a second message sequence log. With aligned checkpoints, exchange
rows are gathered into one Arrow frame per non-empty destination. With unaligned checkpoints,
exchange frames retain a single key group so Flink can redistribute captured channel state during
rescaling without splitting a frame.

Arrow buffers, in-memory state, aggregate scratch/output storage, RocksDB cache and write buffers,
and restore readers are admitted through the operator's existing Flink managed-memory allowance.
Used, peak, and limit gauges are exposed under the operator's StreamFusion metric group.

Retractable `MIN` and `MAX` keep counted ordered values so deleting the current extremum reveals the
next one. Insert-only extrema use a single scalar instead. The batch path deduplicates keys with
`ahash`, decodes each touched accumulator once, and applies every row in input order. This is close
to RisingWave's keyed aggregate-group/cache design and Arroyo's Arrow incremental aggregate, while
emitting immediately to preserve Flink's non-mini-batch changelog contract.

The RowData Nexmark `group-aggregate` harness compares Flink and StreamFusion in separate JVMs for
both state backends. Benchmark builds use the Rust release profile and the build machine's native
CPU feature set. The harness records elapsed time, input throughput, native calculation batches,
and native aggregate batches so exchange fragmentation and JNI call amplification remain visible.

See the [Flink 2.3 Group aggregation documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/group-agg/).

---
title: Changelog conversion
description: Acceleration coverage and fallback behavior for Flink SQL Changelog conversion.
sidebar:
  order: 19
---

**Current status:** Partially accelerated. Planner-inserted `ChangelogNormalize` and
`DropUpdateBefore` have distinct StreamFusion implementations; explicit changelog conversion
functions remain on Flink.

**Future acceleration target:** Not currently a compute target.

## SQL example

```sql
SELECT *
FROM TABLE(FROM_CHANGELOG(TABLE raw_changes, DESCRIPTOR(op)));
```

## Acceleration and fallback

`FROM_CHANGELOG` and `TO_CHANGELOG` remain in Flink. When an upsert source causes Flink to insert
`StreamExecChangelogNormalize`, StreamFusion keeps the latest complete row per declared unique key
and produces Flink's exact `INSERT`, optional `UPDATE_BEFORE`, `UPDATE_AFTER`, and `DELETE`
changelog. A pushed filter removes a previously passing row when its replacement stops passing.
Processing-time state TTL uses the same expiry and duplicate-update rules as Flink. The stored row
supports every Arrow-representable Flink logical scalar and nested type; complex keys use Flink's
generated canonical key bytes.

Synchronous, non-mini-batch normalization is accelerated. Mini-batch bundle semantics, Flink async
state, changelog-state wrapping, mismatched input/output schemas, invalid keys, and logical types
without a portable Arrow representation produce an explicit whole-plan fallback reason.

When Flink inserts
`StreamExecDropUpdateBefore` because a downstream SQL operator or sink does not require retraction
records, StreamFusion accepts that node and drops only `UPDATE_BEFORE`. `INSERT`, `UPDATE_AFTER`,
and `DELETE` retain their values, `RowKind`, timestamp, and order. Watermarks and other control
events remain under Flink.

## Implementation

Changelog normalization receives and emits Arrow batches. Java evaluates Flink's generated pushed
filter through zero-copy Arrow-backed row views, then makes one Arrow C Data call per input batch.
Rust uses one backend multi-get for the batch's distinct keys, applies every row in input order,
makes one atomic backend write batch, and returns visible Arrow columns plus row-kind and input
envelope ordinals. Stored values are schema-aware Arrow rows; Java neither serializes state nor
reconstructs payload rows.

The managed-memory and direct RocksDB backends use Flink-compatible key groups and identical
versioned canonical state bytes. Key-group redistribution and memory-to-RocksDB restoration are
covered directly. The shared native keyed-state lifecycle supplies canonical savepoints, aligned
and unaligned checkpoint recovery, and incremental RocksDB SST reuse. State tables, scratch data,
RocksDB cache/write buffers, and transferred Arrow buffers are charged to Flink managed memory.
Standard I/O counters count logical rows. Additive StreamFusion metrics expose state read/write
batches, backend, memory, checkpoints/restores, processing failures, and TTL expirations.

`StreamFusionExecDropUpdateBefore` selects a focused JVM runtime operator because `RowKind` is
Flink changelog metadata rather than an Arrow data column. It does not create an Arrow boundary
merely to test the row kind. Adjacent native columnar stages still use Arrow batches normally.
Generated operator coverage compares all four row kinds and verifies timestamp and watermark
ordering. Native execution never collapses updates or deletes into append-only rows.

See the [Flink 2.3 Changelog conversion documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/changelog/).

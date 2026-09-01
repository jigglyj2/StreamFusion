---
title: Windowing TVFs
description: Acceleration coverage and fallback behavior for Flink SQL Windowing TVFs.
sidebar:
  order: 5
---

**Current status:** Accelerated for `TUMBLE`, `HOP`, `CUMULATE`, and `SESSION`.

StreamFusion has a distinct native physical operator for the row-semantics `TUMBLE`, `HOP`, and
`CUMULATE` TVFs. The set-semantics `SESSION` TVF uses a keyed native merging operator and emits at
the end of each session, matching Flink.

## SQL example

```sql
SELECT *
FROM TABLE(TUMBLE(TABLE bid, DESCRIPTOR(dateTime), INTERVAL '10' SECOND));
```

## Acceleration and fallback

An aligned Window TVF is eligible when its descriptor is an event-time `TIMESTAMP` column and its
output is the input row followed by `window_start`, `window_end`, and `window_time`. Native session
TVFs additionally support event time or processing time, `TIMESTAMP` or `TIMESTAMP_LTZ`, arbitrary
partition-key and payload types, local time zones, and daylight-saving transitions. Sizes, slides,
steps, offsets, negative timestamps, null timestamps, changelog kinds, and control-event ordering
follow Flink. A null event timestamp drops the row, as it does in Flink.

Aligned processing-time and `TIMESTAMP_LTZ` TVFs, an invalid descriptor, async-state mode,
changelog-state wrapping, or an unsupported surrounding node produce an EXPLAIN fallback reason. A
declared SQL watermark is eligible through StreamFusion's distinct, Flink-managed watermark node,
so supported event-time TVFs can accelerate end to end.

## Implementation

Java encodes the time-column ordinal, window kind, and millisecond size/slide/step/offset in the
versioned plan protobuf. A dedicated Rust physical operator expands Arrow batches and appends
Flink-compatible `window_start`, `window_end`, and `window_time` values. It uses Flink's exact epoch
arithmetic and emission order: newest start first for HOP and increasing end for CUMULATE. Input
columns and the bridge selection ordinal are gathered as Arrow arrays; rows are not serialized.
The operator also publishes Flink's `numNullRowTimeRecordsDropped` counter and updates it
when the same per-record null-time decision is made.

SESSION stores complete input rows as opaque BinaryRowData in native keyed state. It performs one
backend batch read and one atomic write per Arrow batch, merges sessions transitively using Flink's
inclusive boundary rule, replaces native timers as a session grows, and emits original records in
per-key input order with their original INSERT, UPDATE_BEFORE, UPDATE_AFTER, or DELETE kind. Memory
and direct RocksDB share canonical key-group and timer snapshot bytes for aligned/unaligned
checkpointing, cross-backend restore, and rescaling; RocksDB checkpoints reuse SST files.

See the [Flink 2.3 Windowing TVFs documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-tvf/).

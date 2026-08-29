---
title: Windowing TVFs
description: Acceleration coverage and fallback behavior for Flink SQL Windowing TVFs.
sidebar:
  order: 5
---

**Current status:** Partially accelerated.

StreamFusion has a distinct native physical operator for the row-semantics `TUMBLE`, `HOP`, and
`CUMULATE` TVFs. `SESSION` remains on Flink.

## SQL example

```sql
SELECT *
FROM TABLE(TUMBLE(TABLE bid, DESCRIPTOR(dateTime), INTERVAL '10' SECOND));
```

## Acceleration and fallback

An aligned Window TVF is eligible when its descriptor is an event-time `TIMESTAMP` column and its
output is the input row followed by `window_start`, `window_end`, and `window_time`. Sizes, slides,
steps, offsets, negative timestamps, null timestamps, changelog kinds, and control-event ordering
follow Flink. A null event timestamp drops the row, as it does in Flink.

The plan falls back with an EXPLAIN reason for processing time, `TIMESTAMP_LTZ`, `SESSION`, an
invalid descriptor, or an unsupported surrounding node. A declared SQL watermark is eligible
through StreamFusion's distinct, Flink-managed watermark node, so supported event-time TVFs can
accelerate end to end.

## Implementation

Java encodes the time-column ordinal, window kind, and millisecond size/slide/step/offset in the
versioned plan protobuf. A dedicated Rust physical operator expands Arrow batches and appends
Flink-compatible `window_start`, `window_end`, and `window_time` values. It uses Flink's exact epoch
arithmetic and emission order: newest start first for HOP and increasing end for CUMULATE. Input
columns and the bridge selection ordinal are gathered as Arrow arrays; rows are not serialized.
The operator also publishes Flink's `numNullRowTimeRecordsDropped` counter and updates it
when the same per-record null-time decision is made.

See the [Flink 2.3 Windowing TVFs documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-tvf/).

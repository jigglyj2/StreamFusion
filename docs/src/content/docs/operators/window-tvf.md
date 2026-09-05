---
title: Windowing TVFs
description: Acceleration coverage and fallback behavior for Flink SQL Windowing TVFs.
sidebar:
  order: 5
---

**Current status:** Streaming and bounded event-time `TUMBLE`, `HOP`, and `CUMULATE` are
accelerated. Streaming `SESSION` is also accelerated.

StreamFusion has a distinct native physical operator for the row-semantics `TUMBLE`, `HOP`, and
`CUMULATE` TVFs. The set-semantics `SESSION` TVF uses a keyed native merging operator and emits at
the end of each session, matching Flink.

## SQL example

```sql
SELECT *
FROM TABLE(TUMBLE(TABLE bid, DESCRIPTOR(dateTime), INTERVAL '10' SECOND));
```

## Acceleration and fallback

An aligned Window TVF is eligible when its descriptor is an event-time `TIMESTAMP` or
`TIMESTAMP_LTZ` column and its output is the input row followed by `window_start`, `window_end`, and
`window_time`. Native session TVFs additionally support event time or processing time, arbitrary
partition-key and payload types, local time zones, and daylight-saving transitions. Sizes, slides,
steps, offsets, negative timestamps, null timestamps, changelog kinds, and control-event ordering
follow Flink. A null event timestamp drops the row, as it does in Flink.

Aligned streaming processing-time TVFs require a per-record JVM clock reading and remain on
Flink. Flink itself rejects processing-time and session Window TVFs in batch mode, so StreamFusion
retains those batch validation errors rather than inventing different semantics. Native
processing-time window aggregation and the keyed SESSION TVF use Flink's processing-time service.
An invalid descriptor, async-state mode, changelog-state wrapping, or an unsupported surrounding
node produces an EXPLAIN fallback reason. A declared SQL watermark is eligible through
StreamFusion's distinct, Flink-managed watermark node, so supported event-time TVFs can accelerate
end to end.

## Implementation

Java encodes the time-column ordinal, window kind, and millisecond size/slide/step/offset in the
versioned plan protobuf. Both runtime modes lower to the same dedicated Rust physical operator,
which expands Arrow batches and appends
Flink-compatible `window_start`, `window_end`, and `window_time` values. It uses Flink's exact epoch
arithmetic, local-time-zone shifts, daylight-saving gap/overlap resolution, and emission order:
newest start first for HOP and increasing end for CUMULATE. Input
columns and the bridge selection ordinal are gathered as Arrow arrays; rows are not serialized.
The operator also publishes Flink's `numNullRowTimeRecordsDropped` counter and updates it when the
same per-record null-time decision is made. When an input Calc is fused below the TVF, the counter
lives on the native TVF plan node and therefore observes the post-filter timestamp column; Java
reads the node metric after draining the batch instead of scanning the pre-Calc input or adding a
second native boundary. Standard I/O metrics continue to count logical Flink records.

An adjacent supported Calc below the bounded TVF is nested as a DataFusion filter/projection below
the window operator in one native protobuf tree. The window operator consumes that Arrow output
directly. A benchmark plan such as `source -> Calc -> TUMBLE -> sink` consequently performs one
JNI invocation per source Arrow batch, while the Calc and TVF retain separate plan-node identities
and metrics. Aligned TVFs are stateless; memory versus RocksDB state-backend selection, savepoints,
rescaling, and incremental state checkpoints are therefore not applicable to this operator.

SESSION stores complete input rows in Arrow's schema-aware row format in native keyed state. It
performs one backend batch read and one atomic write per Arrow batch, merges sessions transitively
using Flink's inclusive boundary rule, and replaces native timers as a session grows. When a timer
fires, Rust decodes the original columns, appends all three window properties, and exports one Arrow
batch carrying the original INSERT, UPDATE_BEFORE, UPDATE_AFTER, or DELETE kinds in per-key input
order; Java does not reconstruct or transpose rows. Memory and direct RocksDB share canonical
key-group and timer snapshot bytes for aligned/unaligned checkpointing, cross-backend restore, and
rescaling; RocksDB checkpoints reuse SST files.

See the [Flink 2.3 Windowing TVFs documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-tvf/).

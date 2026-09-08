---
title: Watermark assignment
description: Plan coverage and ownership for Flink SQL watermark declarations.
sidebar:
  order: 6
---

**Current status:** Plan-compatible; execution remains intentionally Flink-owned.

## SQL example

```sql
CREATE TABLE bids (
  auction BIGINT,
  bid_time TIMESTAMP(3),
  WATERMARK FOR bid_time AS bid_time - INTERVAL '2' SECOND
) WITH (...);
```

The declaration tells Flink how event time progresses. It is not a relational data transformation
that benefits from Arrow vectorization.

## Acceleration and fallback

Watermark expressions that Flink 2.3 has validated and code-generated are eligible when the
watermark node preserves its input field types. They can appear inside an otherwise fully accelerated plan.
Other unsupported nodes still trigger whole-plan fallback; StreamFusion does not reinterpret or
approximate a watermark expression.

Computed timestamps whose precision changes at the watermark node currently trigger whole-plan
fallback with an explicit timestamp-precision reason. For example, Flink promotes a computed
`TIMESTAMP(0)` to rowtime `TIMESTAMP(3)`, but Arrow represents these in seconds and milliseconds
respectively. Forwarding that buffer without conversion is unsafe. Millisecond computed timestamps
are supported; generated SQL tests compare their complete changelog with Flink and verify fallback
parity for precision 0 and 1.

Watermark expression evaluation itself is not claimed as native acceleration. Idleness timeout,
watermark interval, source watermark alignment, and all other behavior use the corresponding Flink
settings and implementation without StreamFusion-specific toggles.

## Implementation

The planner replaces `StreamExecWatermarkAssigner` with the distinct
`StreamFusionExecWatermarkAssigner`, preserving the original expression, rowtime-field ordinal,
input property, and row type. During translation it uses Flink's own generated watermark expression
over zero-copy RowData views of Arrow batches. The Arrow control operator follows Flink's
watermark state machine and emits Arrow ranges; it never reconstructs the payload as rows.
Flink continues to own processing-time timers,
backpressure-aware idleness, active/idle status changes, ordering, maximum-watermark completion, and
recovery semantics. No Arrow boundary or Rust call is added for this control-plane-only node.

See the [Flink 2.3 time attributes documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/concepts/time_attributes/).

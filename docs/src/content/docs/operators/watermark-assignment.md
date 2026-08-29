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

Every watermark expression that Flink 2.3 has already validated and code-generated is eligible as a
StreamFusion physical node. It can therefore appear inside an otherwise fully accelerated plan.
Other unsupported nodes still trigger whole-plan fallback; StreamFusion does not reinterpret or
approximate a watermark expression.

Watermark expression evaluation itself is not claimed as native acceleration. Idleness timeout,
watermark interval, source watermark alignment, and all other behavior use the corresponding Flink
settings and implementation without StreamFusion-specific toggles.

## Implementation

The planner replaces `StreamExecWatermarkAssigner` with the distinct
`StreamFusionExecWatermarkAssigner`, preserving the original expression, rowtime-field ordinal,
input property, and row type. During translation it uses Flink's own generated watermark expression
and `WatermarkAssignerOperatorFactory`. Consequently Flink continues to own processing-time timers,
backpressure-aware idleness, active/idle status changes, ordering, maximum-watermark completion, and
recovery semantics. No Arrow boundary or Rust call is added for this control-plane-only node.

See the [Flink 2.3 time attributes documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/concepts/time_attributes/).

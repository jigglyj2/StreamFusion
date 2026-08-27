---
title: Group aggregation
description: Acceleration coverage and fallback behavior for Flink SQL Group aggregation.
sidebar:
  order: 6
---

**Current status:** Not accelerated; executed by Flink.

**Future acceleration target:** Yes.

## SQL example

```sql
SELECT bidder, COUNT(*) AS bids, SUM(price) AS spend\nFROM bid\nGROUP BY bidder;
```

## Acceleration and fallback

Accelerate supported grouping keys and aggregate functions when accumulator, retraction, overflow, null, and changelog semantics match Flink. Unsupported UDAFs, data views, or state layouts fall back.

## Implementation

Use DataFusion aggregate kernels where their semantics match. A streaming wrapper maintains keyed accumulators in Rust while Flink owns checkpoint and recovery coordination; mini-batching can amortize state access.

See the [Flink 2.3 Group aggregation documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/group-agg/).

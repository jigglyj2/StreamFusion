---
title: Window aggregation
description: Acceleration coverage and fallback behavior for Flink SQL Window aggregation.
sidebar:
  order: 7
---

**Current status:** Not accelerated; executed by Flink.

**Future acceleration target:** Yes.

## SQL example

```sql
SELECT window_start, bidder, SUM(price)\nFROM TABLE(TUMBLE(TABLE bid, DESCRIPTOR(dateTime), INTERVAL '1' MINUTE))\nGROUP BY window_start, window_end, bidder;
```

## Acceleration and fallback

Accelerate supported window TVFs and aggregates when watermark, lateness, time-zone, and changelog behavior are equivalent. Legacy group windows, unsupported UDAFs, and incompatible triggers fall back.

## Implementation

Combine custom window/state management with DataFusion aggregate kernels. Batch records by key and window while retaining Flink-compatible cleanup and output timing.

See the [Flink 2.3 Window aggregation documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-agg/).

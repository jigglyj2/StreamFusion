---
title: OVER aggregation
description: Acceleration coverage and fallback behavior for Flink SQL OVER aggregation.
sidebar:
  order: 8
---

**Current status:** Not accelerated; executed by Flink.

**Can it be accelerated?** Yes.

## SQL example

```sql
SELECT bidder, price,\n  AVG(price) OVER (PARTITION BY bidder ORDER BY dateTime\n    ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS recent_avg\nFROM bid;
```

## Acceleration and fallback

Accelerate only supported ROWS/RANGE frames, order keys, aggregates, and input changelog modes. Retractions, peer ordering, or frames without exact parity fall back.

## Implementation

Use custom ordered keyed state for the streaming frame and DataFusion aggregate kernels where useful. DataFusion alone does not provide Flink's incremental changelog and watermark behavior.

See the [Flink 2.3 OVER aggregation documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/over-agg/).


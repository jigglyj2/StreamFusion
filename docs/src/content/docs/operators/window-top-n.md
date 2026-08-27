---
title: Window Top-N
description: Acceleration coverage and fallback behavior for Flink SQL Window Top-N.
sidebar:
  order: 15
---

**Current status:** Not accelerated; executed by Flink.

**Can it be accelerated?** Yes.

## SQL example

```sql
SELECT * FROM (\n  SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY total DESC) AS rank_num\n  FROM window_totals\n) WHERE rank_num <= 3;
```

## Acceleration and fallback

Accelerate after a compatible window operation when rank bounds and ordering are supported. Unsupported window properties, rank functions, or changelog modes fall back.

## Implementation

Use custom per-window ordered state, releasing it according to Flink watermarks. Batch rank updates to avoid emitting unchanged rows.

See the [Flink 2.3 Window Top-N documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-topn/).


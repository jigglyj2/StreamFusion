---
title: Window deduplication
description: Acceleration coverage and fallback behavior for Flink SQL Window deduplication.
sidebar:
  order: 17
---

**Current status:** Not accelerated; executed by Flink.

**Can it be accelerated?** Yes.

## SQL example

```sql
SELECT * FROM (\n  SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end, id ORDER BY time_attr ASC) AS row_num\n  FROM windowed_events\n) WHERE row_num = 1;
```

## Acceleration and fallback

Accelerate recognized window-deduplication patterns over supported window TVFs and time attributes. Other rank shapes, order keys, or window configurations fall back.

## Implementation

Keep one candidate per key and window in custom Rust state, then finalize and clean state using Flink watermark progress.

See the [Flink 2.3 Window deduplication documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-deduplication/).


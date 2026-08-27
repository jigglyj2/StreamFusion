---
title: Window join
description: Acceleration coverage and fallback behavior for Flink SQL Window join.
sidebar:
  order: 10
---

**Current status:** Not accelerated; executed by Flink.

**Can it be accelerated?** Yes.

## SQL example

```sql
SELECT l.window_start, l.id, r.value\nFROM left_windowed l JOIN right_windowed r\nON l.id = r.id\nAND l.window_start = r.window_start\nAND l.window_end = r.window_end;
```

## Acceleration and fallback

Accelerate when both inputs use compatible windowing TVFs and the equality/window predicates meet Flink's window-join rules. Mismatched windows, unsupported conditions, or unsupported outer/semi/anti modes fall back.

## Implementation

Use custom per-window hash state and close it from Flink watermarks. DataFusion hash kernels may accelerate matching within a window, but lifecycle and changelog emission remain streaming-specific.

See the [Flink 2.3 Window join documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-join/).


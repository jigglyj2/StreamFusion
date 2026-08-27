---
title: Windowing TVFs
description: Acceleration coverage and fallback behavior for Flink SQL Windowing TVFs.
sidebar:
  order: 5
---

**Current status:** Not accelerated; executed by Flink.

**Can it be accelerated?** Yes.

## SQL example

```sql
SELECT *\nFROM TABLE(TUMBLE(TABLE bid, DESCRIPTOR(dateTime), INTERVAL '10' SECOND));
```

## Acceleration and fallback

TUMBLE, HOP, CUMULATE, and SESSION can accelerate after timestamp, watermark, offset, and late-event behavior have parity coverage. Unsupported descriptors or window parameters fall back.

## Implementation

Use custom Rust window assignment because streaming watermarks and Flink window metadata are outside DataFusion's batch execution model. Emit Flink-compatible window_start, window_end, and window_time columns.

See the [Flink 2.3 Windowing TVFs documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-tvf/).


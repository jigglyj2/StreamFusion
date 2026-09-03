---
title: Window deduplication
description: Acceleration coverage and fallback behavior for Flink SQL Window deduplication.
sidebar:
  order: 17
---

**Current status:** Accelerated for Flink's event-time Window Deduplicate physical node.

## SQL example

```sql
SELECT *
FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY window_start, window_end, id ORDER BY time_attr ASC
  ) AS row_num
  FROM windowed_events
)
WHERE row_num = 1;
```

## Acceleration and fallback

Recognized first/last `ROW_NUMBER() = 1` plans over attached `TUMBLE`, `HOP`, `CUMULATE`, and
`SESSION` windows are accelerated. Partition keys and complete payload rows support every Flink
logical type that can cross the Arrow/RowData boundary, including nested rows, arrays, maps, and
multisets. Ordering uses the planned rowtime timestamp, including Flink's stable first/last tie
behavior. Null rowtimes are ignored and records for windows already closed by the current watermark
are counted as late and dropped.

Flink does not currently plan processing-time Window Deduplicate. Non-attached windows, async-state
mode, and Flink's changelog-state wrapper are explicit whole-plan fallback conditions.

## Implementation

Rust encodes complete candidates with Arrow's schema-aware row format per Flink key group and
window. INSERT and UPDATE_AFTER add candidates; UPDATE_BEFORE and DELETE remove an exact candidate,
so retracting the winner reveals the next eligible row. The operator performs one backend batch read
and one atomic write per Arrow batch, registers native event-time timers, and decodes the final
INSERT directly into Arrow when the window closes. The Java bridge only attaches Flink's RowKind
envelope; it never reconstructs or transposes timer output through RowData.

Flink 2.3 rejects updating input before constructing the Window Deduplicate physical node, so
current SQL plans reach this path as append-only. Direct native changelog, restore, and rescaling
tests cover all four RowKinds for compatibility with future planner shapes.

Memory and direct RocksDB use the same canonical key-group/timer snapshot bytes. Canonical
savepoints can change backend and rescale; ordinary RocksDB checkpoints use the shared incremental
SST lifecycle.

See the [Flink 2.3 Window deduplication documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-deduplication/).

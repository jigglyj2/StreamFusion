---
title: Window Top-N
description: Acceleration coverage and fallback behavior for Flink SQL Window Top-N.
sidebar:
  order: 15
---

**Current status:** Accelerated for Flink's event-time Window Top-N physical node.

## SQL example

```sql
SELECT * FROM (\n  SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY total DESC) AS rank_num\n  FROM window_totals\n) WHERE rank_num <= 3;
```

## Acceleration and fallback

Constant-range `ROW_NUMBER` plans over attached `TUMBLE`, `HOP`, `CUMULATE`, and `SESSION` windows
are accelerated, with or without the rank-number output column. All partition-key and payload types
supported by the RowData/Arrow boundary are accepted. Sort fields retain Flink's generated
comparator, including ascending/descending direction, null placement, UTF-16 string ordering,
decimal/temporal semantics, composite values, and stable input-order ties.

`RANK`, `DENSE_RANK`, variable rank ranges, processing-time Window Top-N (which Flink does not plan),
async-state mode, and Flink's changelog-state wrapper fall back with an EXPLAIN reason.

## Implementation

Native memory or direct RocksDB stores full rows and sort keys as opaque BinaryRowData bytes. All
four input RowKinds are applied, so a retraction can expose a previously displaced candidate. One
native `multi_get` and one atomic mutation batch are used per Arrow input batch. At an event-time
timer, Rust releases the window candidates as one Arrow batch; Java applies the already-generated
Flink comparator once per window, selects the constant rank range, and transposes only the selected
rows. This avoids per-record JNI while keeping comparison semantics byte-for-byte aligned with
Flink.

State and timers use the canonical backend-neutral savepoint representation and the same aligned,
unaligned, incremental RocksDB, restore, and rescaling lifecycle as other native keyed operators.

See the [Flink 2.3 Window Top-N documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-topn/).

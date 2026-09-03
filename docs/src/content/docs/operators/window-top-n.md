---
title: Window Top-N
description: Acceleration coverage and fallback behavior for Flink SQL Window Top-N.
sidebar:
  order: 15
---

**Current status:** Accelerated for Flink's event-time Window Top-N physical node.

## SQL example

```sql
SELECT *
FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY window_start, window_end ORDER BY total DESC
  ) AS rank_num
  FROM window_totals
)
WHERE rank_num <= 3;
```

## Acceleration and fallback

Constant-range `ROW_NUMBER` plans over attached `TUMBLE`, `HOP`, `CUMULATE`, and `SESSION` windows
are accelerated, with or without the rank-number output column. All partition-key and payload types
supported by the RowData/Arrow boundary are accepted. Sort fields retain Flink's generated
comparison semantics in Rust, including ascending/descending direction, null placement, binary
string ordering, decimal/temporal semantics, composite values, and stable input-order ties.

`RANK`, `DENSE_RANK`, variable rank ranges, processing-time Window Top-N (which Flink does not plan),
async-state mode, and Flink's changelog-state wrapper fall back with an EXPLAIN reason.

## Implementation

Native memory or direct RocksDB stores each full candidate once in Arrow's schema-aware row format.
All four input RowKinds are applied, so a retraction can expose a previously displaced candidate.
One native `multi_get` and one atomic mutation batch are used per Arrow input batch. At an
event-time timer, Rust decodes the fired windows as one Arrow batch, applies the protobuf sort
contract, selects the constant rank range, and exports the visible Arrow columns plus RowKind
metadata directly. Java does not serialize sort keys or rows and does not reconstruct timer output.

Flink 2.3 rejects updating input before constructing the Window Top-N physical node, so current SQL
plans reach this path as append-only. Direct native changelog, restore, and rescaling tests cover
all four RowKinds for compatibility with future planner shapes.

State and timers use the canonical backend-neutral savepoint representation and the same aligned,
unaligned, incremental RocksDB, restore, and rescaling lifecycle as other native keyed operators.

See the [Flink 2.3 Window Top-N documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-topn/).

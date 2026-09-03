---
title: ORDER BY
description: Acceleration coverage and fallback behavior for Flink SQL ORDER BY.
sidebar:
  order: 12
---

**Current status:** Partially accelerated. Streaming `ORDER BY ... LIMIT/OFFSET` plans that Flink
lowers to `StreamExecSortLimit` use native Top-N state. A complete sort with no finite upper bound
remains on Flink.

## SQL example

```sql
SELECT auction, price
FROM bid
ORDER BY price DESC
LIMIT 100 OFFSET 10;
```

## Acceleration and fallback

Finite streaming sort-limit accepts the same Flink-valid order-key types, null placement,
ascending/descending directions, changelog strategies, state backends, and recovery contract as
[Top-N](../top-n/). The planner retains a full, unbounded global `ORDER BY` on Flink because it has
no finite streaming result and cannot be represented by the bounded candidate state.

## Implementation

The implemented finite path reuses the native Top-N protobuf node and Rust comparator rather than
creating a second sort-limit runtime. A future bounded full-sort path can use DataFusion's parallel,
spill-capable sort while Flink retains distribution and boundedness decisions.

See the [Flink 2.3 ORDER BY documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/orderby/).

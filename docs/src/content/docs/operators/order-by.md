---
title: ORDER BY
description: Acceleration coverage and fallback behavior for Flink SQL ORDER BY.
sidebar:
  order: 12
---

**Current status:** Partially accelerated. Streaming `ORDER BY ... LIMIT/OFFSET` plans that Flink
lowers to `StreamExecSortLimit` use native Top-N state. Time-ascending streaming sorts that Flink
lowers to `StreamExecTemporalSort` are also native. Other complete sorts remain on Flink.

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
[Top-N](../top-n/).

Temporal sort accepts Flink's event-time and processing-time forms. The first order field must be
the ascending time attribute. Secondary fields support every Flink-comparable scalar, array, and
row type with the planned direction and null placement; map, multiset, and raw values remain valid
payload fields but cannot be order fields. The complete Arrow payload, including nested collections
and `NULL`, is stored without a RowData conversion. Although the native runtime preserves every
input RowKind, Flink 2.3 only constructs this physical node for insert-only input.

Rows and timers use the selected native memory or direct RocksDB backend. Aligned and unaligned
checkpoints preserve pending timestamp groups; canonical savepoints move them between both
backends, and RocksDB checkpoints reuse unchanged SSTs. Event-time rows at or behind the last fired
timestamp are dropped exactly where Flink drops them. Processing-time rows are grouped by Flink's
millisecond timer boundary.

Temporal `ORDER BY` is necessarily global. Flink supplies a singleton distribution, and the native
translator enforces parallelism and max parallelism one. There is therefore no meaningful
key-group redistribution for this operator: scaling it above one would violate total ordering.
The planner retains any other full, unbounded global `ORDER BY` on Flink.

## Implementation

The finite path reuses the native Top-N protobuf node. Temporal sort has its own versioned protobuf
node, persistent native processor, raw keyed state, and timer service, while reusing the shared
Flink-compatible Arrow comparator and state/checkpoint interfaces. Java constructs the physical
plan and owns watermarks, barriers, distribution, recovery, and metric publication; Arrow C Data
crosses only at the fused-plan edge. This follows Comet's distinct replacement-node and protobuf
control-plane model. A future bounded full-sort path can use DataFusion's parallel, spill-capable
sort while Flink retains distribution and boundedness decisions.

See the [Flink 2.3 ORDER BY documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/orderby/).

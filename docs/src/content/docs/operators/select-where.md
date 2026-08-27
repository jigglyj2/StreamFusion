---
title: SELECT & WHERE
description: Acceleration coverage and fallback behavior for Flink SQL SELECT & WHERE.
sidebar:
  order: 2
---

**Current status:** Partially accelerated for the initial integer calc subset.

**Future acceleration target:** Yes.

## SQL example

```sql
SELECT auction, price * 2 AS doubled_price\nFROM bid\nWHERE price >= 100;
```

## Acceleration and fallback

StreamFusion currently accelerates a calc only when it projects one non-null `INT` input reference and has either no predicate or a predicate of the form `integer_column >= integer_literal`. DataFusion executes the filter and projection as separate vectorized physical operators. Any other type, expression, predicate, nullability, cast, or scalar function falls back to Flink.

## Implementation

Java serializes the calc as protobuf. Rust lowers it to a DataFusion `FilterExec`, when needed, followed by `ProjectionExec`. The first boundary implementation batches Flink `RowData` into an Arrow `IntVector`; output is exposed as a reusable `ColumnarRowData` over an Arrow-backed Flink column vector rather than allocating a `GenericRowData` per result. JNI still rebases the current integer vector to a zero-offset Java array while the Arrow C Data boundary is implemented.

See the [Flink 2.3 SELECT & WHERE documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/select/).

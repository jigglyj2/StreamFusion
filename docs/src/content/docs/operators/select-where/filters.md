---
title: Filters
description: Native WHERE predicate coverage and fallback rules.
sidebar:
  order: 2
---

**Current status:** Simple integer comparisons are accelerated.

## SQL example

```sql
SELECT name
FROM events
WHERE id >= 100;
```

## Acceleration and fallback

The supported predicates are `=`, `<>`, `<`, `<=`, `>`, and `>=` between an `INT`
column and an integer literal. Either operand order is accepted, and the filtered column
does not need to appear in the projection. A null input produces SQL unknown and is
removed by `WHERE`, matching Flink. Column-to-column comparisons, compound boolean
predicates, functions, and non-integer operands currently fall back to Flink.

`IS NULL` and `IS NOT NULL` are supported over direct input columns of every scalar type
listed on the [projection coverage page](../projections/).

Supported comparisons and null checks can be recursively composed with `AND`, `OR`, and
`NOT`. StreamFusion preserves SQL's three-valued boolean logic: true, false, and unknown
remain distinct throughout the expression, and `WHERE` retains only true rows.

A direct `BOOLEAN` column is also a supported condition, including inside a compound
tree or beneath `NOT`. Nullable boolean columns preserve unknown rather than coercing
null to false within the expression.

## Implementation

Java encodes the comparison as a protobuf expression. Rust lowers it to DataFusion
`Column`, `Literal`, `BinaryExpr`, `NotExpr`, `IsNullExpr`, and `IsNotNullExpr` nodes
inside a `FilterExec`. The following `ProjectionExec` consumes its Arrow batches directly. A
paired serializable Java evaluator retains the original Flink `RowKind` for each row
that survives native filtering.

See the [Flink WHERE-clause documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/select/#where-clause).

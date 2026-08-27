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

## Implementation

Java encodes the comparison as a protobuf expression. Rust lowers it to DataFusion
`Column`, `Literal`, and `BinaryExpr` nodes inside a `FilterExec`. The following
`ProjectionExec` consumes its Arrow batches directly.

See the [Flink WHERE-clause documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/select/#where-clause).

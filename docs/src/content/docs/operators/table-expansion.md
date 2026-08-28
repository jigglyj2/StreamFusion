---
title: Table and collection expansion
description: Acceleration coverage and fallback behavior for Flink SQL UNNEST and table functions.
sidebar:
  order: 12
---

**Current status:** inner/cross `UNNEST` over directly referenced arrays of supported scalar
values is accelerated. Other table functions and expansion forms fall back to Flink.

## SQL example

```sql
SELECT order_id, product_id
FROM orders
CROSS JOIN UNNEST(product_ids) AS products(product_id);
```

Each input row produces one output row per array element. Array order, duplicates, null elements,
and the input row's changelog `RowKind` are preserved. Null and empty arrays produce no rows.

## Acceleration and fallback

StreamFusion accelerates the operation when all of the following are true:

- Flink planned an inner/cross correlate around its built-in `$UNNEST_ROWS$` function.
- The function has one direct input-field operand whose type is `ARRAY`.
- The element is a supported scalar Arrow boundary type.
- The correlate has no additional condition and its output preserves every input field before
  appending the element field with exactly the array's element type.
- Every other internal node in the plan has a StreamFusion implementation.

`LEFT JOIN UNNEST`, `WITH ORDINALITY`, computed array operands, arrays of rows or nested
collections, maps, multisets, user-defined table functions, and correlate conditions currently
fall back. EXPLAIN identifies the rejected join form, function shape, operand, or element type and
then reports whole-plan fallback. These variants remain on Flink until their null-extension,
ordinality, field-expansion, and changelog behavior has dedicated parity coverage.

## Implementation

The Java planner replaces the eligible `StreamExecCorrelate` with the distinct
`StreamFusionExecArrayUnnest` node and sends a versioned `ArrayUnnest` protobuf operator to Rust.
Rust projects the input columns plus a lightweight duplicate reference to the array and executes
DataFusion's vectorized `UnnestExec` with `NullHandling::Drop`, matching Flink inner-join behavior.
DataFusion allocates take indices because repeating parent values is inherent to expansion; it
does not serialize rows or copy the array merely to hand it to the next native stage.

The hidden input-row ordinal is repeated with each element and remains the final Arrow column, so
the JVM restores the exact input `RowKind` for every produced row. An immediately following Calc
is nested above `ArrayUnnest` in the same DataFusion execution-plan tree, crosses the Arrow C Data
boundary only once, and consumes the expanded batch directly without a Java materialization.

See the [Flink 2.3 joins and UNNEST documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/joins/#unnest).

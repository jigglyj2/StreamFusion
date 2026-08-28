---
title: Table and collection expansion
description: Acceleration coverage and fallback behavior for Flink SQL UNNEST and table functions.
sidebar:
  order: 12
---

**Current status:** inner/cross and left `UNNEST`, with or without `WITH ORDINALITY`, over directly
referenced arrays of supported scalar values are accelerated. Other table functions and expansion
forms fall back to Flink.

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
- The element is a supported scalar Arrow boundary type, including numeric, boolean, character,
  binary, decimal, date, time, and timestamp values.
- The correlate has no additional condition and its output preserves every input field before
  appending the element field with exactly the array's element type.
- Every other internal node in the plan has a StreamFusion implementation.

`WITH ORDINALITY` is accelerated for the same scalar-array cases and appends Flink's non-null,
1-based `INT` position, restarting at one for every input array.

`LEFT JOIN UNNEST(array) ON TRUE` retains one null-extended result for a null or empty array and
otherwise emits the same ordered elements as the inner form. With ordinality, that synthetic row
also has a null position. Computed array operands, arrays of rows or nested collections, maps,
multisets, user-defined table functions, and correlate conditions currently fall back. EXPLAIN
identifies the rejected join form, function shape, operand, or element type and then reports
whole-plan fallback.

## Implementation

The Java planner replaces the eligible `StreamExecCorrelate` with the distinct
`StreamFusionExecArrayUnnest` node and sends a versioned `ArrayUnnest` protobuf operator to Rust.
Rust projects the input columns plus a lightweight duplicate reference to the array and executes
DataFusion's vectorized `UnnestExec` with `NullHandling::Drop`, matching Flink inner-join behavior.
The left form selects `PreserveAndExpandEmpty`, which creates exactly one nullable element for a
null or empty array and retains the parent-row ordinal for changelog restoration.
DataFusion allocates take indices because repeating parent values is inherent to expansion; it
does not serialize rows or copy the array merely to hand it to the next native stage.
For `WITH ORDINALITY`, StreamFusion derives a second Arrow list from the source offsets, fills its
values with vectorized 1-based positions, and unnests the value and position lists together.

The hidden input-row ordinal is repeated with each element and remains the final Arrow column, so
the JVM restores the exact input `RowKind` for every produced row. An immediately following Calc
is nested above `ArrayUnnest` in the same DataFusion execution-plan tree, crosses the Arrow C Data
boundary only once, and consumes the expanded batch directly without a Java materialization.

See the [Flink 2.3 joins and UNNEST documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/joins/#unnest).

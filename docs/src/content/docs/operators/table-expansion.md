---
title: Table and collection expansion
description: Acceleration coverage and fallback behavior for Flink SQL UNNEST and table functions.
sidebar:
  order: 12
---

**Current status:** inner/cross and left `UNNEST`, with or without `WITH ORDINALITY`, over directly
referenced arrays of supported scalar values are accelerated. Inner/cross expansion also supports
arrays of scalar-field rows. Inner/cross and left expansion of maps with supported scalar or
scalar-field row keys and values is accelerated, with or without ordinality. The same forms accelerate multisets of
supported non-null scalar elements. Arrays whose elements are scalar arrays are also accelerated,
with each inner array remaining one output value. Other table functions and expansion forms fall back to Flink.

## SQL example

```sql
SELECT order_id, product_id
FROM orders
CROSS JOIN UNNEST(product_ids) AS products(product_id);

SELECT order_id, attribute_key, attribute_value, position
FROM orders
CROSS JOIN UNNEST(attributes) WITH ORDINALITY
  AS entries(attribute_key, attribute_value, position);

SELECT tag, position
FROM tag_bags
CROSS JOIN UNNEST(tags) WITH ORDINALITY AS entries(tag, position);
```

Each input row produces one output row per array element. Array order, duplicates, null elements,
and the input row's changelog `RowKind` are preserved. Null and empty arrays produce no rows.

## Acceleration and fallback

StreamFusion accelerates the operation when all of the following are true:

- Flink planned an inner/cross or left correlate around its built-in `$UNNEST_ROWS$` function.
- The function has one direct input-field operand whose type is `ARRAY`, `MAP`, or `MULTISET`.
- The element is a supported scalar Arrow boundary type, including numeric, boolean, character,
  binary, decimal, date, time, and timestamp values; a non-empty `ROW` composed of those types; or
  an `ARRAY` of one of those scalar types.
- The correlate has no additional condition and its output preserves every input field before
  appending the array element or map key/value fields with exactly Flink's types.
- Every other internal node in the plan has a StreamFusion implementation.

`WITH ORDINALITY` is accelerated for the same scalar-array cases and appends Flink's non-null,
1-based `INT` position, restarting at one for every input array.

`LEFT JOIN UNNEST(array) ON TRUE` retains one null-extended result for a null or empty array and
otherwise emits the same ordered elements as the inner form. Arrays of `ROW` flatten each element
into its named fields, and a null row element produces null for every flattened field. With
ordinality, the synthetic row also has a null position. Map expansion preserves the paired key and
value arrays and assigns positions in Flink's stored `MapData` entry order; SQL map ordering is not
otherwise guaranteed. Left expansion of arrays of rows, computed collection operands, rows
containing nested collection fields, arrays nested more than one level, maps with nested-collection keys or values,
multisets with nullable or complex elements, user-defined table functions, and correlate
conditions currently fall back. EXPLAIN identifies the rejected join form, function shape,
operand, or element type and then reports whole-plan fallback.

## Implementation

The Java planner replaces the eligible `StreamExecCorrelate` with the distinct
`StreamFusionExecArrayUnnest` node and sends a versioned `ArrayUnnest` protobuf operator to Rust.
Rust projects the input columns plus a lightweight duplicate reference to the array and executes
DataFusion's vectorized `UnnestExec` with `NullHandling::Drop`, matching Flink inner-join behavior.
The left form selects `PreserveAndExpandEmpty`, which creates exactly one nullable element for a
null or empty array and retains the parent-row ordinal for changelog restoration.
For arrays of rows, a native `IS NOT NULL` filter reproduces Flink's behavior of skipping null row
elements. A projection immediately above it applies DataFusion `get_field` expressions to the
Arrow struct and exposes Flink's flattened columns. Both stages remain inside the same native
plan.
For maps, a lightweight physical expression reinterprets Arrow's map offsets and entry struct as
a list without copying its key, value, offset, or validity buffers. `UnnestExec` expands that list,
and the same struct projection exposes the paired key and value columns. Ordinality is derived
from those shared offsets, so it follows the exact entry order received from Flink.
For multisets, the Arrow boundary uses a map-shaped element/count representation for non-null
elements. Rust builds vectorized take indices from each non-negative count, gathers the element
buffer once, and assigns ordinality across the expanded sequence exactly as Flink does. Creating
the repeated output is inherent to multiset expansion; adjacent native operators still consume
the resulting Arrow batch directly.
DataFusion allocates take indices because repeating parent values is inherent to expansion; it
does not serialize rows or copy the array merely to hand it to the next native stage.
For `WITH ORDINALITY`, StreamFusion derives a second Arrow list from the source offsets, fills its
values with vectorized 1-based positions, and unnests the value and position lists together.

The hidden input-row ordinal is repeated with each element and remains the final Arrow column, so
the JVM restores the exact input `RowKind` for every produced row. An immediately following Calc
is nested above `ArrayUnnest` in the same DataFusion execution-plan tree, crosses the Arrow C Data
boundary only once, and consumes the expanded batch directly without a Java materialization.

See the [Flink 2.3 joins and UNNEST documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/joins/#unnest).

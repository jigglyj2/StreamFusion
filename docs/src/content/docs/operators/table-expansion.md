---
title: Table and collection expansion
description: Acceleration coverage and fallback behavior for Flink SQL UNNEST and table functions.
sidebar:
  order: 12
---

**Current status:** inner/cross and left `UNNEST`, with or without `WITH ORDINALITY`, over directly
referenced arrays of supported scalar values are accelerated. Inner/cross expansion also supports
arrays of scalar-field rows. Inner/cross and left expansion of maps with supported scalar or
scalar-field row keys and scalar, scalar-array, or row values composed of scalars and scalar arrays
is accelerated, with or without ordinality. The same forms accelerate multisets of supported
non-null scalar or row elements composed of scalars and scalar arrays. Arrays whose
elements are scalar arrays are also accelerated, with each inner array remaining one output value.
Computed collection operands are accelerated when their complete expression is supported by
StreamFusion Calc. This includes `ARRAY[...]`, supported array and map functions, and nested row
fields containing supported arrays, maps, or multisets. Other table functions and expansion forms
fall back to Flink. Adjacent supported `UNNEST` operations are accelerated as one native plan.

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

SELECT item, position
FROM measurements
CROSS JOIN UNNEST(ARRAY[value, value + 1, CAST(NULL AS INT)]) WITH ORDINALITY
  AS expanded(item, position);

SELECT outer_position, item, inner_position
FROM nested_measurements
CROSS JOIN UNNEST(value_groups) WITH ORDINALITY AS outer_values(values, outer_position)
CROSS JOIN UNNEST(values) WITH ORDINALITY AS inner_values(item, inner_position);

SELECT item, position
FROM UNNEST(ARRAY[1, CAST(NULL AS INT), 3]) WITH ORDINALITY AS values(item, position);

SELECT map_key, map_value, position
FROM UNNEST(MAP['first', 1, 'nullable', CAST(NULL AS INT)]) WITH ORDINALITY
  AS entries(map_key, map_value, position);
```

Each input row produces one output row per array element. Array order, duplicates, null elements,
and the input row's changelog `RowKind` are preserved. Null and empty arrays produce no rows.

## Acceleration and fallback

StreamFusion accelerates the operation when all of the following are true:

- Flink planned an inner/cross or left correlate around its built-in `$UNNEST_ROWS$` function.
- The function has one `ARRAY`, `MAP`, or `MULTISET` operand that is either a direct field or an
  expression the Calc expression translator supports exactly.
- The element is a supported scalar Arrow boundary type, including numeric, boolean, character,
  binary, decimal, date, time, and timestamp values; a non-empty `ROW` composed of those types and
  scalar arrays; or an `ARRAY` of one of those scalar types.
- The correlate has no additional condition and its output preserves every input field before
  appending the array element or map key/value fields with exactly Flink's types.
- Every other internal node in the plan has a StreamFusion implementation.

`WITH ORDINALITY` is accelerated for the same scalar-array cases and appends Flink's non-null,
1-based `INT` position, restarting at one for every input array.

`LEFT JOIN UNNEST(array) ON TRUE` retains one null-extended result for a null or empty array and
otherwise emits the same ordered elements as the inner form. Arrays of `ROW` flatten each element
into its named fields and omit null row elements, matching Flink. For supported left expansion,
the synthetic row also has a null position when ordinality is requested. Map expansion preserves
the paired key and value arrays and assigns positions in Flink's stored `MapData` entry order; SQL
map ordering is not otherwise guaranteed. In left expansion of arrays of rows, null and empty
collections still produce exactly one synthetic all-null row. Unsupported computed collection
expressions, rows containing maps, multisets, or arrays nested more than one level, arrays
nested more than one level, maps with collection keys or collection values outside the documented
scalar-array shapes,
nullable row-array elements with ordinality, multisets with nullable or array elements or collections nested more than one level,
`UNNEST(MAP_ENTRIES(map))` (because Flink 2.3 reports nullable entry rows while preserving a
non-null map-key output),
user-defined table functions, and correlate
conditions currently fall back. EXPLAIN identifies the rejected join form, function shape,
operand, or element type and then reports whole-plan fallback.

Flink 2.3's row-array `WITH ORDINALITY` implementation violates its own output arity contract when
it encounters a null row element. StreamFusion deliberately falls back for nullable row elements
so it does not replace that failure with different observable behavior; EXPLAIN identifies this
version-specific parity restriction.

`MAP_KEYS(map)` and `MAP_VALUES(map)` can be computed and expanded in the same native plan.
`MAP_ENTRIES(map)` remains accelerated as a projection, but directly expanding that computed
array currently falls back because its Flink 2.3 row/nullability contract differs from an ordinary
array of rows. StreamFusion reports the mismatched entry field in EXPLAIN rather than weakening
type validation.

MULTISET elements that are themselves arrays fall back even though the Arrow boundary can carry
the type. Flink's map serialization can reorder array keys relative to Java insertion order, and
`WITH ORDINALITY` makes that ordering difference observable. StreamFusion therefore keeps this
shape on Flink until it can reproduce the serialized key order exactly.

## Implementation

The Java planner replaces the eligible `StreamExecCorrelate` with the distinct
`StreamFusionExecArrayUnnest` node and sends a versioned `ArrayUnnest` protobuf operator to Rust.
The protobuf retains its field index for existing direct-column plans and optionally carries the
same typed `Expression` contract used by Calc. Rust lowers a computed operand through the shared
DataFusion expression planner before `UnnestExec`, keeping expression evaluation and expansion in
one native execution-plan tree and crossing the Arrow boundary only once.
Source-free constructor expansion uses Flink's zero-column, one-row values input. The Arrow
boundary carries its explicit row count even though it has no vectors, and native scalar array or
map expressions are broadcast to that row before expansion and ordinality are derived.
When Flink produces adjacent correlate nodes, the planner nests their `ArrayUnnest` protobufs in
input-to-output order and installs one JVM operator around the entire chain. Each DataFusion
`UnnestExec` consumes the preceding stage's Arrow output directly; intermediate arrays, repeated
parent columns, and ordinality columns never return to Java. A following Calc is nested above the
same chain, so projection and filtering do not introduce another boundary.
Rust projects the input columns plus a lightweight duplicate reference to the array and executes
DataFusion's vectorized `UnnestExec` with `NullHandling::Drop`, matching Flink inner-join behavior.
The left form selects `PreserveAndExpandEmpty`, which creates exactly one nullable element for a
null or empty array and retains the parent-row ordinal for changelog restoration.
For arrays of rows, a native `IS NOT NULL` filter reproduces Flink's behavior of skipping null row
elements. A projection immediately above it applies DataFusion `get_field` expressions to the
Arrow struct and exposes Flink's flattened columns. Both stages remain inside the same native
plan. For the left form, the native plan retains an internal ordinality list even when SQL does
not request it. A null ordinal identifies the synthetic row created for a null or empty array;
the marker is projected away before crossing the JVM boundary.
For maps, a lightweight physical expression reinterprets Arrow's map offsets and entry struct as
a list without copying its key, value, offset, or validity buffers. `UnnestExec` expands that list,
and the same struct projection exposes the paired key and value columns. Ordinality is derived
from those shared offsets, so it follows the exact entry order received from Flink.
For multisets, the Arrow boundary uses a map-shaped element/count representation for non-null
elements. Rust builds vectorized take indices from each non-negative count, gathers the element
buffer once, and assigns ordinality across the expanded sequence exactly as Flink does. Row
elements are gathered as Arrow structs and flattened by the same native field projection used for
arrays of rows. Creating the repeated output is inherent to multiset expansion; adjacent native
operators still consume the resulting Arrow batch directly.
DataFusion allocates take indices because repeating parent values is inherent to expansion; it
does not serialize rows or copy the array merely to hand it to the next native stage.
For `WITH ORDINALITY`, StreamFusion derives a second Arrow list from the source offsets, fills its
values with vectorized 1-based positions, and unnests the value and position lists together.

The hidden input-row ordinal is repeated with each element and remains the final Arrow column, so
the JVM restores the exact input `RowKind` for every produced row. An immediately following Calc
is nested above `ArrayUnnest` in the same DataFusion execution-plan tree, crosses the Arrow C Data
boundary only once, and consumes the expanded batch directly without a Java materialization.

See the [Flink 2.3 joins and UNNEST documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/joins/#unnest).

---
title: Table and collection expansion
description: Acceleration coverage and fallback behavior for Flink SQL UNNEST and table functions.
sidebar:
  order: 12
---

**Current status:** a standalone supported `UNNEST` stage remains eligible. Adjacent
Calc/UNNEST and UNNEST/UNNEST combinations temporarily cause whole-plan Flink fallback under
[architecture admission](/StreamFusion/development/architecture-admission/). This also affects
computed collections when Flink adds an adjacent Calc. The shared native-region implementations
remain covered by direct tests.

Selected UNNEST nodes now build standalone protobuf fragments for the common planner region
collector, alongside Calc, Expand, and row replication. Neither the collector nor the runtime
requires a Calc/UNNEST-specific executor. Mixed-region tests cover complete changelog bytes and
per-stage logical counts against Flink, including null collections/elements and retractions;
the broader memory and metric-surface admission audit is still incomplete. Expand and row-replication
output reservations now follow their Arrow buffers through downstream retention and producer close;
kernel pre-allocation admission remains a separate unfinished requirement.

Expand now produces at most 4,096 rows per native stream pull instead of materializing the
complete input/projection Cartesian product. It preserves input-row-major projection order,
including when a single input row has more than 4,096 projections, and retains only the input
and cursor between pulls. Input slicing shares Arrow buffers. Single-projection chunks also
reuse their evaluated buffers rather than performing an identity gather. Projection descriptors,
selection indices, and repeated literal storage acquire managed-memory allowances before allocation;
interleaving uses a conservative allowance based on the evaluated arrays, not average row width.
Expand now uses Calc's shared expression-admission installation and projection-root scalar materializer,
including declared-width admission for NULL fixed-binary literals. Cached literals are borrowed, and
the immutable projection table is shared across invocations instead of cloning its nested vectors.
Input-slice descriptors are admitted using the common Arrow descriptor estimate before slicing, then
released before the temporary workspace is reduced to output ownership. These changes add no fusion
pair rules or Java handoffs; row-major interleaving remains the Flink-semantic reason for the custom
physical operator rather than a projection-major DataFusion union.
This is a row-count bound, not a fixed byte limit: wide or deeply nested values still consume
their actual storage. General expression scratch-space admission remains unfinished. Generated
Flink-codegen tests cover multi-pull changelog ordering and each stage's logical record counts;
native tests also cover sliced nested values, large projection lists, cancellation, and denial.
New generated cases exercise arithmetic, column-dependent REPEAT, scalar REPEAT and typed NULL projections
through surrounding Calc stages, with all RowKinds and varied batch boundaries. Allocation observations
check that large REPEAT, declared-width NULL binary, and wide bufferless slicing fail before their
payload/descriptor allocation. Arbitrary unaudited expression policies and complete metric-surface
admission remain unfinished; this does not remove the production composition gate or establish
benchmark performance.

SQL Expand and Calc now declare the shared plan-protocol-3 timestamp policy: like Flink's generated
SQL collectors, they drop StreamRecord timestamps while retaining SQL rowtime columns and RowKinds.
The common native stage boundary implements this behavior; it is not a Calc/Expand fusion adapter.
Payload arrays are forwarded directly, and metadata vectors use the existing managed projection
materializer. Existing protocol-1/2 plans retain their previous envelope semantics.
Generated Calc/Expand/Calc harness coverage compares serialized rows and record timestamps, all four
RowKinds, watermarks, idle/active transitions, latency markers, pre-barrier callbacks and terminal
paths. It discovers each stage's registered Flink metric subtree and latency histogram scopes,
comparing names/types, deterministic counters/gauges and histogram counts while checking actual
Flink meter/histogram implementations and runtime-dependent value semantics. This covers the tested
INT/NULL projection shape, including multi-pull output; it is not a claim of full operator admission
or in-flight checkpoint recovery. A separate generated UNION test covers timestamp-clearing and
timestamp-preserving branches in the same native tree.

**Retained implementation scope:** streaming and bounded inner/cross and left `UNNEST`, with or without
`WITH ORDINALITY`, over directly
referenced arrays of supported scalar values are accelerated. Inner/cross expansion also supports
arrays of rows whose fields are scalars or recursively nested arrays. Inner/cross and left expansion of maps with supported scalar or
scalar-field row keys and scalar, recursively nested array, or row values composed of scalars and recursively nested arrays
is accelerated, with or without ordinality. The same forms accelerate multisets of supported
non-null scalar or row elements composed of scalars and recursively nested arrays. Arrays whose
elements are recursively nested arrays are also accelerated, with each inner array remaining one output value.
Computed collection operands are accelerated when their complete expression is supported by
StreamFusion Calc. This includes `ARRAY[...]`, supported array and map functions, and nested row
fields containing supported arrays, maps, or multisets. Other table functions and expansion forms
fall back to Flink. Adjacent supported `UNNEST` implementations form one native plan in direct tests,
but SQL selection is temporarily gated pending complete stage metric parity.

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
  binary, decimal, date, time, and timestamp values; a non-empty `ROW` recursively composed of
  those types, arrays, and maps with supported keys and recursively supported values; an `ARRAY`
  recursively containing another supported array element; or such a supported map value.
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
collections still produce exactly one synthetic all-null row. Map-valued fields inside expanded
rows remain Arrow map children and are not materialized in Java. Unsupported computed collection
expressions, rows containing multisets, maps with collection keys or values outside the
documented recursive shapes,
nullable row-array elements with ordinality, and multisets with nullable or direct array elements,
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

The Java planner replaces an eligible `StreamExecCorrelate` or `BatchExecCorrelate` with the
distinct `StreamFusionExecArrayUnnest` or `StreamFusionBatchExecArrayUnnest` node and sends the
same versioned `ArrayUnnest` protobuf operator to Rust. The bounded implementation is not a
row-oriented alternate path and has no operator state, so the configured keyed-state backend is
not involved.
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

Calcs immediately below or above an UNNEST are nested around it in the same protobuf tree. This
keeps projection pruning, collection computation, expansion, filtering, and final projection in
one Comet-style native plan and one Arrow/JNI invocation per source batch.
The hidden input-row ordinal is repeated with each element and remains the final Arrow column, so
the JVM restores the exact input `RowKind` for every produced row. An immediately following Calc
is nested above `ArrayUnnest` in the same DataFusion execution-plan tree, crosses the Arrow C Data
boundary only once, and consumes the expanded batch directly without a Java materialization.

See the [Flink 2.3 joins and UNNEST documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/joins/#unnest).

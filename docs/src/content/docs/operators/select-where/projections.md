---
title: Projections
description: Native SELECT projection coverage and fallback rules.
sidebar:
  order: 1
---

**Current status:** Input references, literals, supported arithmetic/casts, conditionals,
and the explicitly listed stateless functions are accelerated.

## SQL example

```sql
SELECT name, enabled, id
FROM events
WHERE id >= 1;
```

## Acceleration and fallback

StreamFusion can select, reorder, omit, or repeat direct input columns of these types:

- `BOOLEAN`, `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `FLOAT`, and `DOUBLE`
- `CHAR`, `VARCHAR`, `BINARY`, and `VARBINARY`
- `DECIMAL`, `DATE`, `TIME`, `TIMESTAMP`, and `TIMESTAMP_LTZ`
- `ARRAY`, `MAP`, and `ROW`, recursively containing Arrow-compatible types

Complex types may be selected, reordered, omitted, or repeated as direct input references.
Named `ROW` fields may also be projected or used inside supported expressions, including
chains through nested rows. A null parent row produces a null child value, matching Flink.
Typed `NULL` literals are accelerated for `ARRAY`, `MAP`, and `ROW`, including recursively
nested arrays and rows. One-based `ARRAY` access is accelerated for positive integer literal
indexes; a null array, null element, or index beyond the array length produces null. The
selected element may itself be complex, so expressions such as `rows[1].name` are supported.
Map lookup is accelerated when both the map and key are otherwise supported expressions of
the declared map types; present values, present null values, absent keys, and null maps match
Flink's scalar result. `CARDINALITY` is accelerated for maps and non-nested arrays, returning
an `INT` count or null for a null collection. Nested-array cardinality remains on Flink because
Flink counts the outer array while DataFusion recursively counts leaf elements. Zero, negative,
and computed array indexes, non-null `MAP` and `ROW` literals,
`MULTISET`, and collection functions not explicitly listed below still fall back with the whole Calc. Nested child types, field names,
ordering, nullability, and Arrow offsets are preserved across the native plan.

Precision, scale, fixed width, and nullability are preserved. Constant `TINYINT` and
`SMALLINT` projections are accelerated across their complete value ranges. Computed `INT`, `BIGINT`,
`FLOAT`, `DOUBLE`, and `DECIMAL` projections support literals and recursively nested
addition, subtraction, and multiplication. Decimal operands retain the precision and
scale assigned to each expression by Flink's planner, including the precision growth of
intermediate results. Signed integer overflow follows Flink's wrapping semantics.
Floating-point literals must be finite. Constant `TRUE` and `FALSE` projections are also
accelerated, as are constant `DATE` values, including dates before the Unix epoch.
Unary minus is accelerated for the same numeric types.
Direct `DOUBLE` division is accelerated for arbitrary supported operands. It follows
IEEE-754 semantics for positive and negative zero divisors, infinities, and NaN results.
`FLOAT` division falls back because Flink plans that expression with widening casts.
Boolean projections can recursively compose direct boolean columns and constants with
`NOT`, `AND`, and `OR` using SQL's three-valued logic. Every comparison and null-check
shape listed on the [filter coverage page](../filters/) can also be projected as a boolean
value instead of being used by `WHERE`.
Null-safe `IS TRUE`, `IS FALSE`, `IS NOT TRUE`, and `IS NOT FALSE` expressions are
accelerated and always produce a non-null boolean result.
Null-safe `IS DISTINCT FROM` and `IS NOT DISTINCT FROM` comparison results are also
accelerated for the types supported by filters.
`INT` and `BIGINT` division is accelerated when the divisor is a direct nonzero literal;
signed results truncate exactly as Flink does. `MOD` remainder supports the same types
and divisor restriction. A planner representation that wraps a negative `BIGINT` divisor
in a cast or unary expression falls back because it is no longer a direct literal.
Lossless signed-integer widening projections are accelerated: `TINYINT` to `SMALLINT`,
`INT`, or `BIGINT`; `SMALLINT` to `INT` or `BIGINT`; and `INT` to `BIGINT`.
Integer-to-floating projections are accelerated for every signed integer source and
`FLOAT` or `DOUBLE` target. Wider integer inputs use the same IEEE-754 rounding as Flink;
parity coverage includes the `FLOAT` and `DOUBLE` precision cliffs and integer extrema.
`FLOAT` to `DOUBLE` and `DOUBLE` to `FLOAT` are accelerated. Narrowing uses Flink's
IEEE-754 rounding and preserves signed zero and NaN behavior; magnitudes beyond the
`FLOAT` range become positive or negative infinity as they do in Flink.
`SMALLINT` to `TINYINT`, `INT` to `TINYINT` or `SMALLINT`, and `BIGINT` to
`TINYINT`, `SMALLINT`, or `INT` are accelerated with Flink's two's-complement wrapping behavior for
out-of-range values. StreamFusion uses a custom vectorized Rust expression because
DataFusion's standard cast rejects those values instead of matching Flink.
Constant `TIME` values preserve their declared precision and millisecond-of-day value.
Constant `TIMESTAMP WITHOUT TIME ZONE` values preserve their local calendar value and
sub-millisecond nanoseconds without applying a session or JVM timezone.
Direct constant character literals are encoded as UTF-8 and accelerated. Character
expressions that require a planner-inserted cast still fall back with the whole Calc.
Direct hexadecimal binary literals are accelerated with their exact byte sequence and
fixed width. Cast-derived and computed binary expressions remain on Flink.
Typed `NULL` literals are accelerated for the supported scalar projection types except
`TIMESTAMP_LTZ`, and for `ARRAY`, `MAP`, and `ROW`. The protobuf carries the complete
recursive declared type, including `CHAR(n)` and `BINARY(n)` width, so
DataFusion materializes a correctly typed all-null Arrow vector rather than an untyped
Arrow `Null` vector. Arrow requires map keys to be non-nullable, so StreamFusion normalizes
that schema bit while preserving the Flink map container and value nullability; a null map
contains no keys and therefore cannot expose a semantic difference.
`COALESCE` is accelerated when it has at least two arguments and every argument can be
lowered as the same supported Flink result type. Nullability and left-to-right first-non-null
selection are preserved; an unsupported argument causes the whole Calc to fall back.
Searched and simple `CASE` projections and three-argument `IF` are accelerated when every
condition and result branch is otherwise supported and Flink assigns one common result type.
Conditions are evaluated in order, null conditions do not match, and the final `ELSE` value
is required. Any unsupported condition or branch causes whole-Calc fallback.

Integer division or remainder by zero or a non-literal divisor, decimal division and
remainder, floating-point remainder, unary plus, non-decimal mixed-width arithmetic, non-finite
floating-point literals, arithmetic on other types, casts, and unlisted functions currently
fall back to Flink, except for the lossless casts and functions listed above. The Calc also falls back if its
filter is unsupported.

## Implementation

Java recursively encodes input references, literals, and arithmetic in the protobuf
plan through the same typed serializer used by filters, following Comet's single
expression-to-protobuf model. Rust maps them to DataFusion `Column`, `Literal`, and `BinaryExpr` expressions in a
`ProjectionExec`. DataFusion shares referenced Arrow buffers for direct projections,
including the parent and child buffers of arrays, maps, and rows, and allocates a result
vector when arithmetic produces new values.

For `ROW` access, Java resolves every field against Flink's authoritative row type and
encodes its name as a nested protobuf expression. Rust lowers each step to DataFusion's
vectorized `get_field`. The child value buffers remain shared where Arrow validity permits;
DataFusion may create a validity bitmap to combine nullable parent and child rows.
Positive literal array indexes are encoded as `INT64` in the protobuf and lowered to
DataFusion's vectorized `array_element`, which has the same one-based and out-of-range-null
behavior. StreamFusion deliberately rejects the remaining index shapes until their Flink
semantics have dedicated parity coverage.
DataFusion's `map_extract` returns a one-element nullable list rather than Flink's scalar map
value. StreamFusion follows Comet's composition model and immediately applies
`array_element(..., 1)` within the same native expression tree, producing the Flink scalar
shape without crossing the JVM boundary.
Flat-array and map `CARDINALITY` lower to DataFusion's vectorized nested-type function. Its
unsigned result is safely narrowed to Flink's `INT`: Arrow list and map offsets cap a single
row's collection length within the signed 32-bit range.
`ARRAY_CONTAINS` is accelerated in projections and filters when its needle is provably
non-null and both arguments otherwise lower natively. Null arrays yield null, and null elements
do not prevent a non-null needle from matching another element. Nullable needles remain on
Flink with an EXPLAIN reason: Flink treats a null needle as a search for a null element, whereas
DataFusion returns null without searching the array.
`ARRAY_REVERSE` is accelerated for Arrow-compatible element types, including nested `ROW`
elements and nullable elements. It lowers directly to DataFusion's vectorized array reversal;
null and empty arrays retain Flink's behavior, and the resulting Arrow array stays inside the
fused native plan until the output boundary.
`ARRAY_APPEND` and `ARRAY_PREPEND` are accelerated when the input array and element are
otherwise supported expressions, including null elements and complex elements obtained from
another native expression. StreamFusion wraps DataFusion's vectorized kernels with an explicit
null-array guard because DataFusion otherwise treats a null array like an empty array, while
Flink requires the result to remain null. Element nullability in the result follows Flink's
planned array type.
`ARRAY_CONCAT` is accelerated for two or more otherwise supported array expressions with the
common array type resolved by Flink, including arrays of nested rows and composition with other
native array functions. Empty inputs contribute no elements. StreamFusion adds a null guard
around DataFusion's vectorized concatenation because Flink returns null when any input array is
null, while DataFusion otherwise skips null arrays. The concatenated batch remains inside the
fused native plan until its output boundary.
`ARRAY_POSITION` is accelerated when its array and search value otherwise lower natively. It
returns the first one-based match, `0` when no non-null element matches, and null when either
the array or search value is null. StreamFusion wraps DataFusion's vectorized search with an
input-null guard and converts DataFusion's missing-match null to Flink's `INT` zero. This follows
the same semantic-adapter model as Comet's array-position implementation while retaining the
upstream DataFusion kernel.
`ARRAY_REMOVE` is accelerated when its array and search value otherwise lower natively and the
search value is provably non-null. It removes every matching value, preserves null elements and
input order, and returns null for a null array. Nullable search values cause whole-Calc fallback
with an EXPLAIN reason because Flink removes null elements when the search value is null, while
DataFusion's remove-all kernel returns null instead.
`ARRAY_MIN` and `ARRAY_MAX` are accelerated for arrays of `TINYINT`, `SMALLINT`, `INT`,
`BIGINT`, `DECIMAL`, `VARCHAR`, and `DATE`. Null elements are ignored; empty, all-null, and null
arrays produce null. Rust uses DataFusion's vectorized extrema kernels. `FLOAT` and `DOUBLE`
arrays stay on Flink with an EXPLAIN reason because Flink and DataFusion order `NaN` differently;
generated edge coverage includes `NaN`, infinities, and signed zero to enforce that fallback.
`ARRAY_JOIN` is accelerated for `VARCHAR` arrays when its delimiter and optional null replacement
are non-null literals. It lowers directly to DataFusion's vectorized `array_to_string` expression,
which skips null elements when no replacement is supplied and substitutes the requested text when
one is supplied. Null arrays, empty arrays, empty delimiters, and empty replacements preserve Flink
behavior. Dynamic or null delimiter/replacement arguments stay on Flink with an EXPLAIN reason
because DataFusion applies those controls once per batch rather than once per row.
`SPLIT` is accelerated for `VARCHAR` values with a nonempty, non-null literal delimiter. Rust
lowers it to DataFusion's vectorized `string_to_array` expression, preserving leading, trailing,
and consecutive empty fields as well as null input strings. Empty delimiters stay on Flink with
an EXPLAIN reason because Flink splits the input into Unicode characters while DataFusion retains
it as one element; null and dynamic delimiters also remain on Flink for now.
`ARRAY_SORT` is accelerated for arrays of `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `DECIMAL`,
`VARCHAR`, and `DATE` when its optional ascending and null-first controls are non-null boolean
literals. StreamFusion converts Flink's boolean controls to DataFusion's `ASC`/`DESC` and
`NULLS FIRST`/`NULLS LAST` options and invokes its vectorized array-sort kernel. The one-argument
form defaults to ascending with nulls first; the two-argument form puts nulls first when ascending
and last when descending, matching Flink. Floating-point arrays stay on Flink because NaN and
signed-zero ordering is not yet parity-approved; dynamic or null controls also produce an explicit
EXPLAIN fallback.
`ARRAY_SLICE` is accelerated for Arrow-compatible arrays when its start and optional inclusive
end positions are non-null integer literals. Positive, zero, negative, and out-of-range positions
use DataFusion's vectorized one-based slice kernel, whose clamping and negative-from-end behavior
matches Flink; the omitted end is represented natively as an unbounded upper position. Dynamic or
null positions remain on Flink with an explicit EXPLAIN reason.
`ELEMENT(array)` currently stays on Flink. A singleton array returns its value and an empty or null
array returns null, but Flink raises a runtime error when the array contains more than one element.
DataFusion's indexed access does not enforce that cardinality contract, so StreamFusion reports the
semantic mismatch in EXPLAIN instead of approximating it. A future native implementation requires a
dedicated checked expression that preserves the same runtime failure.
`ARRAY_DISTINCT` is accelerated for Arrow-compatible element types, including nested `ROW`
elements. It preserves the first occurrence order, retains at most one null element, and
preserves null and empty arrays. Rust lowers it directly to DataFusion's vectorized set kernel;
generated parity cases cover duplicate primitive values and duplicate nested rows so complex
element identity is verified rather than inferred.
`ARRAY_UNION` is accelerated for compatible native array inputs, including direct arrays,
typed null arrays, `ARRAY_REVERSE`, and arrays of nested rows. It preserves first-occurrence
order across the left array followed by the right, emits at most one null element, and returns
null if either input array is null. Rust lowers it to DataFusion's vectorized set kernel.
Planner-coerced compositions whose operand type Calcite cannot currently expose—such as an
`ARRAY_APPEND` directly nested inside a union—still cause whole-Calc fallback instead of
guessing the coerced element type.
`ARRAY_INTERSECT` is accelerated for the same compatible array shapes, including nested rows.
It returns each value present in both inputs at most once in left-input order, treats null as a
set value, and returns null if either array is null. The implementation directly uses
DataFusion's vectorized intersection kernel and keeps the result inside the native plan.
Non-empty `ARRAY[...]` value constructors are accelerated when every element is an otherwise
supported expression of Flink's resolved common element type. This includes arrays containing
dynamic scalar values, arrays, or rows. Java serializes each element independently and Rust
lowers the constructor to DataFusion's vectorized `make_array`; no materialized collection is
sent through protobuf. Empty constructors remain on Flink with an EXPLAIN reason because
DataFusion's untyped `List<Null>` result cannot preserve Flink's declared element type.
Non-empty typed `ROW(...)` constructors are accelerated when every field expression lowers
natively. Java serializes Flink's resolved field names and expressions in the plan protobuf; Rust
interleaves those names with the native field expressions and lowers the constructor to
DataFusion's `named_struct`. Primitive, nullable, and nested native fields therefore remain Arrow
children of one struct array without JVM materialization. Empty rows stay on Flink because
DataFusion named structs require at least one field.
Non-empty typed `MAP[...]` constructors are accelerated when every key is a unique, non-null
literal and every value expression lowers natively. Java separates the resolved key/value pairs in
the protobuf; Rust builds Arrow key and value lists and passes them directly to DataFusion's map
constructor. Dynamic, null, or duplicate keys remain on Flink with an EXPLAIN reason because Flink
uses last-value-wins semantics for duplicate keys while DataFusion rejects them. Empty maps remain
fallback until the protobuf carries an explicit Arrow key/value type for an empty constructor.
`ARRAY_EXCEPT` is accelerated for compatible array inputs, including arrays of nested rows and
native `ARRAY[...]` constructors. It preserves the first occurrence of each left-side value not
present on the right, treats null as a comparable set value, removes duplicates, and returns null
if either array is null. Rust lowers it directly to DataFusion's vectorized difference kernel.

Rust lowers `COALESCE` to DataFusion's vectorized `CaseExpr`: each argument except the last
becomes an `IS NOT NULL` branch and the last argument is the fallback value.
`CASE` and `IF` use the same DataFusion expression with explicit ordered condition/result
branches and an `ELSE` expression.
`ABS` is accelerated for supported numeric operands. Floating-point and decimal inputs use
DataFusion's vectorized math function. Signed integers use a vectorized DataFusion `CASE`
around StreamFusion's wrapping unary minus so Flink's minimum-value behavior is preserved.
Parity coverage includes signed integer minima, nulls, infinities, NaN, and signed zero.
`CEIL` and `FLOOR` are accelerated for signed integers, `FLOAT`, and `DOUBLE`. Integer
inputs are already integral and remain zero-copy references;
floating-point inputs use DataFusion's vectorized math functions. Decimal and temporal forms
currently cause whole-Calc fallback because their result-type and calendar semantics need
separate parity work.
`SIGN` is accelerated for `INTEGER`, `BIGINT`, floating-point values, and decimals. Integer
and decimal inputs lower to ordered DataFusion comparisons and a `CASE`. Floating-point
inputs first preserve either signed zero through a `CASE`, then use DataFusion `signum` for
nonzero values so NaN is retained. `TINYINT` and `SMALLINT` remain fallback because Flink 2.3
currently generates uncompilable Java for those calls, preventing a byte-parity contract.
`CHAR_LENGTH` and `CHARACTER_LENGTH` are accelerated for `VARCHAR` operands. DataFusion's
Unicode kernel counts UTF-8 code points, matching Flink for ASCII, multibyte text, emoji,
combining marks, embedded NUL characters, empty strings, and nulls. `CHAR` operands remain
fallback until fixed-width padding semantics have dedicated coverage.
`LOWER` and `UPPER` are accelerated for `VARCHAR` under locale-independent JVM case-mapping
locales. They use DataFusion's vectorized Unicode kernels and preserve empty strings and
nulls. StreamFusion falls back for Turkish, Azerbaijani, and Lithuanian JVM locales, where
Flink's default-locale Java mapping can differ from locale-independent Unicode mapping, and
for fixed-width `CHAR` until padding behavior has dedicated coverage.
`CONCAT` is accelerated for two or more `VARCHAR` arguments. A DataFusion `CASE` first checks
that every argument is non-null before invoking the vectorized concat kernel, preserving
Flink's null-if-any-argument-is-null rule (DataFusion concat alone skips nulls). Character
literals and nested supported projections may be arguments; binary and fixed-width character
concatenation remain fallback pending their distinct type-width rules.
`SUBSTRING` and `SUBSTR` are accelerated for `VARCHAR` with a positive literal start and an
optional nonnegative literal length. Positions are one-based and count Unicode code points;
zero length, starts beyond the value, empty strings, and nulls preserve Flink behavior.
Dynamic indices, zero or negative starts, negative lengths, binary strings, and `CHAR` fall
back because Flink's negative-position rule differs from DataFusion's PostgreSQL semantics.

Cast approval is table-driven. Java maps an explicitly approved Flink source/target pair
to a stable protobuf cast kind; Rust independently verifies that kind against the actual
Arrow source type and declared target before creating a DataFusion `CastExpr`. Adding a
cast family therefore extends one compatibility matrix and its generated parity cases,
while semantic exceptions remain isolated.

See the [Flink SELECT-clause documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/select/#select-clause).

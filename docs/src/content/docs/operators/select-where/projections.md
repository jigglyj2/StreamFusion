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

`TYPEOF(value [, force_serializable])` is accelerated for every otherwise supported operand type.
Like Flink's own specialized function, the Java planner derives the summary or serializable type
name from Flink's `LogicalType` and sends that value as a native string literal, repeated once per
input row. Nullable scalar, decimal, array, and row types have parity coverage; unsupported Flink
types still cause whole-Calc fallback rather than using a DataFusion type name.
`IFNULL(value, replacement)` is accelerated by lowering Flink's distinct Rex call to the same
versioned protobuf expression as `COALESCE`. Both arguments must resolve to a parity-approved
common type; integer, widened `BIGINT`, string, null, projection, and filter behavior have generated
byte-parity coverage.
`INET_NTOA(integer)` is accelerated for `TINYINT`, `SMALLINT`, `INT`, and `BIGINT`. Rust widens the
Arrow integer without loss and formats values in Flink's unsigned IPv4 range `[0, 4294967295]` as
dotted decimal; null, negative, and out-of-range values return null. All integer widths, both range
boundaries, invalid values, projections, and filters have byte-parity coverage.
`INET_ATON(varchar)` is accelerated with Flink's MySQL-compatible decimal parser, including the
`a`, `a.b`, `a.b.c`, and `a.b.c.d` forms and decimal leading zeros. Empty components, non-digits,
whitespace, extra components, and values above 255 return null. Standard, abbreviated, boundary,
invalid, nested `INET_NTOA(INET_ATON(...))`, projection, and filter cases have parity coverage.
`GREATEST` and `LEAST` are accelerated when their common type is `TINYINT`, `SMALLINT`, `INT`,
`BIGINT`, `DECIMAL`, `VARCHAR`, `DATE`, `TIME`, or timezone-free `TIMESTAMP` at precision 0 through
6. DataFusion performs the
vectorized extremum comparison, wrapped in a native
null guard so any null argument produces null as Flink requires. All widths, negative values, three
arguments, nulls, projections, and filters have parity coverage. Floating-point, fixed-width
`CHAR`, `TIMESTAMP(7..9)`, and `TIMESTAMP_LTZ` overloads remain on Flink with an EXPLAIN reason until
their NaN, signed-zero, padding, range, and session-zone rules are independently proven. Date ordering compares
Arrow's signed
epoch-day representation and covers pre-epoch dates, leap days, the supported range, nulls,
projections, and filters.
Time and timestamp ordering uses the precision-specific Arrow Time32, Time64, and Timestamp arrays
without timezone conversion. Precisions 0/3 for Flink `TIME` and 0/3/6 for `TIMESTAMP`, values
across the Unix epoch, fractional seconds, the supported timestamp range, nulls, and filters have
parity coverage. Flink timestamps at precision 7 through 9 use Arrow nanoseconds, whose physical
range is narrower than Flink's calendar range; those Calcs fall back before boundary conversion.
Decimal extrema retain Flink's resolved common precision and scale in Arrow `Decimal128` arrays.
Mixed-scale coercion, precision 38, negative values, nulls, projections, and filters have parity
coverage.
Floating extrema stay on Flink because its boxed comparison order places NaN above every numeric
value while DataFusion's extrema kernels place it below numeric values. Runtime-generated NaN and
infinity coverage verifies this whole-Calc fallback.
`VARCHAR` extrema use a dedicated vector expression that compares UTF-16 code units exactly as
Flink's Java runtime does. This intentionally differs from ordinary UTF-8 byte ordering for some
supplementary Unicode characters. ASCII, composed and decomposed Unicode, supplementary/BMP order,
empty values, nulls, projections, and filters have parity coverage. Fixed-width `CHAR` remains on
Flink pending padding-specific parity work.

StreamFusion can select, reorder, omit, or repeat direct input columns of these types:

- `BOOLEAN`, `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `FLOAT`, and `DOUBLE`
- `CHAR`, `VARCHAR`, `BINARY`, and `VARBINARY`
- `DECIMAL`, `DATE`, `TIME`, `TIMESTAMP(0..6)`, and `TIMESTAMP_LTZ(0..6)`
- `ARRAY`, `MAP`, and `ROW`, recursively containing Arrow-compatible types

Complex types may be selected, reordered, omitted, or repeated as direct input references.
Named `ROW` fields may also be projected or used inside supported expressions, including
chains through nested rows. A null parent row produces a null child value, matching Flink.
Typed `NULL` literals are accelerated for `ARRAY`, `MAP`, and `ROW`, including recursively
nested arrays and rows. One-based `ARRAY` access is accelerated for literal and computed `INT`
indexes; a null array, null index, null element, nonpositive index, or index beyond the array
length produces null. Flink rejects a nonpositive literal during validation, while the native
runtime adapter preserves its per-row behavior for a computed nonpositive value. The
selected element may itself be complex, so expressions such as `rows[1].name` are supported.
Map lookup is accelerated when both the map and key are otherwise supported expressions of
the declared map types; present values, present null values, absent keys, and null maps match
Flink's scalar result. `CARDINALITY` is accelerated for maps and arrays of any supported nesting
depth, returning an `INT` count or null for a null collection. A dedicated native expression counts
only the outer array, matching Flink rather than DataFusion's recursive leaf count. Non-null `MAP` and `ROW` literals,
`MULTISET`, and collection functions not explicitly listed below still fall back with the whole Calc. Nested child types, field names,
ordering, nullability, and Arrow offsets are preserved across the native plan.

Precision, scale, fixed width, and nullability are preserved. Timestamp precision 7 through 9
falls back because Arrow nanosecond timestamps cannot represent Flink's complete calendar domain.
Constant `TINYINT` and
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
Finite `FLOAT` arithmetic is also accelerated when Flink's inferred result remains `FLOAT`.
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
The operand may be any recursively supported native expression, not only an input column;
the cast remains a distinct protobuf/DataFusion expression after its child is evaluated.
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
expressions that require a converting planner-inserted cast still fall back with the whole Calc.
Planner-inserted casts that leave the complete logical type unchanged, including width,
precision, and scale, are removed on the Java side and the enclosed expression is serialized
normally. This covers the redundant typed-literal casts Flink introduces around some `VALUES`
branches without delegating any conversion semantics to DataFusion.
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
`NULLIF(left, right)` is accelerated for otherwise supported comparable types after Flink rewrites
it to its canonical conditional form. StreamFusion serializes that comparison and ordered result
selection through the existing protobuf expressions, preserving three-valued null behavior without
adding a duplicate native operator.
Unary `+` is accelerated for every otherwise supported numeric expression by eliminating the
identity node on the Java planner side. The operand remains an independently encoded native
expression, so this does not introduce a redundant DataFusion kernel or batch copy.
`TRY_CAST` is accelerated when it preserves the complete type or performs a lossless widening from
`TINYINT` through `BIGINT`. These conversions cannot take the failure branch, so they reuse the
parity-approved native cast expressions. Parsing, narrowing, floating, temporal, and nested
conversions stay on Flink because their exact null-on-failure boundary is not yet proven; the
restriction appears in `EXPLAIN`.

Integer division or remainder by zero or a non-literal divisor, decimal division and
remainder, floating-point remainder, non-decimal mixed-width arithmetic, non-finite
floating-point literals, arithmetic on other types, converting casts, and unlisted functions currently
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
Array and map `CARDINALITY` use a dedicated vector expression over Arrow offsets. It reads only the
outer offset pair for each row, so nested arrays match Flink's outer-cardinality semantics without
walking or materializing child values. Arrow list and map offsets cap a single row's result within
the signed 32-bit range.
`ARRAY_CONTAINS` is accelerated in projections and filters when both arguments otherwise lower
natively. Null arrays yield null, and null elements do not prevent a non-null needle from matching
another element. For a null needle, a dedicated Arrow expression searches the element-validity
range and returns true exactly when the array contains a null, preserving Flink's behavior where
DataFusion's stock kernel would return null.
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
`ARRAY_REMOVE` is accelerated when its array and search value otherwise lower natively. It removes
every matching value, preserves input order, and returns null for a null array. Non-null needles
use DataFusion's vectorized remove-all kernel. For a null needle, a dedicated Arrow expression
copies only the non-null child ranges into a new list, matching Flink instead of DataFusion's null
result.
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
end positions are otherwise supported `INT` expressions. Positive, zero, negative, and out-of-range
positions use DataFusion's vectorized one-based slice kernel, with an Arrow-native adapter for
Flink's explicit end-zero rule. Null bounds produce null, and an omitted end is represented
natively as an unbounded upper position.
`ELEMENT(array)` is accelerated for otherwise supported arrays. A singleton array returns its value,
an empty or null array returns null, and an array with more than one element raises an execution
error. StreamFusion uses a dedicated vector expression rather than unchecked indexed access so
Flink's cardinality contract is preserved for scalar and nested element types.
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
sent through protobuf. Flink 2.3 rejects empty `ARRAY[]` constructors during SQL validation before
a physical plan or inferred element type exists, so there is no native operator case to replace.
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
uses last-value-wins semantics for duplicate keys while DataFusion rejects them. Flink 2.3 rejects
empty `MAP[]` constructors during SQL validation before a physical plan or inferred key/value type
exists, so there is no native operator case to replace.
`MAP_KEYS` and `MAP_VALUES` are accelerated for otherwise native map expressions. They lower
directly to DataFusion's Arrow map projection kernels and preserve entry order, null maps, and
nullable values. They can consume a native `MAP[...]` constructor without materializing the map in
Java or crossing an intermediate JVM boundary.
`MAP_ENTRIES` is accelerated under the same conditions and returns an Arrow list of structs named
`key` and `value`, matching Flink's `ARRAY<ROW<key, value>>` shape and preserving entry order and
value nullability. It also composes directly with a native map constructor.
`MAP_FROM_ARRAYS(keys, values)` stays on Flink 2.3. Some generated consumers cast Flink's internal
`MapDataForMapFromArrays` result to `GenericMapData` and fail at runtime. A native implementation
would make those same jobs succeed, violating observable failure parity, so EXPLAIN identifies the
version-specific runtime contract. This can be revisited after the supported Flink line fixes the
consumer representation.
`MAP_UNION(map, ...)` also stays on Flink 2.3. Its private `MapDataForMapUnion` representation uses
rightmost-map precedence, permits null keys, and can be observed by representation-sensitive
generated consumers. Arrow maps require non-null keys, so StreamFusion reports this incompatibility
instead of dropping keys or changing which Flink jobs fail.
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
`SQRT` is accelerated for numeric operands after Flink coerces them to `DOUBLE`. Flink lowers the
call to `POWER(value, 0.5)`; StreamFusion preserves that shape in protobuf and uses DataFusion's
vectorized power kernel. This retains Flink's NaN result for negative inputs, signed-zero behavior,
infinities, and null propagation. General `POWER` remains on Flink because DataFusion rejects zero
raised to a negative exponent while Flink returns a signed infinity.
`EXP` is accelerated for numeric operands after Flink coerces them to `DOUBLE`. It lowers to
DataFusion's vectorized exponential kernel, including byte-parity coverage around underflow and
overflow as well as signed zero, infinities, NaN, and nulls.
`PI()` and `E()` are accelerated as exact `DOUBLE` constants. The Java planner records the same
IEEE-754 values used by Flink directly in protobuf, and native Calc treats them as DataFusion
literals that can compose with other projections and predicates without a per-row function call.
`UUID()`, `RAND([seed])`, and `RAND_INTEGER([seed,] bound)` remain on Flink. UUID values come from
the JVM's secure random generator, while Flink's random-number state advances per row and function
instance. Native vector evaluation cannot reproduce the same execution-history-dependent sequence
byte-for-byte, including for seeded calls, so `EXPLAIN` reports the lifecycle dependency instead of
claiming approximate distribution parity.
`SIN`, `COS`, and `TAN` are accelerated for numeric operands after Flink coerces them to `DOUBLE`.
Each remains a distinct protobuf expression and DataFusion vectorized operator; parity coverage
includes signed zero, infinities, NaN, and nulls.
`COT` is accelerated as a distinct DataFusion vector expression after the same `DOUBLE` coercion.
Parity coverage includes signed zero, multiples of pi, infinities, NaN, and nulls.
`LN` and `LOG10` are accelerated as separate DataFusion vector expressions after Flink coerces the
operand to `DOUBLE`. They preserve Flink's IEEE-754 domain behavior: negative inputs produce NaN,
positive and negative zero produce negative infinity, positive infinity remains infinite, and null
propagates. Generated parity tests include subnormal and maximum finite inputs and composition in
comparison filters.
Unary `LOG(value)` is accelerated with Flink's natural-logarithm semantics rather than DataFusion's
single-argument base-10 convention. `LOG(base, value)` preserves Flink's argument order in protobuf
and lowers to DataFusion's arbitrary-base vector expression. Invalid bases and values retain
IEEE-754 infinity and NaN behavior. `LOG2` stays on Flink with an explicit EXPLAIN reason because
DataFusion differs from Flink by one ULP for finite inputs such as `10.0`.
`POWER(base, exponent)` is accelerated when Flink assigns a `DOUBLE` result and the exponent is a
provably nonnegative `DOUBLE` literal. The special exponent `0.5` continues to lower to the
parity-approved square-root expression; other approved exponents use DataFusion's vectorized power
kernel. Negative and dynamic exponents stay on Flink with an EXPLAIN reason because DataFusion
raises an error for zero to a negative power while Flink returns IEEE infinity.
`ROUND` remains on Flink with an explicit EXPLAIN reason. Flink's floating implementation throws
for non-finite values while DataFusion returns a value, so native acceleration would otherwise make
job success depend on which engine planned the same data. Integer and decimal rounding remain
deferred until they have separate typed parity contracts.
`TRUNCATE(value [, scale])` is accelerated for `INTEGER` and `BIGINT`, including a dynamic integer
scale. The dedicated native expression truncates toward zero at the requested decimal position,
preserves the original integer width, returns the input unchanged for nonnegative scales, and
returns zero when a negative scale is wider than the type. Narrow integers remain on Flink because
Flink 2.3's generated implementation has incompatible return assignments; accelerating them would
hide an observable Flink failure. Decimal and floating-point overloads remain on Flink pending
exact `DecimalData` conversion and non-finite error parity. `EXPLAIN` reports these restrictions.
`SINH` and `TANH` are accelerated under the same `DOUBLE` coercion contract and remain distinct
native stages. Generated parity coverage includes finite values, overflow, signed zero, infinities,
NaN, and nulls. `COSH` stays on Flink with an explicit EXPLAIN reason because DataFusion's kernel
differs from Flink by one ULP for finite inputs; approximate equality is not sufficient for
StreamFusion's byte-parity contract.
`ASIN`, `ACOS`, and `ATAN` are accelerated as distinct DataFusion vector expressions after Flink's
`DOUBLE` coercion. Tests include values outside the `ASIN`/`ACOS` domain to verify Flink-compatible
NaN results, along with endpoints, signed zero, infinities, and nulls.
`ATAN2(y, x)` is accelerated as a two-input DataFusion vector expression with Flink's argument order
preserved. Generated parity coverage exercises every quadrant, signed zero, infinities, NaN, and
null propagation.
`DEGREES` and `RADIANS` are accelerated through DataFusion's vectorized angle-conversion kernels.
They accept Flink-coerced `DOUBLE` inputs and preserve signed zero, infinities, NaN, and nulls.
Parity-approved scalar expressions can participate in numeric comparison filters when Calcite keeps
an integral literal beside a `DOUBLE` expression; StreamFusion widens the literal to `DOUBLE` in the
protobuf tree. Native floating comparisons explicitly mask NaN for equality and ordered operators,
preserving Flink/Java comparison semantics rather than DataFusion's total NaN ordering.
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
Timezone-free `YEAR`, `QUARTER`, `MONTH`, `WEEK`, `DAYOFMONTH`, `DAYOFYEAR`, `DAYOFWEEK`, and their
equivalent `EXTRACT` forms are accelerated for `DATE`; explicit `ISOYEAR` and `ISODOW` extraction
is supported too. Flink canonicalizes the convenience functions to one
`EXTRACT` expression; the Java planner records the calendar field and Calcite result width in
protobuf, and Rust lowers it to DataFusion's Arrow temporal kernel. ISO week boundaries, dates
before the Unix epoch, leap days, nulls, and the supported Flink date range have parity coverage.
`EXTRACT(EPOCH FROM date)` is also accelerated and returns Flink's signed whole-second offset from
the Unix epoch. Pre-epoch dates, the epoch boundary, nulls, projections, and filters have parity
coverage.
Timestamp, local-time-zone, and additional calendar fields remain on Flink
until their session-zone and precision contracts are separately proven.
`CURRENT_DATE`, `CURRENT_TIME`, `LOCALTIME`, `CURRENT_TIMESTAMP`, `NOW`,
`CURRENT_ROW_TIMESTAMP`, and `LOCALTIMESTAMP` stay on Flink because their values are bound to
Flink's job, per-row, and configured session-clock lifecycle. StreamFusion does not independently
sample a native clock and risk different values within one logical query.
`DATE_FORMAT`, `FROM_UNIXTIME`, `UNIX_TIMESTAMP`, `TO_DATE`, `TO_TIMESTAMP`,
`TO_TIMESTAMP_LTZ`, and `CONVERT_TZ` stay on Flink pending exact Java pattern, locale, invalid-input,
DST gap/overlap, session-zone, and precision parity. `TIMESTAMPDIFF`, temporal overlap, and `AT`
likewise stay on Flink until their calendar-unit truncation, interval, overflow, and zone contracts
are proven. EXPLAIN distinguishes these runtime-context restrictions from an unknown expression.
`HOUR(time)`, `MINUTE(time)`, `SECOND(time)`, `EXTRACT(MILLISECOND FROM time)`, and their applicable
`EXTRACT` forms are accelerated for timezone-free `TIME`. The same protobuf expression lowers to
Arrow's temporal kernel over the corresponding Time32 vector. Midnight, end-of-day and fractional
values, Flink's supported precisions 0/3, nulls, projections, and filters have parity coverage.
Microsecond/nanosecond fields, timestamp, and local-time-zone extraction remain on Flink.
Their EXPLAIN fallback identifies the unresolved session-zone and subsecond precision contract.
Interval extraction likewise remains on Flink pending signed-field decomposition parity, while
`MILLENNIUM`, `CENTURY`, and `DECADE` date extraction remains there pending BCE and year-zero
calendar parity. StreamFusion reports those reasons rather than a generic unsupported expression.
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
`CONCAT_WS(separator, value, ...)` is accelerated for a supported `VARCHAR` separator and one
or more supported `VARCHAR` values. The separator and values may be computed expressions.
DataFusion's vectorized kernel matches Flink by returning null for a null separator, skipping
null values without adding their separators, retaining empty strings, and returning an empty
string when every value is null. Fixed-width `CHAR` columns remain on Flink pending padding
coverage; character literals are accepted as variable-width arguments.
`TRIM`, `BTRIM`, `LTRIM`, and `RTRIM` are accelerated for `VARCHAR` values. Standard SQL `LEADING`,
`TRAILING`, and `BOTH` directions and Flink's optional trim-character expressions are preserved
as an explicit protobuf direction plus native child expressions. The one-argument forms remove
only ASCII spaces, matching Flink rather than treating tabs or other Unicode whitespace as spaces.
Custom character sets may be literals or computed `VARCHAR` values; null values or character sets
produce null. A `CHAR` literal is accepted as a trim-character set, while fixed-width `CHAR` input
values and dynamic `CHAR` character sets remain on Flink pending padding-specific parity coverage.
`SUBSTRING` and `SUBSTR` are accelerated for `VARCHAR` with a positive literal start and an
optional nonnegative literal length. Positions are one-based and count Unicode code points;
zero length, starts beyond the value, empty strings, and nulls preserve Flink behavior.
Dynamic indices, zero or negative starts, negative lengths, binary strings, and `CHAR` fall
back because Flink's negative-position rule differs from DataFusion's PostgreSQL semantics.
`REPLACE(value, search, replacement)` is accelerated when all three arguments are supported
`VARCHAR` expressions. It uses DataFusion's vectorized replacement kernel and supports literal
or computed search and replacement strings, repeated matches, Unicode text, empty strings, and
null propagation. Binary strings and fixed-width `CHAR` remain on Flink pending their distinct
type semantics.
`TRANSLATE(value, source_characters, target_characters)` is accelerated for supported `VARCHAR`
expressions. It uses DataFusion's vectorized Unicode code-point mapping, including Flink's
first-mapping-wins rule for duplicate source characters and deletion when the target alphabet is
shorter. StreamFusion converts a null target alphabet to empty before execution because Flink
deletes matched characters in that case; null values or source alphabets retain Flink's result.
Fixed-width `CHAR` columns remain on Flink pending padding coverage.
`ELT(index, value, ...)` is accelerated for `VARCHAR` values and `INTEGER` indices. A native
DataFusion `CASE` selects the one-based value and returns
typed null for a null, nonpositive, or out-of-range index. Both indices and values may be computed
expressions. Other signed index widths stay on Flink because Flink 2.3 itself throws
`ClassCastException` for valid boxed `TINYINT`, `SMALLINT`, and `BIGINT` selections; StreamFusion
does not silently introduce behavior where baseline Flink cannot produce a result. Binary values
and fixed-width `CHAR` columns remain on Flink pending their distinct coercion and padding coverage.
`SPLIT_INDEX(value, delimiter, index)` is accelerated for a supported `VARCHAR` value, a nonempty
literal delimiter, and a supported `INTEGER` index expression. StreamFusion lowers it to
DataFusion `string_to_array` plus dynamic array extraction, preserving Flink's zero-based index,
empty tokens, and null result for negative, out-of-range, empty-input, or null arguments. Empty
and dynamic delimiters stay on Flink because an empty delimiter activates Java whitespace
splitting, which is not equivalent to DataFusion's empty-delimiter behavior.
`BIN(value)` is accelerated for `TINYINT`, `SMALLINT`, `INTEGER`, and `BIGINT` expressions. A
dedicated native vector expression widens each value to a signed 64-bit integer and emits the same
un-padded two's-complement representation as Flink, including 64 digits for negative values and
`0` for zero. Null values remain null.
`UNHEX(value)` is accelerated for `VARCHAR` expressions and returns Arrow binary data. Its
dedicated native vector expression accepts upper- or lowercase ASCII hexadecimal digits, returns
null for invalid input, and preserves Flink's unusual odd-length rule: the leading digit is
validated but represented by a zero byte. Empty input produces an empty byte array and null input
remains null. Fixed-width `CHAR` values remain on Flink pending padding parity coverage.
`URL_ENCODE(value)` is accelerated for `VARCHAR` expressions using a dedicated vectorized form
encoder. It matches Java's `application/x-www-form-urlencoded` rules used by Flink: spaces become
`+`, ASCII letters, digits, `.`, `-`, `*`, and `_` remain literal, and every other UTF-8 byte uses
uppercase percent encoding. Empty strings and nulls preserve Flink behavior. Fixed-width `CHAR`
values remain on Flink pending padding parity coverage.
`URL_DECODE(value)` is accelerated for `VARCHAR` expressions. Its dedicated native form decoder
maps `+` to space, accepts case-insensitive `%HH` escapes, decodes each consecutive escape run as
UTF-8 with Java-compatible replacement for malformed byte sequences, and returns null for an
incomplete or non-hex escape. Literal Unicode remains outside adjacent escape-run decoding, as in
Java's `URLDecoder`. Empty strings and nulls preserve Flink behavior; fixed-width `CHAR` remains on
Flink pending padding parity coverage.
`PARSE_URL(value, part [, key])` remains on Flink because its accepted URL grammar, normalization,
component extraction, and malformed-input behavior come from `java.net.URL`. Native URL parsers
make observably different decisions for valid edge cases, so `EXPLAIN` records the JVM parser
dependency rather than approximating it.
`ENCODE(value, charset)` and `DECODE(value, charset)` remain on Flink. Their charset aliases,
installed providers, malformed-input replacement, and unsupported-charset failures are defined by
the running JVM. StreamFusion reports that environment-dependent contract in `EXPLAIN` until a
native implementation is proven against the complete Java `Charset` behavior, rather than
supporting only a convenient list of encodings.
`PRINTF(format, value, ...)` stays on Flink because `java.util.Formatter` defines its locale,
argument conversions, numeric rounding, and invalid-format exceptions. A similar Rust formatter is
not a byte-parity implementation, so EXPLAIN records the JVM dependency.
`JSON_QUOTE(value)` is accelerated for `VARCHAR` expressions by a dedicated native vector
expression. It wraps the value in quotes, escapes Flink's ASCII quote, backslash, slash, and control
characters, and emits lowercase `\\u` escapes for every non-ASCII UTF-16 code unit. StreamFusion
intentionally preserves Flink 2.3's supplementary-character quirk: the high-surrogate position
emits the full code point and the low surrogate is then escaped again. Nulls remain null;
fixed-width `CHAR` stays on Flink pending padding parity coverage.
`JSON_UNQUOTE` remains on Flink. It validates the complete input with Flink's shaded Jackson
configuration, unescapes only valid quoted JSON strings, and returns invalid JSON unchanged. No
native validator has yet been proven to accept and reject exactly the same edge cases, so
StreamFusion reports that dependency in `EXPLAIN` rather than approximating pass-through behavior.
The remaining SQL/JSON scalar functions—including `IS JSON`, `JSON_EXISTS`, `JSON_VALUE`,
`JSON_QUERY`, `JSON_STRING`, `JSON_OBJECT`, `JSON_ARRAY`, `JSON`, `PARSE_JSON`, and
`TRY_PARSE_JSON`—stay on Flink. Their path modes, wrapper clauses, `ON EMPTY`/`ON ERROR` branches,
number representation, and JSON logical-type serialization need one exact contract before native
execution can replace them.
The scalar `BITMAP_*` family stays on Flink. Flink's specialized `BITMAP` logical type, Java bitmap
serialization bytes, signed integer domain, null behavior, and byte round trips through
`BITMAP_TO_BYTES`/`BITMAP_FROM_BYTES` must be proven against one native bitmap implementation before
any member can cross the Arrow boundary. Aggregate bitmap functions are additionally stateful and
outside Calc acceleration.
`OBJECT_OF` and `OBJECT_UPDATE` stay on Flink. These functions create class-backed structured
values whose classloader identity, class name, field order, constructors, and JVM object
materialization are part of the contract. StreamFusion does not substitute a superficially similar
Arrow struct until both the native plan and RowData boundary can preserve that identity exactly.
`HASH_CODE(value)` is accelerated for `VARCHAR` expressions. A dedicated native vector expression
reproduces Java `String.hashCode()` over UTF-16 code units with wrapping 32-bit arithmetic, then
applies Flink's overflow-preserving absolute value. This includes supplementary Unicode and the
`Integer.MIN_VALUE` hash edge case. Nulls remain null; fixed-width `CHAR` and other internal
type-specific hash overloads remain on Flink.
`IS_ALPHA` and `IS_DIGIT` remain on Flink because their Commons Lang implementations classify
individual UTF-16 code units with the JVM's `Character` tables. Native Unicode scalar predicates
disagree for supplementary characters and may use a different Unicode-data version. `IS_DECIMAL`
also remains on Flink because it delegates character input to Java's integer, long, and double
parsers, including Java-specific syntax and overflow behavior. Each function reports its precise
parity dependency in `EXPLAIN` instead of substituting a similar native predicate or parser.
`STR_TO_MAP` remains on Flink. Flink treats both separators as Java regular expressions, whereas
DataFusion's similarly named Spark function performs literal delimiter matching; using it would
produce different keys for valid Flink expressions. StreamFusion reports this distinction in
`EXPLAIN` instead of approximating the result.
`REGEXP`, `REGEXP_COUNT`, `REGEXP_EXTRACT`, `REGEXP_EXTRACT_ALL`, `REGEXP_INSTR`,
`REGEXP_SUBSTR`, `REGEXP_REPLACE`, and the `SIMILAR TO` predicates remain on Flink. They use Java
`Pattern` syntax, matching, capture, and replacement semantics, including constructs such as
look-around and backreferences that Rust/DataFusion regex deliberately does not implement.
`EXPLAIN` identifies the regex-engine dependency rather than accepting only an undocumented subset.
`REPEAT(value, count)` is accelerated for supported `VARCHAR` values and `INTEGER` counts,
including computed counts. StreamFusion widens the count inside the native plan for DataFusion's
vectorized kernel; zero and negative counts produce the empty string as in Flink, while null
values or counts produce null. Fixed-width `CHAR` remains on Flink.
`REVERSE(value)` is accelerated for supported `VARCHAR` expressions in projections and filters
using DataFusion's vectorized Unicode reverse kernel. Both engines reverse Unicode scalar values,
so supplementary characters remain intact while combining marks retain their independent code
point behavior. Generated parity coverage includes ASCII, multilingual text, supplementary
characters, combining marks, embedded NUL bytes, empty strings, and nulls.
`INITCAP(value)` is accelerated for supported `VARCHAR` expressions in projections and filters.
StreamFusion uses a dedicated vectorized expression instead of DataFusion's Unicode-aware
`initcap`: Flink defines words and case conversion using ASCII `[A-Za-z0-9]` only, with every
other character acting as a word boundary. Generated parity coverage includes mixed case, digits,
punctuation, non-ASCII boundaries, empty strings, and nulls.
`ENDSWITH(value, suffix)` is accelerated in projections and filters when both arguments are
supported `VARCHAR` expressions. Both literal and computed suffixes lower to DataFusion's
vectorized UTF-8 predicate, including empty suffixes, Unicode text, and null propagation.
The binary-string overload and fixed-width `CHAR` remain on Flink.
`POSITION(needle IN haystack)` is accelerated when both operands are supported `VARCHAR`
expressions. StreamFusion reverses Flink's syntax operands when constructing DataFusion's
vectorized `strpos(haystack, needle)` expression. Results remain one-based, missing needles
return zero, empty needles return one, Unicode positions count code points, and nulls propagate.
Fixed-width `CHAR` remains on Flink.
The two-argument forms `INSTR(haystack, needle)` and `LOCATE(needle, haystack)` use that same
native expression with planner-side argument normalization. Computed `VARCHAR` operands are
supported. Extended forms with a start position or occurrence count stay on Flink until their
forward/backward search rules have dedicated parity coverage.
`LEFT(value, count)` and `RIGHT(value, count)` are accelerated for `VARCHAR` and `INTEGER`
expressions, including computed counts and use inside filters. They count Unicode code points.
StreamFusion inserts a native count-normalization expression because DataFusion interprets a
negative count as exclusion while Flink returns the empty string; zero and negative counts are
therefore clamped to zero before the vectorized DataFusion kernel. Nulls propagate and counts
beyond the string length return the complete value. Fixed-width `CHAR` remains on Flink.
`LPAD` and `RPAD` stay on Flink with an explicit EXPLAIN reason. Flink measures and truncates
UTF-16 code units, whereas DataFusion uses Unicode code points; truncation can split a surrogate
pair and produce a value that Arrow UTF-8 cannot represent. Supporting only ASCII would make
acceleration data-dependent, so StreamFusion defers the entire operators for now.
`OCTET_LENGTH` and `BIT_LENGTH` are not StreamFusion operators because Flink 2.3 rejects their
character and binary SQL overloads during validation. StreamFusion does not expose unreachable
Calcite runtime behavior as an extension.
`ASCII(value)` is accelerated for supported `VARCHAR` expressions in projections and filters.
Flink returns its first UTF-8 byte sign-extended with Java byte semantics, rather than a Unicode
code point; StreamFusion therefore uses a focused vectorized compatibility expression instead
of DataFusion's differing `ascii` kernel. Empty strings return zero and nulls remain null.
Fixed-width `CHAR` remains on Flink.
`OVERLAY(value PLACING replacement FROM start [FOR length])` remains on Flink. Flink indexes and
slices UTF-16 code units, so a start or length can bisect a supplementary character and create an
unpaired surrogate that Arrow UTF-8 cannot represent. Since safety depends on each row's contents
and indices, StreamFusion falls back for the whole plan and reports this reason through EXPLAIN.
`CHR(code)` is accelerated for signed integer expressions in projections and filters. Flink does
not interpret `code` as a Unicode scalar: negative values produce the empty string and nonnegative
values become the Java character represented by only their low eight bits. A focused Rust
expression preserves those rules instead of using DataFusion's Unicode-oriented `chr` kernel.
Generated parity coverage includes negative values, zero, byte boundaries, wrapped values,
integer extrema, and nulls.
`HEX(value)` is accelerated for signed integer and `VARCHAR` expressions. Java records a distinct
protobuf expression; Rust widens signed integers to `INT64` before DataFusion's hexadecimal kernel
so negative values retain Flink's 64-bit two's-complement representation. Text is encoded from its
UTF-8 bytes, and both paths compose DataFusion's uppercase kernel to match Flink's output casing.
Generated parity coverage includes integer extrema, multilingual text, supplementary characters,
embedded NUL bytes, empty strings, and nulls. Other input types remain on Flink.
`TO_BASE64(value)` is accelerated for `VARCHAR`, `BINARY`, and `VARBINARY` expressions using
DataFusion's padded Base64 kernel. Generated parity coverage includes arbitrary binary bytes,
UTF-8 text, nulls, and long values immediately below and above 76 encoded characters to ensure
Flink 2.3's unwrapped output is preserved. Fixed-width `CHAR` remains on Flink pending
padding-specific parity coverage.
`FROM_BASE64(value)` remains on Flink. Flink declares a `VARCHAR` result but permits decoded bytes
that are not valid UTF-8, while Arrow UTF-8 arrays require every value to be valid. StreamFusion
therefore cannot decide safely from the plan whether the boundary is representable, and EXPLAIN
reports this invariant mismatch instead of making acceleration data-dependent.
`MD5(value)` is accelerated for `VARCHAR`, `BINARY`, and `VARBINARY` expressions. DataFusion's
vectorized digest kernel hashes the original UTF-8 or binary bytes and returns Flink-compatible
lowercase hexadecimal text. Generated parity coverage includes empty values, arbitrary binary
bytes, multilingual text, embedded NUL bytes, and null propagation. Fixed-width `CHAR` remains
on Flink pending padding-specific parity coverage.
`SHA224(value)`, `SHA256(value)`, `SHA384(value)`, and `SHA512(value)` are accelerated for supported
character expressions. A shared protobuf digest expression selects a fixed algorithm;
Rust runs the corresponding DataFusion vectorized digest kernel and converts its binary digest to
Flink-compatible lowercase hexadecimal text. `SHA1(value)` is also accelerated for `VARCHAR`;
because DataFusion does not provide SHA-1, its isolated Rust compatibility expression hashes each
UTF-8 value with RustCrypto and emits the same lowercase hexadecimal representation. Generated
projection and filter parity coverage includes empty strings, multilingual text, embedded NUL
bytes, and null propagation. `SHA2(value, bit_length)` is accelerated when `bit_length` is a
non-null literal equal to `224`, `256`, `384`, or `512`, reusing the same protobuf and DataFusion
digest paths as the named functions. Dynamic `INTEGER` lengths use an isolated vectorized Rust
expression that selects the digest per row; null values or lengths produce null and unsupported
runtime lengths fail the job as in Flink. Unsupported non-null literals remain on Flink because
Flink rejects them during operator initialization rather than row evaluation, and EXPLAIN states
that distinction.

Cast approval is table-driven. Java maps an explicitly approved Flink source/target pair
to a stable protobuf cast kind; Rust independently verifies that kind against the actual
Arrow source type and declared target before creating a DataFusion `CastExpr`. Adding a
cast family therefore extends one compatibility matrix and its generated parity cases,
while semantic exceptions remain isolated.

See the [Flink SELECT-clause documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/select/#select-clause).

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

Complex types are currently accelerated as direct input references: they may be selected,
reordered, omitted, or repeated, but their contents cannot yet be constructed, accessed, or
transformed natively. Nested field access, collection element access, complex literals,
`MULTISET`, and collection functions fall back with the whole Calc. Nested child types,
field names, ordering, nullability, and Arrow offsets are preserved across the native plan.

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
`TIMESTAMP_LTZ`. The protobuf carries the declared type, including `CHAR(n)` and `BINARY(n)`
width, so
DataFusion materializes a correctly typed all-null Arrow vector rather than an untyped
Arrow `Null` vector.
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

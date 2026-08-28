---
title: Projections
description: Native SELECT projection coverage and fallback rules.
sidebar:
  order: 1
---

**Current status:** Input-reference projections are accelerated.

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
plan. Rust maps them to DataFusion `Column`, `Literal`, and `BinaryExpr` expressions in a
`ProjectionExec`. DataFusion shares referenced Arrow buffers for direct projections and
allocates a result vector when arithmetic produces new values.

Rust lowers `COALESCE` to DataFusion's vectorized `CaseExpr`: each argument except the last
becomes an `IS NOT NULL` branch and the last argument is the fallback value.
`CASE` and `IF` use the same DataFusion expression with explicit ordered condition/result
branches and an `ELSE` expression.
`ABS` is accelerated for supported numeric operands. Floating-point and decimal inputs use
DataFusion's vectorized math function. Signed integers use a vectorized DataFusion `CASE`
around StreamFusion's wrapping unary minus so Flink's minimum-value behavior is preserved.
Parity coverage includes signed integer minima, nulls, infinities, NaN, and signed zero.

Cast approval is table-driven. Java maps an explicitly approved Flink source/target pair
to a stable protobuf cast kind; Rust independently verifies that kind against the actual
Arrow source type and declared target before creating a DataFusion `CastExpr`. Adding a
cast family therefore extends one compatibility matrix and its generated parity cases,
while semantic exceptions remain isolated.

See the [Flink SELECT-clause documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/select/#select-clause).

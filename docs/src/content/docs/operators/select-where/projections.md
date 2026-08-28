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

Precision, scale, fixed width, and nullability are preserved. Computed `INT`, `BIGINT`,
`FLOAT`, `DOUBLE`, and `DECIMAL` projections support literals and recursively nested
addition, subtraction, and multiplication. Decimal operands retain the precision and
scale assigned to each expression by Flink's planner, including the precision growth of
intermediate results. Signed integer overflow follows Flink's wrapping semantics.
Floating-point literals must be finite. Constant `TRUE` and `FALSE` projections are also
accelerated, as are constant `DATE` values, including dates before the Unix epoch.
Unary minus is accelerated for the same numeric types.
Boolean projections can recursively compose direct boolean columns and constants with
`NOT`, `AND`, and `OR` using SQL's three-valued logic. Every comparison and null-check
shape listed on the [filter coverage page](./filters/) can also be projected as a boolean
value instead of being used by `WHERE`.
Null-safe `IS TRUE`, `IS FALSE`, `IS NOT TRUE`, and `IS NOT FALSE` expressions are
accelerated and always produce a non-null boolean result.
Null-safe `IS DISTINCT FROM` and `IS NOT DISTINCT FROM` comparison results are also
accelerated for the types supported by filters.
`INT` and `BIGINT` division is accelerated when the divisor is a direct nonzero literal;
signed results truncate exactly as Flink does. `MOD` remainder supports the same types
and divisor restriction. A planner representation that wraps a negative `BIGINT` divisor
in a cast or unary expression falls back because it is no longer a direct literal.
Constant `TIME` values preserve their declared precision and millisecond-of-day value.
Constant `TIMESTAMP WITHOUT TIME ZONE` values preserve their local calendar value and
sub-millisecond nanoseconds without applying a session or JVM timezone.
Direct constant character literals are encoded as UTF-8 and accelerated. Character
expressions that require a planner-inserted cast still fall back with the whole Calc.
Direct hexadecimal binary literals are accelerated with their exact byte sequence and
fixed width. Cast-derived and computed binary expressions remain on Flink.

Division or remainder by zero or a non-literal divisor, decimal and floating-point
division and remainder, unary plus, non-decimal mixed-width arithmetic, non-finite
floating-point literals, arithmetic on other types, casts, and functions currently fall
back to Flink. The Calc also falls back if its filter is unsupported.

## Implementation

Java recursively encodes input references, literals, and arithmetic in the protobuf
plan. Rust maps them to DataFusion `Column`, `Literal`, and `BinaryExpr` expressions in a
`ProjectionExec`. DataFusion shares referenced Arrow buffers for direct projections and
allocates a result vector when arithmetic produces new values.

See the [Flink SELECT-clause documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/select/#select-clause).

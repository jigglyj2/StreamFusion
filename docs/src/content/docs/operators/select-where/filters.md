---
title: Filters
description: Native WHERE predicate coverage and fallback rules.
sidebar:
  order: 2
---

**Current status:** Simple numeric, decimal, variable-width string and binary, date, time,
and local timestamp comparisons are accelerated.

## SQL example

```sql
SELECT name
FROM events
WHERE id >= 100;
```

## Acceleration and fallback

The supported predicates are `=`, `<>`, `<`, `<=`, `>`, and `>=` between a `TINYINT`,
`SMALLINT`, `INTEGER`, `BIGINT`, `FLOAT`, or `DOUBLE` column and a compatible literal. Either operand order is accepted,
and the filtered column does not need to appear in the projection. A null input produces
SQL unknown and is removed by `WHERE`, matching Flink. Unsupported column pairs,
functions, and other operand types currently fall back to Flink.

Floating-point literals must be finite. StreamFusion falls back for NaN or infinite
literals until their ordering and equality semantics are proven identical across Flink,
Arrow, and DataFusion. Planner-inserted casts that obscure the direct column/literal
shape also cause the whole Calc to fall back.

The same six predicates support direct, exactly matching column pairs for `TINYINT`,
`SMALLINT`, `INTEGER`, `BIGINT`, `VARCHAR`, `BINARY`, `VARBINARY`, `DECIMAL`, `DATE`,
`TIME`, and `TIMESTAMP WITHOUT TIME ZONE`. This includes exactly matching `CHAR` columns. Decimal precision
and scale and temporal precision must match on both sides. Direct `BOOLEAN` pairs support
`=` and `<>`. A null on either side produces SQL unknown. Floating-point, mismatched,
and planner-cast column pairs currently
fall back to Flink.

The same six predicates support `DATE` columns compared with `DATE` literals, including
dates before the Unix epoch. Flink epoch-day values lower directly to Arrow `Date32`
without changing units or timezone.

Direct `CHAR` and `VARCHAR` columns support all six ordered comparisons against a direct
string literal or an exactly matching column. Ordering is binary UTF-8, including for
empty and non-ASCII strings. Flink pads or truncates `CHAR(n)` values by Unicode character
count before comparison; StreamFusion accelerates a `CHAR(n)` literal only after the planner
has produced exactly `n` characters, including trailing spaces. Unnormalized literals and
comparisons between different character types fall back.

Direct `BINARY` and `VARBINARY` columns support all six ordered comparisons against a
direct binary literal or an exactly matching column. Bytes compare lexicographically as
unsigned values, and shorter equal-prefix values sort first. `BINARY(n)` is accelerated
only when both column operands have the same width or the planner has already padded or
truncated the literal to exactly `n` bytes; other shapes fall back.

`TIME` comparisons preserve Flink's millisecond-of-day representation. The protobuf also
carries the declared precision so Rust creates the matching Arrow `Time32` or `Time64`
literal and scales milliseconds to seconds, microseconds, or nanoseconds exactly as the
boundary writer does. `TIME WITH TIME ZONE` is not a Flink SQL type and is not supported.

`TIMESTAMP WITHOUT TIME ZONE` comparisons preserve Flink's local calendar value without
applying a session or JVM timezone. Milliseconds and the remaining nanoseconds travel
separately in the plan protobuf, then Rust constructs a matching Arrow timestamp scalar
at the declared precision. `TIMESTAMP_LTZ` remains on Flink pending separate timezone and
daylight-saving parity coverage.

Exact `DECIMAL` comparisons support Flink precision up to 38 with a same-scale literal.
The plan carries the signed unscaled integer, precision, and scale; Rust validates these
before constructing a DataFusion `Decimal128` scalar. Comparisons that require planner
rescaling or exceed Flink's decimal range fall back rather than rounding.

`IS NULL` and `IS NOT NULL` are supported over direct input columns of every scalar type
listed on the [projection coverage page](../projections/).
For a direct boolean input, `IS UNKNOWN` is accelerated through the same null-check path.
`IS NOT UNKNOWN` is not yet claimed because Flink normalizes that form differently before
physical planning.

Supported comparisons and null checks can be recursively composed with `AND`, `OR`, and
`NOT`. StreamFusion preserves SQL's three-valued boolean logic: true, false, and unknown
remain distinct throughout the expression, and `WHERE` retains only true rows.

`BETWEEN`, `NOT BETWEEN`, `IN`, and `NOT IN` are accelerated for direct signed-integer,
`DECIMAL`, `VARCHAR`, `DATE`, `TIME`, or `TIMESTAMP WITHOUT TIME ZONE` columns and non-null same-type literals.
Decimal endpoints must retain the
column's precision and scale. Flink represents these predicates as search-argument ranges;
StreamFusion expands each bounded or unbounded range into the existing comparison tree.
Lists containing `NULL` and search arguments over other types currently fall back.
Bounded `VARBINARY BETWEEN` ranges are also accelerated. `VARBINARY IN` and `NOT IN`
point searches fall back because Flink's binary-literal coercion is not raw byte equality.

A direct `BOOLEAN` column is also a supported condition, including inside a compound
tree or beneath `NOT`. Nullable boolean columns preserve unknown rather than coercing
null to false within the expression. Comparisons such as `flag = TRUE`, `TRUE = flag`,
and their `FALSE` or `<>` equivalents are accelerated when Flink normalizes them to
the same direct or negated boolean expression.

The null-safe boolean predicates `IS TRUE`, `IS FALSE`, `IS NOT TRUE`, and `IS NOT FALSE`
are accelerated over any supported boolean expression.

`IS DISTINCT FROM` and `IS NOT DISTINCT FROM` are accelerated for the same numeric,
`CHAR`, `VARCHAR`, `BINARY`, `VARBINARY`, and temporal column/literal and matching column/column types as
ordinary comparisons, plus matching boolean column pairs. Unlike `=` and `<>`, they
always return a non-null boolean and treat two nulls as not distinct.
This includes matching `TINYINT` and `SMALLINT` column pairs; no widening cast is inserted.

## Implementation

Java encodes literal or column operands as protobuf expressions. Rust lowers them to DataFusion
`Column`, `Literal`, `BinaryExpr`, `NotExpr`, `IsNullExpr`, and `IsNotNullExpr` nodes
inside a `FilterExec`. The following `ProjectionExec` consumes its Arrow batches directly. A
paired serializable Java evaluator retains the original Flink `RowKind` for each row
that survives native filtering.

See the [Flink WHERE-clause documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/select/#where-clause).

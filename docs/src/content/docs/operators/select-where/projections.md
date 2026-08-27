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

Precision, scale, fixed width, and nullability are preserved. Every projection in the
Calc must be a direct input-column reference; literals, arithmetic, casts, and functions
currently fall back to Flink. The Calc also falls back if its filter is unsupported.

## Implementation

Java encodes each input reference and its Flink logical type in the protobuf plan. Rust
maps the references to DataFusion `Column` expressions in a `ProjectionExec`. DataFusion
shares the referenced Arrow buffers when no computation requires a new result buffer.

See the [Flink SELECT-clause documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/select/#select-clause).

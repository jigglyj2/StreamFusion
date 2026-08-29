---
title: VALUES
description: Native Arrow batch production and fallback behavior for Flink SQL VALUES.
sidebar:
  order: 4
---

**Current status:** Accelerated for parity-approved scalar literals.

## SQL example

```sql
VALUES
  (1, 'one', DATE '2026-08-28'),
  (2, CAST(NULL AS STRING), DATE '2026-08-29');
```

## Acceleration and fallback

StreamFusion accelerates inline VALUES rows containing nullable or non-null TINYINT,
SMALLINT, INT, BIGINT, FLOAT, DOUBLE, BOOLEAN, CHAR, VARCHAR, BINARY, VARBINARY,
DECIMAL, DATE, TIME, and TIMESTAMP literals. The declared Flink nullability and
precision are retained in the Arrow schema. An empty VALUES physical node is also
valid and produces an empty batch with its declared schema.

Complex literals and types without an exact Flink-to-Arrow literal mapping fall back
with the tuple and field path in EXPLAIN. Because plan replacement is all-or-nothing,
that fallback keeps the complete query on Flink.

## Implementation

The Java planner serializes the typed rows and schema into the versioned StreamFusion
protobuf. Rust constructs DataFusion scalar columns and a `MemorySourceConfig` directly;
the JNI call has no input Arrow array. The resulting Arrow batch crosses the C Data
boundary once and is exposed to Flink as lightweight RowData views. Native execution
therefore does not create a fake input batch or decode rows before constructing Arrow
columns.

See the [Flink 2.3 VALUES documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/sql/queries/values/).

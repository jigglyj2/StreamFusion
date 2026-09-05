---
title: VALUES
description: Native Arrow batch production and fallback behavior for Flink SQL VALUES.
sidebar:
  order: 4
---

**Current status:** Accelerated for parity-approved scalar literals in streaming and bounded plans.

## SQL example

```sql
VALUES
  (1, 'one', DATE '2026-08-28'),
  (2, CAST(NULL AS STRING), DATE '2026-08-29');
```

## Acceleration and fallback

StreamFusion accelerates inline VALUES rows containing nullable or non-null TINYINT,
SMALLINT, INT, BIGINT, FLOAT, DOUBLE, BOOLEAN, CHAR, VARCHAR, BINARY, VARBINARY,
DECIMAL, DATE, TIME, and TIMESTAMP literals at precision 0 through 6. The declared Flink nullability and
precision are retained in the Arrow schema. An empty VALUES physical node is also
valid and produces an empty batch with its declared schema. StreamFusion also accepts
Flink's one-row, zero-column VALUES seed for source-free expressions such as `UNNEST`.

Flink can lower heterogeneous typed rows into a native `UNION ALL` whose branches project
literals over one-row VALUES inputs. StreamFusion removes only semantic no-op casts whose
complete source and target logical types match, then executes those VALUES, Calc, and union
nodes natively in both runtime modes. Multi-row bounded VALUES may remain one
`BatchExecValues` or be normalized into singleton bounded VALUES branches plus
`BatchExecUnion`; both shapes have distinct StreamFusion physical nodes. Casts that change a type, width, precision, or scale still require their own
parity-approved Calc implementation or cause all-or-nothing fallback.

Complex literals, timestamp precision 7 through 9, and types without an exact Flink-to-Arrow literal mapping fall back
with the tuple and field path in EXPLAIN. Because plan replacement is all-or-nothing,
that fallback keeps the complete query on Flink.

## Implementation

The Java planner replaces the original streaming or batch node rather than adding a native
branch inside Flink's VALUES implementation. Both replacements serialize the typed rows and schema into the versioned StreamFusion
protobuf. Rust constructs DataFusion scalar columns and a `MemorySourceConfig` directly;
the JNI call has no input Arrow array. The resulting Arrow batch crosses the C Data
boundary once and remains the internal Arrow payload until the sink-edge view adapter. Native execution
therefore does not create a fake input batch or decode rows before constructing Arrow
columns. Its Flink `numRecordsOut` counter reports the number of logical VALUES rows, not the
single Arrow batch used to carry them. Native plan and output buffers are admitted through the
ordinary Flink operator managed-memory pool.

See the [Flink 2.3 VALUES documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/sql/queries/values/).

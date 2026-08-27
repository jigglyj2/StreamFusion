---
title: SELECT & WHERE
description: Acceleration coverage and fallback behavior for Flink SQL SELECT & WHERE.
sidebar:
  order: 2
---

**Current status:** Not accelerated; executed by Flink.

**Future acceleration target:** Yes.

## SQL example

```sql
SELECT auction, price * 2 AS doubled_price\nFROM bid\nWHERE price >= 100;
```

## Acceleration and fallback

Accelerate when every projection, predicate, cast, and scalar function has Flink-compatible Arrow/DataFusion semantics. Expressions that are unsupported, nondeterministic, depend on Flink runtime context, or differ in null/error behavior force fallback.

## Implementation

Lower Calc/projection/filter nodes to DataFusion physical expressions over Arrow batches. Fuse projection and filtering where profitable and preserve Flink's logical types and three-valued Boolean semantics.

See the [Flink 2.3 SELECT & WHERE documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/select/).

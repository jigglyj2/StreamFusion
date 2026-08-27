---
title: LIMIT
description: Acceleration coverage and fallback behavior for Flink SQL LIMIT.
sidebar:
  order: 13
---

**Current status:** Not accelerated; executed by Flink.

**Can it be accelerated?** For bounded inputs.

## SQL example

```sql
SELECT * FROM bid\nLIMIT 100;
```

## Acceleration and fallback

Accelerate when Flink identifies a bounded input and early termination does not violate connector or checkpoint semantics. Streaming cases or plans requiring Flink-specific cancellation behavior fall back.

## Implementation

Lower to DataFusion Limit and stop producing Arrow batches after the requested row count.

See the [Flink 2.3 LIMIT documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/limit/).


---
title: ORDER BY
description: Acceleration coverage and fallback behavior for Flink SQL ORDER BY.
sidebar:
  order: 12
---

**Current status:** Not accelerated; executed by Flink.

**Future acceleration target:** For bounded inputs.

## SQL example

```sql
SELECT auction, price\nFROM bid\nORDER BY price DESC;
```

## Acceleration and fallback

Accelerate global or partitioned sorting only when the input is bounded and sort keys, null ordering, collation, and spill behavior are compatible. Unbounded global ordering and unsupported types fall back.

## Implementation

Use DataFusion's parallel sort and spill-capable execution. Flink retains distribution and boundedness decisions.

See the [Flink 2.3 ORDER BY documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/orderby/).

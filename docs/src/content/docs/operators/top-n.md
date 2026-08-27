---
title: Top-N
description: Acceleration coverage and fallback behavior for Flink SQL Top-N.
sidebar:
  order: 14
---

**Current status:** Not accelerated; executed by Flink.

**Future acceleration target:** Yes.

## SQL example

```sql
SELECT * FROM (\n  SELECT *, ROW_NUMBER() OVER (PARTITION BY bidder ORDER BY price DESC) AS rank_num\n  FROM bid\n) WHERE rank_num <= 3;
```

## Acceleration and fallback

Accelerate recognized Flink Top-N patterns when partition/order keys, rank range, update mode, and retention policy are supported. General OVER expressions or unsupported changelog keys fall back.

## Implementation

Maintain a compact ordered structure per key in custom Rust code and emit only changed ranks. Apply Flink's no-ranking-output optimization when the rank column is not consumed.

See the [Flink 2.3 Top-N documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/topn/).

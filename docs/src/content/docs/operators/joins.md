---
title: Joins
description: Acceleration coverage and fallback behavior for Flink SQL Joins.
sidebar:
  order: 9
---

**Current status:** Not accelerated; executed by Flink.

**Future acceleration target:** Yes, depending on join type.

## SQL example

```sql
SELECT b.bidder, b.price, p.name\nFROM bid AS b\nJOIN person AS p ON b.bidder = p.id;
```

## Acceleration and fallback

Bounded equi-joins are strong DataFusion candidates. Streaming regular, interval, temporal, lookup, outer, semi, and anti joins require separate parity-tested implementations. Non-equi conditions, connector lookup behavior, unsupported keys, or incompatible state retention fall back.

## Implementation

Use DataFusion hash/sort-merge joins for bounded relations. Use custom Rust keyed state for streaming and interval joins; retain Flink for lookup calls and temporal semantics until a native path can reproduce them.

See the [Flink 2.3 Joins documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/joins/).

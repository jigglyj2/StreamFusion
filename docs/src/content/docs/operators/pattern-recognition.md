---
title: Pattern recognition
description: Acceleration coverage and fallback behavior for Flink SQL Pattern recognition.
sidebar:
  order: 18
---

**Current status:** Not accelerated; executed by Flink.

**Can it be accelerated?** Potentially, with custom execution.

## SQL example

```sql
SELECT * FROM orders\nMATCH_RECOGNIZE (\n  PARTITION BY customer_id ORDER BY order_time\n  MEASURES LAST(B.amount) AS total\n  PATTERN (A B+)\n  DEFINE B AS B.amount > A.amount\n);
```

## Acceleration and fallback

Only explicitly supported MATCH_RECOGNIZE patterns, quantifiers, measures, navigation functions, time constraints, and after-match strategies could accelerate. Every other pattern falls back.

## Implementation

DataFusion has no equivalent streaming CEP engine. Acceleration would require a custom Rust NFA with Flink-compatible match selection, event-time timers, and state snapshots.

See the [Flink 2.3 Pattern recognition documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/match_recognize/).


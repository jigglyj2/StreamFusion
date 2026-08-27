---
title: Set operations
description: Acceleration coverage and fallback behavior for Flink SQL Set operations.
sidebar:
  order: 11
---

**Current status:** Not accelerated; executed by Flink.

**Future acceleration target:** Yes, by operation.

## SQL example

```sql
SELECT bidder FROM mobile_bids\nUNION ALL\nSELECT bidder FROM web_bids;
```

## Acceleration and fallback

UNION ALL can usually pass through accelerated batches. UNION DISTINCT, INTERSECT, EXCEPT, IN, and EXISTS require compatible equality, null, state, and changelog semantics; otherwise they fall back.

## Implementation

Use DataFusion set operations for bounded data. Streaming distinct/intersection/difference require custom reference-counted keyed state coordinated with Flink checkpoints.

See the [Flink 2.3 Set operations documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/set-ops/).

---
title: Set operations
description: Acceleration coverage and fallback behavior for Flink SQL Set operations.
sidebar:
  order: 11
---

**Current status:** `UNION ALL` is accelerated. Other set operations fall back to Flink.

## SQL example

```sql
SELECT bidder FROM mobile_bids
UNION ALL
SELECT bidder FROM web_bids;
```

## Acceleration and fallback

`UNION ALL` is eligible when every input has the same Flink logical row type and every
internal operator in every branch is accelerated. It preserves duplicates, nulls,
changelog records, and each record's `RowKind`. A rejected node in any branch causes the
entire query to fall back under the normal all-or-nothing rule.

`UNION`/`UNION DISTINCT` is not accelerated because it adds deduplication. `INTERSECT`,
`EXCEPT`, `IN`, and `EXISTS` also remain on Flink. Their equality, null, keyed-state,
retention, and changelog behavior needs dedicated parity work. EXPLAIN reports the
unimplemented physical node that caused whole-plan fallback.

## Implementation

Flink streaming `UNION ALL` is deliberately a zero-work topology operation rather than
a runtime operator. StreamFusion replaces `StreamExecUnion` with the distinct
`StreamFusionExecUnion` physical node, then preserves Flink's `UnionTransformation`.
This lets Flink retain scheduling, watermark, checkpoint-barrier, and record-interleaving
semantics without converting or copying rows. Native Calc chains in each input branch
still execute normally at their own Arrow boundaries.

StreamFusion does **not** lower streaming `UNION ALL` to DataFusion `UnionExec`, whose
partition-at-a-time batch behavior is not Flink's streaming merge contract. Future
bounded-only set operations may use DataFusion. Stateful streaming distinct,
intersection, and difference require Flink-checkpointed native keyed state.

See the [Flink 2.3 Set operations documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/set-ops/).

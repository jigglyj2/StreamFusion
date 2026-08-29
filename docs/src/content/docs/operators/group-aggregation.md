---
title: Group aggregation
description: Acceleration coverage and fallback behavior for Flink SQL Group aggregation.
sidebar:
  order: 6
---

**Current status:** Not accelerated; executed by Flink.

**Future acceleration target:** Yes.

## SQL example

```sql
SELECT bidder, COUNT(*) AS bids, SUM(price) AS spend\nFROM bid\nGROUP BY bidder;
```

## Acceleration and fallback

Group aggregation is not accelerated because StreamFusion does not yet implement Flink's
streaming aggregate state and changelog behavior. The internal `StreamExecExpand` step
used by grouping sets, `ROLLUP`, and `CUBE` does have a native implementation when every
projection is a supported Calc expression. That partial coverage does not accelerate a
grouping query: the all-or-nothing planner falls back the complete plan while its
aggregate node remains unsupported.

## Implementation

Native Expand lowers each Flink projection alternative to a DataFusion `ProjectionExec`
and combines the alternatives with `UnionExec`. The alternatives share the input Arrow
batch and carry StreamFusion's private input-row ordinal, allowing RowKind and timestamp
metadata to be restored at the JVM boundary. No aggregate kernel or aggregate state is
implemented yet.

See the [Flink 2.3 Group aggregation documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/group-agg/).

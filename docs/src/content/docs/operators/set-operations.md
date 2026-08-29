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
changelog records, each record's `RowKind`, timestamps, and the order of records within
each individual input. Flink does not define a stable ordering between different union
inputs. A rejected node in any branch causes the entire query to fall back under the
normal all-or-nothing rule.

The common row type must also fit Arrow's complete value domain. In particular,
`TIMESTAMP(7..9)` and nested occurrences of it fall back before boundary conversion because
Arrow nanosecond timestamps cannot represent Flink's complete calendar range.

`UNION`/`UNION DISTINCT` is not accelerated because it adds deduplication. `INTERSECT`,
`EXCEPT`, `IN`, and `EXISTS` also remain on Flink. Their equality, null, keyed-state,
retention, and changelog behavior needs dedicated parity work. EXPLAIN reports the
unimplemented physical node that caused whole-plan fallback.

## Implementation

The planner replaces `StreamExecUnion` with the distinct `StreamFusionExecUnion` node.
Its Flink runtime is a non-keyed multiple-input operator: Flink still schedules and
multiplexes the inputs, aligns checkpoint barriers, combines watermarks, and tracks input
idleness. The operator transposes buffered rows from each input into Arrow batches and
sends a versioned protobuf `Union` tree plus those batches through Arrow C Data. Rust
binds each indexed protobuf `Input` to its corresponding Arrow batch and executes
DataFusion `UnionExec`.

The native union forwards the input batches' shared Arrow buffers; it does not serialize
or copy complete batches between native children. A global input-row ordinal travels as
a private Arrow column so the JVM boundary can restore each record's `RowKind` and
timestamp after native execution. Buffered records are flushed before watermarks,
watermark-status changes, record attributes, latency markers, checkpoint barriers, and
end-of-input notifications. This keeps data ahead of the Flink control event that
follows it and leaves recovery and coordination in Flink.

Stateful streaming distinct, intersection, and difference require Flink-checkpointed
native keyed state and remain unsupported.

See the [Flink 2.3 Set operations documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/set-ops/).

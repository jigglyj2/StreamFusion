---
title: Set operations
description: Acceleration coverage and fallback behavior for Flink SQL Set operations.
sidebar:
  order: 11
---

**Current status:** `UNION ALL` and `UNION DISTINCT` are accelerated. Other set operations fall back to Flink.

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

`UNION`/`UNION DISTINCT` uses native `UNION ALL`, native hash exchange, and the native
group-aggregate membership state used by `SELECT DISTINCT`. It therefore supports the
same complete Arrow key-type matrix, changelog retractions, memory/RocksDB backends,
canonical restore and rescaling contract, and managed-memory accounting as native
distinct aggregation. `INTERSECT`, `EXCEPT`, `IN`, and `EXISTS` remain on Flink. Their
equality, null, keyed-state, retention, and changelog behavior needs dedicated parity
work. EXPLAIN reports the unimplemented physical node that caused whole-plan fallback.

## Implementation

The planner replaces `StreamExecUnion` with the distinct `StreamFusionExecUnion` node.
Its Flink runtime is a non-keyed multiple-input operator: Flink still schedules and
multiplexes the inputs, aligns checkpoint barriers, combines watermarks, and tracks input
idleness. A multiple-input gate is an unavoidable Flink network boundary, so each native
branch first emits the same schema-negotiated Arrow IPC exchange frame used by native
hash exchange. The union decodes each frame using Flink-managed memory and forwards its
Arrow batch immediately. It does not buffer rows, transpose through `RowData`, invoke a
merge kernel, or copy the decoded column buffers merely to implement union semantics.

The frame carries the input batches' Flink `RowKind` and record-timestamp envelope as
metadata vectors. Decoding restores that envelope without reconstructing payload rows.
Flink's multiple-input operator provides the control-event ordering, combined watermark,
barrier alignment, idleness, and end-of-input behavior.

Stateful streaming intersection and difference require dedicated Flink-checkpointed
native keyed operators and remain unsupported.

See the [Flink 2.3 Set operations documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/set-ops/).

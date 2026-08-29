---
title: Exchange
description: Acceleration coverage and fallback behavior for planner-inserted Flink exchanges.
sidebar:
  order: 12
---

**Current status:** Hash and singleton exchanges are accelerated when the entire physical plan is eligible.

## SQL example

Exchange is not SQL syntax of its own. Flink inserts it when an operation needs data redistributed,
for example before a keyed aggregation:

```sql
SELECT seller, COUNT(*)
FROM auction
GROUP BY seller;
```

The aggregation in this example is not accelerated yet, so the all-or-nothing rule currently keeps
the complete query on Flink. It illustrates where Flink introduces the exchange; StreamFusion's
runtime and planner-node tests exercise an eligible exchange independently until an accelerated SQL
operator needs redistribution.

## Acceleration and fallback

Hash distribution is eligible for nullable or composite keys made from booleans, numeric and decimal
values, character or binary strings, dates, times, and timestamp values. Singleton distribution is
also eligible. Complex `ARRAY`, `MAP`, `MULTISET`, or `ROW` hash keys fall back because their exact
Flink `BinaryRowData` key encoding is not implemented. Complex non-key columns remain supported when
their boundary representation is supported.

Unsupported distributions, Arrow-incompatible boundary types, dictionary-encoded IPC batches, or
any other unsupported node in the graph cause whole-plan fallback. EXPLAIN identifies the rejected
exchange or the other node that prevented selection.

## Implementation

Flink still owns the network topology, control events, checkpointing, recovery, maximum parallelism,
and rescaling. A native writer receives the existing Arrow batch, reuses its data buffers, adds only
the Flink record-envelope vectors, and Rust computes exactly the same
`BinaryRowData` hash, Murmur mix, and stable key group as Flink 2.3. Each schema-free Arrow IPC frame
contains rows for one key group. Flink maps that key group to the current downstream subtask and can
remap restored frames after rescaling with its `RANGE` channel-state mapping. A native reader decodes
the frame back into an Arrow batch and restores the envelope sidecar without row materialization.
Flink's record counters continue to report logical rows on both sides of the exchange;
internal Arrow IPC frames are transport units and are not published as record counts.

The exchange stays in StreamFusion's core Flink runtime and planner modules because that mirrors
Flink's own module design; it is not a separately deployed connector or integration.

See the [Flink 2.3 streaming dataflow documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/flink-architecture/#dataflow-programming-model).

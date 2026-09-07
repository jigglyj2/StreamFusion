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

This aggregation and its exchange can be accelerated when the aggregate calls and boundary types
are supported.

## Acceleration and fallback

Hash distribution is eligible for nullable or composite keys across supported Flink SQL types,
including intervals, `ARRAY`, `MAP`, `MULTISET`, `ROW`, distinct types, and nested combinations.
Scalar keys are encoded directly in Rust. For key shapes without an independently proven native
encoder, the Java writer adds one input-only opaque `BinaryRowData` key sidecar. Rust hashes those
canonical bytes directly and strips the sidecar before network transport. Singleton distribution is
also eligible.

The shared native key codec also supports Arrow lists, maps, and structs recursively for native
state consumers after an exchange strips its routing sidecar. It writes Flink's nested container
layouts into one caller-owned scratch buffer, with container-relative offsets and array-specific
NaN normalization. Exact array/map/row byte fixtures are checked against Flink's serializers.
This does not change the exchange planner's existing sidecar selection or claim full nested-type
rescaling coverage; native state consumers and exchange routing share the key-group hash code.

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
the frame directly at the native-plan edge and restores its owned record envelope. The decoded
batch stays in Rust; it is not imported into Arrow Java and exported back to Rust. Frame-consuming
regions use plan protocol 3 even when their tree contains only stateless UNION stages. The incoming
allocation aligns the IPC body so fixed-width and decimal buffers do not need decoder alignment
copies; padding and the allocation capacity are included in the single payload reservation.
Flink's record counters continue to report logical rows on both sides of the exchange;
internal Arrow IPC frames are transport units and are not published as record counts.

The exchange stays in StreamFusion's core Flink runtime and planner modules because that mirrors
Flink's own module design; it is not a separately deployed connector or integration.

See the [Flink 2.3 streaming dataflow documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/flink-architecture/#dataflow-programming-model).

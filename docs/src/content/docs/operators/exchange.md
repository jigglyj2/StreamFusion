---
title: Exchange
description: Acceleration coverage and fallback behavior for planner-inserted Flink exchanges.
sidebar:
  order: 12
---

The writer binds its validated routing plan, key descriptors, and Flink reservation broker once at
task open and reuses them for each Arrow batch. Native IPC metadata and bodies are copied directly
into the final Java transport envelope, without an intermediate concatenated Rust payload. The
standard Arrow IPC wire format and key-group metadata are unchanged. Early routing or admission
failures release any unconsumed Arrow C Data exports before their descriptor storage is closed.

**Current status:** Hash and singleton exchanges are accelerated when the entire physical plan is eligible.

Exchange input plans and Arrow transport schemas are bound to native input ports at task open.
The receiver reuses those schemas for every frame; it does not send a protobuf plan through JNI
or rebuild the schema for each network message. Rebinding a port to a different exchange plan is
an error. Binding memory belongs to the native context and is released on close. A failed frame
releases its temporary buffers while retaining that prepared schema for a valid retry. IPC still
has one body-aligned payload copy at the receiving plan edge,
then shares its buffers through the native operators.

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
Scalar keys are encoded directly in Rust using one reusable scratch buffer per batch. Both
key-group and destination routing use the same key loop. For key shapes without an independently proven native
encoder, the Java writer adds one input-only opaque `BinaryRowData` key sidecar. Rust hashes those
canonical bytes in place without copying each key and strips the sidecar before network transport.
Null or non-word-aligned opaque keys produce a recoverable routing error. Singleton distribution is
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
Routing projects away input-only key sidecars before gathering any payload. A destination whose
rows form a contiguous range uses an Arrow slice, including non-zero offsets; IPC writes that
slice without first copying it into another Arrow batch. Only scattered destination rows require
a gather. Nullable, nested, variable-width, decimal, and changelog values retain their Flink bytes.

Routing reserves bucket descriptors, row selections, and reusable key scratch before allocating
those workspaces. Before each destination is gathered and encoded, it reserves a conservative
Arrow gather/IPC allowance while keeping previously encoded frames charged. The allowance covers
nested child selections, validity and offset rebasing, IPC padding, and buffer growth. It counts
transmitted buffer lengths rather than charging a shared input allocation once per column. Input
buffers retain their existing Arrow ownership accounting. Completed frame capacities stay reserved
until their Java transport envelopes have been exported; admission or encoding failure releases
partial frames and their reservations. This uses Flink's existing native execution budget and has
no separate tuning option or per-row reservation callback.

For native producers with scalar native-encoded keys or singleton distribution, routing is bound
to the producing native plan. Its output driver builds the IPC frames directly from native Arrow
batches and returns them on Flink transport side outputs. There is no separate Java writer or
Arrow Java export/import round trip on that path. Multiple exchange consumers reuse the producer
batch's reference-counted buffers. A simultaneous Arrow consumer keeps its own C Data output;
the finalized Flink graph determines which outputs are needed. Routing plans are prepared once
at task open and released with the native context, including cancellation paths.

Java control/source edges and keys requiring the opaque Flink key sidecar still use the separate
Java writer. For those complex-key native outputs, the Arrow Java round trip remains a limitation
until the native key encoder has the required exchange parity coverage. Both paths use the same
Rust routing and memory-admission implementation; this does not change the network frame format.

Flink's record counters continue to report logical rows on both sides of the exchange;
internal Arrow IPC frames are transport units and are not published as record counts.

The exchange stays in StreamFusion's core Flink runtime and planner modules because that mirrors
Flink's own module design; it is not a separately deployed connector or integration.

See the [Flink 2.3 streaming dataflow documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/flink-architecture/#dataflow-programming-model).

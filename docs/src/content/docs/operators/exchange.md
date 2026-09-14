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
Scalar keys and recursively supported `ARRAY`, `MAP`, and `ROW` keys are encoded directly in
Rust using one reusable scratch buffer per batch. Admission uses the existing physical protobuf
mapping: intervals use integers, multisets use maps with integer counts, distinct types use their
source type, and structured types use rows. These aliases do not require a Java key-selector pass
or an extra input-side key vector. Key-group and destination routing use the same key loop.
Singleton distribution is also eligible. Older protocol 1 plans with an opaque `BinaryRowData`
key sidecar remain readable; Rust hashes those bytes in place and strips input-only keys before
network transport. Null or non-word-aligned opaque keys produce a recoverable routing error.

The shared native key codec also supports Arrow lists, maps, and structs recursively for native
state consumers after an exchange strips its routing sidecar. It writes Flink's nested container
layouts into one caller-owned scratch buffer, with container-relative offsets and array-specific
NaN normalization. Exact array/map/row byte fixtures are checked against Flink's serializers.
Exchange protocol 2 selects that encoder for recursively supported nested keys. When a native
consumer still expects the canonical key column, Rust encodes it once into an admitted Arrow
Binary vector, hashes those same bytes, and carries the vector in IPC. Payload buffers stay shared.
Generated tests compare complete canonical key bytes with Flink for sliced nullable nested input,
including array null-word boundaries, array NaNs, decimals, timestamps, maps, and rows; the same
key groups map to Flink's destinations at multiple parallelisms. The generated cases also compare
complete serialized changelog records and record timestamps. Alias cases include scalar and nested
intervals, multisets, distinct scalar/array types, and structured rows. A native-output topology test
compares a composite alias key and every RowKind with Flink while asserting that no Java writer is
inserted. The tests materialize Flink's physical rows before passing them to the original logical
type's key selector, as Flink's generic BinaryWriter does not directly accept DISTINCT_TYPE.

Unsupported distributions, Arrow-incompatible boundary types, dictionary-encoded IPC batches, or
any other unsupported node in the graph cause whole-plan fallback. EXPLAIN identifies the rejected
exchange or the other node that prevented selection.

## Implementation

Flink still owns the network topology, control events, checkpointing, recovery, maximum parallelism,
and rescaling. A native writer receives the existing Arrow batch, reuses its data buffers, adds only
the Flink record-envelope vectors, and Rust computes exactly the same
`BinaryRowData` hash, Murmur mix, and stable key group as Flink 2.3. Each schema-free Arrow IPC frame
contains rows for one key group when unaligned checkpoints require rescalable channel state.
These frames follow contiguous key-group runs in input order; recurring groups are not collected
into a single frame. Flink maps each key group to the current downstream subtask and can
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

This preserves Flink's per-channel FIFO ordering, including the order between different keys sent
to the same downstream subtask. The previous unaligned routing grouped every occurrence of a key
group across a batch, changing the complete changelog order. Aligned destination routing already
preserves input order within each destination. Key-group runs use Arrow slices with shared buffers;
when keys alternate, a run can contain one row and the batch can produce more frames than maximum
parallelism. This is required by Flink's one-key-group-per-record recovery filter. It can increase
IPC overhead; no throughput improvement is claimed. The frame format, routing tags, and decoder
are unchanged. Source comparison uses Flink 2.3.0 `KeyGroupStreamPartitioner` and `RecordWriter`.
Generated recovery tests now compare complete ordered channel bytes, including changelog kinds,
timestamps, and control events, against Flink across fragmentation, disk spill, and rescaling.

Routing reserves bucket descriptors, row selections, and reusable key scratch before allocating
those workspaces. Unaligned admission includes up to one run/frame descriptor per input row.
Before each destination is gathered and encoded, it reserves a conservative
Arrow gather/IPC allowance while keeping previously encoded frames charged. The allowance covers
nested child selections, validity and offset rebasing, IPC padding, and buffer growth. It counts
transmitted buffer lengths rather than charging a shared input allocation once per column. Input
buffers retain their existing Arrow ownership accounting. Completed frame capacities stay reserved
until their Java transport envelopes have been exported; admission or encoding failure releases
partial frames and their reservations. This uses Flink's existing native execution budget and has
no separate tuning option or per-row reservation callback.

For native producers with native-encoded scalar/nested keys or singleton distribution, routing is bound
to the producing native plan. Its output driver builds the IPC frames directly from native Arrow
batches and returns them on Flink transport side outputs. There is no separate Java writer or
Arrow Java export/import round trip on that path. Multiple exchange consumers reuse the producer
batch's reference-counted buffers. A simultaneous Arrow consumer keeps its own C Data output;
the finalized Flink graph determines which outputs are needed. Routing plans are prepared once
at task open and released with the native context, including cancellation paths.

Java control/source edges still use the separate Java writer. Older explicitly preencoded plans
retain their key adapter for compatibility. Newly planned supported aliases use the producing
native plan's output route, just like their physical scalar/map/row types. Both paths use the same
Rust routing and memory-admission implementation; the network frame format is unchanged.

Flink's record counters continue to report logical rows on both sides of the exchange;
internal Arrow IPC frames are transport units and are not published as record counts. Failure
paths count the input rows already received and the logical rows in each attempted output frame,
including the frame whose consumer threw, following Flink's count-before-collect semantics.

The exchange stays in StreamFusion's core Flink runtime and planner modules because that mirrors
Flink's own module design; it is not a separately deployed connector or integration.

See the [Flink 2.3 streaming dataflow documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/flink-architecture/#dataflow-programming-model).

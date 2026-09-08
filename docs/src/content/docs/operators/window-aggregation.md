---
title: Window aggregation
description: Acceleration coverage and fallback behavior for Flink SQL Window aggregation.
sidebar:
  order: 7
---

**Current status:** Temporarily uses whole-plan Flink fallback under the
[architecture admission requirements](/StreamFusion/development/architecture-admission/). The native paths
described below are retained for development and direct parity tests; SQL planning does not select them.

**Retained implementation scope:** Partial implementation for native `TUMBLE`, `HOP`, `CUMULATE`, and `SESSION`
aggregation, including Flink's legacy group-window physical node.

## SQL example

```sql
SELECT window_start, bidder, SUM(price)
FROM TABLE(TUMBLE(TABLE bid, DESCRIPTOR(dateTime), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, bidder;
```

## Q5 admission work

Ordinary Q5 EXPLAIN on both backends currently reports blocked local/global window stages and
an aggregate reused by two consumers. The retained standalone handles do not yet participate in
the common native execution, control and metric lifecycle. In particular, the existing local
kernel emits partials per Arrow batch; Flink's `LocalSlicingWindowAggOperator` buffers across
batches and flushes on applicable watermarks, checkpoint pre-barriers or memory pressure.
That difference must be corrected and tested before admission. Duplicating the reused aggregate
or disabling Flink's reuse optimizer is not an acceptable workaround.

An extracted SQL-generated Flink local slicer now pins the control contract in focused tests.
A triggering watermark flushes every buffered slice in first-appearance order, including future
slices, and emits timestamp-less INSERT partials. A later row for an already-due slice can remain
buffered until the next scheduled trigger or checkpoint pre-barrier. A 3 MiB operator-only managed
memory fixture verifies pressure flushing and preservation of all 180,000 generated contributions.
These upstream-oracle tests define the required native behavior; they do not claim its migration
is complete.

A native capacity calculation now matches shared fixtures checked against Flink's actual
`WindowBytesMultiMap`, including fixed-width rows, large variable-width keys/values, hash-table
growth and reset. It counts Flink page geometry without constructing or serializing RowData.
This is semantic flush-capacity bookkeeping, separate from coarse native buffer reservations.
A buffered implementation, available through explicit task-resource bindings, uses this capacity model with DataFusion grouped state
retained across Arrow batches. It probes Arrow row encodings by reference and copies keys only
when a new group appears. It matches the SQL-generated Flink watermark/pre-barrier oracle and
all partials from the 180,000-row pressure fixture with Arrow batch sizes 127, 4,096 and 100,000.
Flush output is limited to 2,048 partials per pull and preserves first-appearance order; pending
input must drain before another batch or control. Coarse admitted workspace transfers credit to
retained state and Arrow output owners, with denial and cancellation/last-owner tests. The tested
buffered subset uses UTC, non-null time/bound columns, append-only grouped accumulators and
fixed-width Flink row geometry. Nullable time/bound admission remains deferred until the streaming
Flink parity contract is verified.

A Calc → local window → Calc test runs this buffer through the shared native unary execution tree.
`NativeTaskBindings` v1 binds each local-window plan-node ID to Flink's resolved buffer-memory
share and page size before capability negotiation and execution. The JNI constructor keeps these
non-keyed resources separate from keyed-state bindings; malformed, duplicate, missing, unsupported
or late bindings are rejected transactionally. Direct Java tests compare generated control
changelogs and pressure-flush outputs with the real SQL-generated Flink slicer through Arrow C
Data/C Stream. Watermarks and checkpoint pre-barriers drain its bounded output before the
control completes; invocation EOF and end-input alone do not flush. Partials carry timestamp-less
INSERT metadata, each stage counts logical records, and invalid RowKinds or cancellation require
recovery. Payload buffers retain their existing native leases; only new metadata receives another
allocation allowance. Binding the original Flink operator memory share from Java and checking the
complete Flink metric surface and replay lifecycle remain required. The legacy production handle still flushes per batch, and Q5
continues to fall back.

Compatible append-only local windows now use DataFusion's `GroupsAccumulator` vectors across
all keys in a batch. The native handle prepares these adapters once and reuses them after each
flush. SQL columns are shared directly with the kernels; nullable timestamp and FILTER masks
select contributions without gathering or copying the whole input batch. Canonical Flink
accumulator objects are constructed only while encoding each output partial, rather than retained
for every key. The grouped-state test covers 4,096 groups across four batches, overflow, nulls and
nullable filters; observed-allocation tests check the coarse workspace on hot and unique keys.
The real Flink SQL matrix also includes FILTER predicates. This does not yet change the legacy
per-batch flush lifecycle or admit Q5.

The local kernel now uses shared DataFusion aggregate adapters for reusable integer COUNT/SUM/AVG
and append-only MIN/MAX computation. Ordered retractions and numeric subsets that require Flink
semantics retain the existing adapters. A coarse workspace covers row encodings, selections,
accumulator deltas and partial-output buffers before allocation; output ownership moves from that
allowance to the compatibility C Data edge without a post-allocation reservation. Plan decoding
and schema/codec construction are separately admitted. Generated direct-kernel tests compare
TUMBLE/HOP/CUMULATE outputs byte-for-byte with real Flink SQL across two batch sizes and both
backends, including nullable keys/timestamps/values and integer overflow. Native tests additionally
compare ordered accumulator bytes for all RowKinds and verify memory-denial cleanup. This is a
prerequisite, not Q5 admission.

The SQL-generated global HOP oracle additionally pins shared-slice state and late-input behavior
on both Flink backends, before and after checkpoint restore. One buffered partial becomes one
slice state and one initial timer, rather than one state per overlapping window. A partial arriving
after its base slice fired is still accepted until its last overlapping window fires; the late-drop
counter increments once only when that last window has fired. An extra empty-window timer ends
the trigger chain after the final nonempty window. Flink restores its checkpointed watermark
before processing replayed input. The retained native global-partial kernel now matches that input-level late counter on both
backends, including partially late inputs that still contribute to later windows. Its partial-input
logic and tests live in a separate operator submodule. It still expands slices into windows and
needs shared-slice state, restored-watermark and shared-control corrections before admission.

Global partial batches containing COUNT and compatible append-only MIN/MAX now merge through
DataFusion grouped accumulators. Each accepted input accumulator is decoded once, and temporary
merge columns are built in chunks of at most 2,048 contributions. COUNT partials use DataFusion's
wrapping BIGINT SUM kernel; SQL FILTER is already represented in the partial and is not reapplied.
Flink's ordered empty-group and timer transitions remain separate from aggregate computation.
Delta inputs, cardinality overflow, DISTINCT, retractable extrema and other aggregate families
retain the existing ordered merge path. Generated SQL parity covers both this grouped subset and
the mixed SUM/AVG path on both backends, with nullable values, filters and different batch sizes.
Coarse partial workspace accounts for overlapping-window fanout and encoded payload size before
allocation. New timers from these append-only partial batches are deduplicated before one host
reservation. Window timer firing transfers existing key/namespace credit into a bounded callback
owner and admits its descriptor vector before removing timers. Credit stays live through the
callback, including when the timer service closes first; failed admission leaves the timer index
unchanged. Canonical timer bytes and firing order are unchanged. This does not yet implement
shared-slice storage or change ordinary planner admission.

A separate development implementation now stores each HOP base slice once, using versioned
Arrow row keys for the grouping identity and sortable slice end. Flink BinaryRow hashing still
selects the key group. DataFusion grouped accumulators merge COUNT and compatible append-only
extrema on input and when windows fire. Input state reads/writes are batched; firing reads at most
4,096 requested slice keys per page and emits at most 1,024 windows per pull. It advances one timer
timestamp at a time so timers created during firing run before later windows delete their slices.
The extra empty-window timer is preserved.

This development path eagerly merges each incoming Arrow batch into slice state instead of
retaining Flink's additional global raw-row buffer. This follows the native batch execution and
batched-state contract: emission still waits for the watermark. Unlike the local buffer, the global
buffer does not emit intermediate partial rows. Full shared-runtime metric and lifecycle parity
must still verify this adaptation before admission. The heap timer index is serialized at Flink's
canonical or physical checkpoint boundary, rather than rewritten after every input batch.
Snapshot markers fingerprint the window contract and pin the slice/Arrow encoding; restoration
rejects expanded-window state, whose original slices cannot generally be recovered from merged
extrema. Flink must supply the restored operator watermark separately from keyed snapshots.

Native development tests cover the SQL-generated Flink global HOP control contract, generated
inputs compared with the existing Flink-verified expanded kernel, memory/RocksDB restore,
1→2→1 rescaling, physical RocksDB checkpoints, memory denial, and a 5,000-slice window read in
bounded pages. Storage instrumentation checks one retained value per slice and writes that do
not grow with the entire timer index. This implementation is currently compiled only for tests;
shared execution-tree/resource bindings, direct generated Flink SQL parity through that tree,
complete metric/control recovery checks and ordinary planner admission remain outstanding.
There is no Q5 acceleration or new Q5 benchmark result yet.

## Retained semantic implementation

The retained implementation supports direct time-attribute window aggregation for event time and processing
time. It recognizes both Flink's one-phase node and its default local-aggregate, exchange,
global-aggregate shape. For Flink's two-phase plan, StreamFusion preserves all three stages: a
state-free native local aggregate emits one opaque canonical accumulator per key and base slice,
the existing key-group exchange partitions those partials, and the native global aggregate merges
them into keyed window state. Partial payloads use Arrow, but these older standalone stages still have intermediate JNI/Java
handoffs. Their migration into the shared native execution tree remains required.
The retained kernel also supports an aggregate over already attached `window_start` and `window_end` columns, as
produced by a preceding window aggregate. Each attached pair is one exact namespace; it is not
assigned to overlapping windows a second time. This represents the nested hopping aggregation in
Nexmark Q5 at the kernel level; fused composition and ordinary admission are not yet verified.
Legacy SQL/Table API time windows lower to the same canonical native state machine. Legacy Table
API processing-time row-count tumbling and sliding windows also have retained kernels; Flink 2.3's SQL
grammar does not expose numeric row-count intervals, but its Table API and physical executor do.
In bounded mode, the retained lowering represents Flink's hash- and sort-based legacy time-window executors in both
one- and two-phase forms. StreamFusion never substitutes one form for the other. A Flink one-phase
node becomes one stateful native aggregate; a Flink local/exchange/global plan retains all three
physical stages. In the two-phase form the native local phase emits opaque accumulator bytes, the
Arrow exchange partitions them by Flink key group, and the native global phase merges them. In the
one-phase form the final node consumes either the existing in-task Arrow stream or, when Flink
requires redistribution, decodes the Arrow IPC frame directly at its native-plan edge. Tumbling and
pane-based sliding windows share these paths and emit only at end of input. As in Flink's bounded
executor, intermediate watermarks are forwarded as control records and do not fire or evict
windows before end of input.
The native calls are `COUNT(*)`, `COUNT(value)`, `SUM`, `AVG`, `MIN`, and `MAX`, including SQL
`FILTER (WHERE ...)` with nullable Boolean predicates. `AVG` accepts every Flink numeric input,
supports retractions, and merges its sum/count buffers across session namespaces. Keys use Arrow's canonical row encoding and
include nullable scalar, decimal, temporal, binary, array, map, multiset, row, and nested SQL
values. Input `INSERT`, `UPDATE_BEFORE`, `UPDATE_AFTER`, and `DELETE` kinds are supported when Flink
selects retractable accumulators.

Window start, end, row-time, and processing-time properties are supported. Null event timestamps,
late rows, watermark cleanup, timer ordering, offsets, negative epochs, `TIMESTAMP_LTZ`, configured
local time zones, and daylight-saving gaps/overlaps follow Flink. Session windows perform
transitive merging and keep Flink's merged namespace when a bridging row retracts.

Legacy early/late firing, distinct/approximate or user-defined aggregate calls, async state,
changelog-state wrapping, and unsupported surrounding physical nodes produce an explicit
whole-plan fallback reason. Row-count windows do not expose time window properties, matching
Flink's legacy contract.

## Implementation

The Java planner serializes the complete physical contract in the versioned plan protobuf. Rust
batches records by key and window, computes Flink key groups, and uses the shared opaque keyed-state
interface. Both the managed in-memory backend and the optional direct RocksDB component perform
batched reads and atomic batched mutations without per-record JNI state calls.
Row-count windows store their per-key element index in the same canonical key-group envelope; each
input batch performs one batched index read, one batched window-state read, and one atomic write.
Time windows perform one batched state read and one atomic state/timer write per input batch. The
hot input loop reuses Flink BinaryRow-key and assigned-window scratch buffers, while canonical
state keys receive owned storage only when a new key/window is staged.

The current local kernel is state-free across Arrow batches, which is a known lifecycle gap
relative to Flink and is being replaced before admission. Its
temporary hash table and output buffers are charged to the local stage's Flink managed-memory
share. The global half is the sole owner of canonical keyed state and timers, so aligned and
unaligned checkpointing, savepoint restoration, backend switching, and rescaling use exactly the
same state format as one-phase execution. One-phase execution owns that canonical keyed state
directly. RocksDB performs one batched read and one atomic batched write per incoming raw or partial
Arrow batch.

Timer firing is bounded to 4,096 namespaces per output batch. If one watermark or bounded
end-of-input closes more windows, Java repeatedly advances the same native timer frontier and emits
multiple Arrow batches. This keeps state reads, mutations, and output allocation inside Flink's
managed-memory grant without changing deterministic timer order.

A backend-neutral native timer service stores event-time and processing-time timers per key group.
Its canonical bytes travel with raw keyed snapshots, so aligned and unaligned checkpoints,
cross-backend canonical savepoints, and 1-to-2-to-1 rescaling preserve pending windows. Direct
RocksDB checkpoints reuse immutable SST handles incrementally. Timer, state, scratch, and exported
Arrow allocations are charged through Flink managed memory. Append-only sessions merge compact
accumulators; changelog sessions additionally retain the exact event contributions required for
retractions. Stateful window aggregates request the same relative Flink operator-memory weight as
native deduplication, grouped aggregation, and Top-N; this prevents nested attached-window plans
from receiving a stateless stage's undersized share while retaining Flink's single managed-memory
budget and admission control.

Attached-window coverage includes generated byte-for-byte SQL parity, all four changelog kinds in
the native state test, and canonical memory-to-RocksDB restoration. The restoration test also
asserts that no additional HOP namespaces survive after the single attached namespace fires.
Bounded one- and two-phase coverage compares Flink and StreamFusion results for fixed-width
tumbling, variable-width tumbling, and pane-based sliding aggregates on both state backends. The
tests force Flink's phase strategy: one-phase runs require a zero local-stage count, while
two-phase runs require both native phase counters to be nonzero. Each sliding case additionally
runs with distributed parallelism four to exercise the key-group exchange. The direct one-phase
and framed global operators have aligned, unaligned, and canonical savepoint restore coverage,
including memory-to-RocksDB and RocksDB-to-memory restore; the framed path also covers 1-to-2-to-1
rescaling restore.

See the [Flink 2.3 Window aggregation documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-agg/).

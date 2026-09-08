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

Ordinary Q5 EXPLAIN on both backends still reports blocked local/global window stages and an
aggregate reused by two consumers. Explicit native resource bindings now run the local buffer
and global HOP slicer, including attached-window MAX/COUNT, through the common DataFusion execution tree.
The selected global physical node now produces its own protobuf fragment and joins that shared
region, retaining the original Flink stage identity and keyed-state binding. These development
paths do not yet change ordinary planner selection, and there is no Q5 performance result.

### Local buffer

The buffered local implementation retains DataFusion `GroupsAccumulator` vectors across Arrow
batches. It probes Arrow row keys by reference and copies retained keys only for new groups.
Flink-compatible watermark, checkpoint pre-barrier and memory-pressure boundaries flush partials
in first-appearance order, including future slices when a trigger flushes the buffer. Outputs are
timestamp-less INSERT partials, limited to 2,048 rows per pull. Invocation EOF and end-input alone
do not flush; a terminal watermark uses the normal event-time path.

`NativeTaskBindings` v1 binds each local stage to the original Flink operator's resolved memory
share and page size before lowering. A capacity model matches Flink's `WindowBytesMultiMap`
geometry without constructing RowData. This bookkeeping preserves observable pressure-flush
boundaries; actual native buffers and retained state use coarse Flink memory reservations.
The buffered subset currently requires UTC, non-null time/bound columns, fixed-width Flink row
geometry and compatible append-only DataFusion aggregates. Nullable time/bound streaming parity
and automatic binding of the original physical operator's memory share remain outstanding.

The retained legacy local handle still flushes per Arrow batch. Its reusable integer COUNT/SUM/AVG
and append-only MIN/MAX computation uses DataFusion, with ordered Flink adapters for retractions
and incompatible numeric semantics. It is not the buffered shared-runtime path.

### Global HOP slicer

The global fragment builder admits append-only UTC event-time HOP partials for COUNT and
compatible DataFusion MIN/MAX calls. It validates canonical partial/bound schemas, grouping and
call types, window strategy, effective persisted/table configuration, synchronous state, and the
available state/backend metric surface before lowering. Unsupported windows, retractions,
DISTINCT/SUM/AVG, floating-point or Boolean grouped extrema, and unsupported properties receive
specific fallback reasons. The legacy direct kernels retain their separately documented scope.
No operator-specific Java driver or key selector is created for the selected global node; the
common native region owns Arrow execution, routing, state, clocks, metrics and checkpoints.

The shared global HOP implementation stores each base slice once using versioned Arrow row keys
for grouping and sortable slice ends. Flink BinaryRow hashing independently selects the key group.
Both the ordered in-memory backend and RocksDB batch reads and writes at incoming Arrow batch
boundaries. DataFusion grouped accumulators merge COUNT and compatible append-only extrema on
input and when a window fires. Firing reads at most 4,096 requested slice keys per page and emits
at most 1,024 windows per pull. One timestamp is drained at a time so timers created during firing
run before later windows delete their slices. Flink's extra empty-window timer is preserved.

For attached HOP partials, the same executor follows Flink's `WindowedSliceAssigner`: one
namespace is stored and read per window, the start is the end minus the planned size, and the
window fires and expires once without scheduling a follow-up empty window. Input bounds must
match that fixed-size contract. The executor does not expand an attached partial into overlapping
HOP windows. Both layouts use DataFusion grouped merging and the same Arrow ownership, batched
state and checkpoint interfaces; the snapshot fingerprint prevents restoring one layout as the other.

A late partial remains eligible until its last overlapping window fires; the late-drop counter
increments once only when the complete input partial is dropped. Unlike Flink's additional global
raw-row buffer, this implementation merges each incoming Arrow batch directly into slice state.
That adaptation follows the native batch and batched-state contract while preserving watermark
emission boundaries. The global buffer emits no intermediate partial rows. The shared metric
channel publishes Flink's `numLateRecordsDropped` Counter, its `lateRecordsDroppedRate` MeterView,
and `watermarkLatency` Long Gauge. The latency gauge reads Flink's processing clock on demand,
returns zero for a negative timer watermark, and otherwise preserves the signed clock difference.
Counter/meter values are updated with one batched metric sample at the native plan edge.
Final selected-topology metrics and Flink-managed recovery remain admission requirements.

The timer index stays in admitted native memory and is serialized at Flink's canonical or physical
checkpoint boundary, instead of being rewritten after every input batch. Snapshot markers
fingerprint the window contract and pin the slice/Arrow encoding. Restore rejects expanded-window
state: original slices cannot generally be recovered from merged extrema. `NativeStateBindings`
protocol 3 carries Flink's restored union-operator watermark separately from keyed snapshots,
even when keyed state contains no entries. The shared Java region restores that clock from Flink
union operator state before constructing native bindings or importing keyed snapshots. Each fused
window uses its stable plan-node identity to namespace the union state, since several original
Flink operators share one lifecycle owner. Rescaling takes the minimum of every restored subtask's
clock, including `Long.MIN_VALUE`. After native output drains successfully, watermark propagation
updates the checkpointed clock. Replayed older watermarks forward the restored clock, matching
Flink's `WindowAggOperator`, while input gauges observe the actual arrival. Direct restores with
missing clocks, unsupported versions and clocks attached to other operator families are rejected.
Versions 1 and 2 remain valid for their existing contracts.

The shared execution adapter preserves Arrow ownership through adjacent Calc stages and attaches
timestamp-less INSERT metadata without re-admitting or copying payload buffers. Watermark output
drains before control propagation. EOF/end-input alone does not fire windows; checkpoint
pre-barriers do not emit rows. Invalid RowKinds and cancelled invocations require recovery. New
timers use batch admission, and fired keys retain their memory credit through the callback.

### Validation and remaining admission work

A selected-graph topology test places Calc stages on both sides of the global window and verifies
one keyed Arrow runtime, original physical IDs/names/UIDs, a single external source translation,
and no Java transformation for an internal native stage. The generated global and attached parity
fixtures build their plans through the same public global fragment builder.

Direct Java tests compare the SQL-generated Flink local slicer with the native tree for generated
control sequences and every partial from a 180,000-row pressure fixture. Direct global HOP COUNT
tests compare complete serialized changelog records at each input/control boundary on both
backends, including nullable keys, negative times, late inputs, large watermark jumps and restore
before replayed input. Attached MAX/COUNT tests use the SQL-generated Flink attached stage and
cover generated partials, extreme BIGINT values, late inputs and restored clocks on both backends.
They verify timestamp/RowKind metadata and logical I/O counts for each
native stage. Generated shared and attached window tests also discover and compare the complete
default Flink global-window metric surface, including counter and meter types, rate behavior,
clock changes without native invocations, pre-barriers, and terminal paths. Timer ties between independent keys are compared without imposing an order Flink
does not guarantee.

Native tests additionally cover memory/RocksDB restore, 1→2→1 rescaling, physical RocksDB
checkpoints, memory denial and cancellation, and a 5,000-slice window read in bounded pages.
Storage instrumentation verifies one retained value per slice and input writes independent of
the growing timer index. Shared Java region tests additionally compare generated replayed data
and watermark output with Flink after canonical, aligned and unaligned operator snapshots, on
both backends, including canonical backend switching. Real Flink union-state repartitioning tests
cover 2→1 clock restore with no live window entries. A mailbox-task matrix also exercises aligned
and unaligned checkpoints with two real Flink input channels. It captures generated Arrow IPC
partials between their barriers, restores them through Flink's channel-state reader, and compares
complete changelogs and watermarks with Flink on both backends. Cases include fully late and
partially late input, shared/attached namespaces, and an older replayed watermark after restore.
The test seam supplies network capture for Flink's test channel; checkpoint handles, serialization,
state writing/reading, routing, and the Arrow-to-RowData sink boundary use their real implementations.
These direct-region checks still leave production planner admission outstanding. Remaining
Q5 requirements include:

- Local-window fragment and resource binding, including the original local memory share.
- Ownership of the reused aggregate's two outputs, without duplicating computation or disabling
  Flink reuse.
- Final physical-topology metric and checkpoint/replay contracts, including optional state/backend
  metric settings, latency scopes, and recovery of the complete selected multi-stage topology.
- Ordinary whole-plan admission, followed by release benchmarks and profiling on both backends.

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
Nexmark Q5 at the kernel level. The shared attached-HOP partial path above now has direct fused
composition coverage; ordinary admission remains gated.
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

---
title: Window aggregation
description: Acceleration coverage and fallback behavior for Flink SQL Window aggregation.
sidebar:
  order: 7
---

**Current status:** Partial acceleration through ordinary whole-plan selection. Verified two-phase,
append-only UTC event-time TUMBLE/HOP windows use DataFusion grouped COUNT/MIN/MAX with BIGINT results
and arguments, BIGINT/INTEGER grouping keys (or no keys), synchronous state and mini-batch disabled.
DISTINCT-only TUMBLE also accepts nullable BIGINT/INTEGER/VARCHAR grouping keys, including
composite keys, under the same execution settings. Single-stage SESSION supports unfiltered COUNT(*)
with one nullable or non-null BIGINT partition key and TIMESTAMP(3) event time, on both backends.
Other window families retain whole-plan fallback under the
[architecture admission requirements](/StreamFusion/development/architecture-admission/).
Both in-memory and supported default RocksDB state use the common native runtime.

TUMBLE uses one unshared slice per window: a watermark at end minus one emits it once and removes
its state, without HOP's follow-up timers. The local DataFusion buffer retains the same Flink
pressure and checkpoint flush boundaries. Generated two-phase SQL comparisons include all-null
COUNT/MIN/MAX windows, grouped and ungrouped layouts, and both backends. The common metric,
canonical restore/backend-switch/rescaling and aligned/unaligned channel-replay matrices also run
with TUMBLE. The [Q7 release comparison](/StreamFusion/benchmarks/q7-rowdata/) records its
verified query path, measurements and managed-memory capacity limit.

**Retained implementation scope:** Partial implementation for native `TUMBLE`, `HOP`, `CUMULATE`, and `SESSION`
aggregation, including Flink's legacy group-window physical node.

## SQL example

```sql
SELECT window_start, bidder, COUNT(*)
FROM TABLE(HOP(TABLE bid, DESCRIPTOR(dateTime), INTERVAL '2' SECOND, INTERVAL '10' SECOND))
GROUP BY window_start, window_end, bidder;
```

## SESSION COUNT execution

Ordinary whole-plan selection admits append-only SESSION with one BIGINT partition key,
unfiltered COUNT(*), positive fixed gaps, and TIMESTAMP(3) event time without time zone. Keys may
be null. Supported properties are window start, end and rowtime. Synchronous state and disabled
mini-batching are required; other grouping shapes, aggregate calls, filtered counts, retractions,
TIMESTAMP_LTZ, processing time and unsupported state/metric configurations fall back with a reason.
No SQL query name or particular gap value is special-cased.

The shared native window runtime uses the same Arrow tree and control/ownership lifecycle as
adjacent Calc stages, preserving each original Flink physical metric identity. Controlled
Flink-generated changelog and complete registered metric-surface tests run on both backends.
Generated ordinary SQL tests also cover nullable keys, negative timestamps, shuffled arrivals,
different gaps and window rowtime output. Official Q11 also matches the complete Flink result
changelog for 10,000 generated events at parallelism one/four on both backends, through ordinary
selection with positive native plan/Calc activity. These tests are not performance measurements.
The separate [Q11 release report](/StreamFusion/benchmarks/q11-rowdata/) compares one- and
ten-million-event workloads and longer mixed profiles on both backends. Ten-million-event median
throughput ratios are 1.181× Flink in memory and 2.001× on RocksDB, with substantial Flink RocksDB
dispersion; the report retains all forks and the earlier optimization baselines.

The kernel processes arrivals in input order to assign Flink merging namespaces, then uses
DataFusion grouped aggregate update/merge kernels over Arrow slices. It does not sort arrivals
before the lateness decision or apply COUNT updates in a handwritten row loop. Namespace lookups
use an ordered interval map; merging retains contribution ordinals for vectorized computation.
Null rowtime and overflowing session endpoints fail before state access.

Persisted entries use a length-framed partition prefix and Arrow 59 encoded session end. Flink's
BinaryRow identity still determines the key group. Each interval has its own `SFSN` version-one
accumulator value; input events and whole partition lists are not retained. A batch reads relevant
ordered ranges for distinct touched keys before computation and writes only changed intervals in
one atomic batch. Both memory and RocksDB support the same index. A native regression updates one
session among 10,000 disjoint sessions with 64 events: one range read, at most one 256-entry page,
and one write batch containing under 256 key/value bytes. This is state-access evidence, not a
throughput benchmark. Timer state is serialized at checkpoint boundaries.

Coarse batch, decoded-state, retained-state and output reservations use Flink's existing allowance.
Decoded interval workspace grows in 64 KiB chunks, avoiding a host budget call for every small
partition page. If that optional headroom is denied, admission retries the exact required size;
it does not reject a workload merely because the next chunk does not fit. The workspace is released
at the batch boundary. A regression loads 1,024 distinct existing partitions on each backend with
fewer than 64 host growth calls across workspace, state and timers, and verifies exact-fit admission.
Admission failure poisons the invocation so it must recover through a fresh context; no partial
session computation can be resumed. Boundary tests retain output credit after context close and
verify early denial before state access. Canonical native tests switch backends while restoring
Flink's operator watermark. Legacy `SFWS`/`SFWI` append-only snapshots migrate to ordered entries
only after their indexes, accumulators and timers agree; retained retraction-event state is rejected.
Generated SESSION recovery tests now compare exact changelog bytes with a SQL-generated Flink
operator across canonical backend switches, aligned and unaligned checkpoints, and key-group
rescaling from one to two subtasks and back. Nullable keys route through Arrow IPC; live sessions
accept older bridging events after restore. RocksDB checkpoints also verify incremental SST reuse.
A separate real-barrier test captures and replays Arrow IPC channel state once, comparing data and
watermark bytes on both backends. Other native aggregate-call variants do not yet carry the
complete SESSION conformance evidence established for COUNT and remain gated.

## Processing-time window contract

Processing-time TVF aggregation remains whole-plan Flink fallback. EXPLAIN reports the missing
shared processing-time planner resource binding and production parity contract. The logical `PROCTIME()`
Calc slot lowers to DataFusion's typed null, matching Flink's generated placeholder; this does
not read a clock or admit the window. `PROCTIME_MATERIALIZE` still reports a clock-lifecycle fallback.
Legacy processing-time shape folding does not hide rejected inputs. A logical time attribute
must not be replaced with a batch timestamp.

Per-record clock transport and Flink-owned timer scheduling are implemented as shared-plan
prerequisites. The reusable DataFusion grouped buffer now also supports clock-driven TUMBLE
assignment and Flink's processing-time flush progression. It leaves new records buffered at
repeated/older timer timestamps and flushes them before checkpointing. SQL-generated Flink
oracles on both backends show that a timer with no published state can emit COUNT zero in this
case; eager state accumulation or suppressing that output would change the changelog.

A reusable native TUMBLE COUNT(*) component now combines this buffer with the existing ordered
slice store and DataFusion partial merger. Raw Arrow arrivals register absolute processing-time
timers before publishing accumulators; a later buffer flush cannot recreate an already-fired
timer. State reads/writes are batched for each bounded partial flush and timer frontier on both
backends. Output is bounded to 1,024 timers per batch, and retained state/buffers and growing
workspaces use Flink reservations. The persisted marker identifies the original raw-input plan,
including columns omitted from the partial layout. Snapshots reject unflushed updates.

Native component tests cover the Flink-oracle clock cases, watermark/EOF behavior, memory denial,
nullable/global keys, both backends and cross-backend timer restore while rescaling one owner to two.
The shared factory now binds this component in the native tree and shared-region execution paths.
Task-resource protocol 2 supplies the original Flink buffer share and page size before keyed state
construction. Missing capacity or a legacy processing-time resource version fails transactionally
before opening the backend. The factory consumes its negotiated per-record Arrow clock, exposes
absolute deadlines to the existing Flink scheduler, and removes clock metadata before Arrow output.
A shared-region test verifies that two exits share the same result buffers and one timer owner.
The original-resource pass now captures single-stage processing-time buffer owners and carries
their stable identities into protocol 2. Tests compare their capacity/page bytes with generated
Flink job graphs on both backends, including weighted boundaries and distinct slot-sharing groups.
These resource tests do not yet establish ordinary selected-plan admission.

Generated Java tests run the SQL-created Flink operator and actual shared native factory through
identical clocks with nullable keys, varying batch sizes and one-/ten-/37-second windows. They
compare complete changelog/control bytes and the entire registered metric surface, including
logical-record counters, meters and watermark latency. Direct C Data and Arrow IPC input, backward
and repeated timers, terminal watermark/finish, and pending-timer checkpoint restore pass on both
backends. Capacity-pressure tests match Flink's published/remaining counts over 300,000 records
at two Arrow batch sizes using the original buffer share. Generated tests also cover one-to-two-to-one
rescaling, canonical backend switches, aligned/unaligned keyed snapshots and incremental RocksDB
SST reuse. Real task/network tests capture and replay Arrow IPC after both barrier modes: records
replayed in a later processing-time window use the restored task clock, while checkpointed records
keep their absolute timers. Complete changelog bytes match Flink, ignoring only its unspecified
order among independent keys at the same timer deadline. Ordinary planner resource binding and
production parity remain pending; Q12 is not admitted.

The SQL-generated Flink reference tests use explicit UTC clocks, nullable keys, generated counts
and one-/ten-/37-second windows. They verify that the window samples its own clock even when the
physical PROCTIME slot is null, only processing-time timers emit results, and pending timers survive
both-backend checkpoint restore. A terminal watermark and bounded finish leave an open processing-time
window un-emitted. These reference tests define the controlled-clock contract used by the native parity fixtures;
they do not establish ordinary planner admission. Non-UTC clock/zone behavior needs its own
proof. Q12's bounded blackhole output can therefore be empty or incomplete depending on wall-clock
alignment and cannot by itself establish result parity or acceleration.

## Q5 checkpoint

Q5's local/global HOP COUNT, attached MAX/COUNT and binary join now use ordinary planner
selection. Its reused global aggregate has one native owner and two Arrow exits; one branch
continues through Calc and local MAX in the same native plan. Original Flink resource shares,
state identity, metrics and network boundaries are retained. Generated SQL and channel recovery
coverage is described below. The [Q5 release comparison](/StreamFusion/benchmarks/q5-rowdata/)
records one-million-event measurements, separate longer profiles and larger-workload memory
limits on the verified plan. The opt-in official Nexmark test compares 10,000
input events at parallelism one and four on both backends: complete collected changelog bytes,
materialized results, ordinary acceleration and native plan activity. It also checks that the
retained standalone local-window JNI path receives no batches.

### Local buffer

The buffered local implementation retains DataFusion `GroupsAccumulator` vectors across Arrow
batches. It probes Arrow row keys by reference and copies retained keys only for new groups.
Its hash table stores ordinals into the first-appearance key vector, so it does not duplicate
retained keys. Batch reservations admit replacement index buffers only when the incoming batch
can cross their capacity, together with conservative DataFusion vector growth and batch scratch.
A 100,000-distinct-key regression fits a 16 MiB Flink share with both 1,024-row and 16,384-row
inputs, preserves partial order and duplicate counts across rehashes, and returns all credit
after flushing. The previous duplicate-key index
and blanket replacement reservation exhausted that same allowance before reaching the flush.
The incoming Arrow batch and its encoded keys remain retained while DataFusion consumes
zero-copy slices of at most 2,048 rows. Scratch and index-growth reservations are renewed at
those compute boundaries; finishing a slice does not emit a partial or add a JNI batch crossing.
Flink-compatible watermark, checkpoint pre-barrier and memory-pressure boundaries flush partials
in first-appearance order, including future slices when a trigger flushes the buffer. Outputs are
timestamp-less INSERT partials, limited to 2,048 rows per pull. Invocation EOF and end-input alone
do not flush; a terminal watermark uses the normal event-time path.

`NativeTaskBindings` v1/v2 binds each local stage to the original Flink operator's resolved memory
share and page size before lowering. A capacity model matches Flink's `WindowBytesMultiMap`
geometry without constructing RowData. This bookkeeping preserves observable pressure-flush
boundaries; actual native buffers and retained state use coarse Flink memory reservations.
The buffered subset requires UTC, TIMESTAMP(3) time columns, verified Flink row geometry
and compatible append-only DataFusion aggregates. Direct event-time columns may be declared
nullable: a batch containing an actual null rowtime fails before changing grouped state, with
Flink's `RowTime field should not be null` error. The bitmap check runs once per batch.
Attached window ends must remain non-null.

The local fragment also supports VARCHAR input and grouping columns. It reads UTF-8 lengths
directly from Arrow: Flink embeds at most seven bytes in the fixed word and rounds longer values
to eight-byte variable storage. Null strings add no variable bytes. Key and input row sizes feed
the existing page model without transposing or serializing rows. Generated Flink local-operator
comparisons cover nullable, empty, Unicode, seven/eight-byte and wider strings, TUMBLE/HOP controls,
checkpoint pre-barriers and pressure flushes across Arrow batch sizes. Native checks cover sliced
inputs, allocation peaks, output ownership and wide-key output under a constrained share.
Variable key bytes have coarse encoding/retention allowances; output chunks include their actual
encoded key lengths. Ordinary VARCHAR grouping is admitted for DISTINCT-only TUMBLE; VARCHAR
COUNT/MIN/MAX and DISTINCT-only HOP retain whole-plan fallback pending equivalent verification.

Global DISTINCT-only TUMBLE has generated SQL and fragment-level changelog and complete registered
metric-surface comparisons on both backends for nullable BIGINT and composite BIGINT/VARCHAR
keys. The existing DataFusion grouped row-count state represents group presence without adding a
SQL aggregate column. Canonical restore switches between backends; aligned and unaligned
checkpoints retain live windows and restored clocks. Rescaling exercises one-to-two-to-one
parallelism through Arrow IPC key-group routing. Mailbox tests capture serialized in-flight
Arrow frames at real barriers and replay them after restore, including late input and duplicate
partials. The shared COUNT/MIN/MAX channel suites also guard the common control machinery.
Official Q8 now passes ordinary selection and complete collected/materialized parity for
10,000 events at parallelism one and four on both backends, with positive native plan activity
and zero standalone local-window JNI batches. The [Q8 release comparison](/StreamFusion/benchmarks/q8-rowdata/)
records measurements at one and ten million events and separate longer profiles, including its
in-memory median regression and larger RocksDB median improvement.

Global input admission includes logical grouping-column spans before allocating Arrow row keys,
state mutations or timer keys. A shared IPC parent is still owned by its producer. The grouping
index stores ordinals into one owned key vector and borrows the Arrow row encoding. Wide nullable
Unicode/composite-key tests check allocation peaks, sliced duplicate inputs, cross-backend restore,
output bytes, complete credit return and early budget denial without state mutation.
Flink's `TimerHeapInternalTimer.comparePriorityTo` compares timestamps only. Different keys firing
at the same window end therefore have no defined relative order. The DISTINCT fixture compares
their complete serialized records as a multiset within that tied end; it preserves window-end
order and every control boundary, RowKind, record envelope and duplicate. Separate tests verify
that normalization cannot hide changed records, multiplicity or control/window ordering.

The shared local fragment builder supports direct and attached HOP COUNT/MIN/MAX for that
verified subset. Attached windows follow Flink's `WindowedSliceAssigner`: only the attached end
is consumed, and the start is derived as end minus the full window size. A start column can be
pruned or retained without changing grouping or assignment. The versioned local protobuf accepts
end-only UTC TUMBLE/HOP attachment; an explicit pair retains the legacy supplied-bound contract.
Start-only, end-only CUMULATE, and end-only non-UTC contracts are rejected. The shared runtime factory now resolves local capacities at startup from serialized original
Flink resource weights, slot-group totals and use cases, independently of the fused runtime's
allocation allowance. An original-resource graph calculator now derives that metadata from the complete pipeline,
with Flink job-graph comparisons for reuse, weighted boundaries, slot groups and operators added
after SQL translation. A complete-pipeline hook now resolves graph-specific factory copies before
JobGraph serialization, preserving independent capacities across repeated pipeline builds. Selected
local-window nodes now attach that calculator automatically and produce native fragments. Conversion
preserves the original local node and exchange identities. A source-side local region consumes Arrow
batches directly; an actual Flink exchange still carries IPC frames decoded once at the receiving
native edge. An attached local stage after a global window joins the same native tree.

The retained legacy local handle still flushes per Arrow batch. Its reusable integer COUNT/SUM/AVG
and append-only MIN/MAX computation uses DataFusion, with ordered Flink adapters for retractions
and incompatible numeric semantics. It is not the buffered shared-runtime path.

Generated SQL comparisons now run direct and attached HOP graphs with three input seeds on each
backend and compare the complete collected changelog bytes against Flink. These use explicit test
selection through the ordinary planner, without a test admission bypass. Topology checks verify stable
original identities, original memory fractions including weighted source/sink boundaries, serialized
factories, and direct Arrow input to source-side local regions. The shared single-input runtime also
passes the pressure/control parity fixture; the existing aligned/unaligned channel and restore/rescale
fixtures cover its common control path. These controlled fixtures supplement the separate full-query
integration and release comparison.

The buffered local path reserves compact DataFusion group vectors, its ordered key index,
and batch scratch together. It borrows incoming Arrow arrays and admits serialized partial
output separately when Flink's buffer or control event triggers a flush. It does not reserve
the eager compatibility path's hypothetical input copies and simultaneous output. A large
flush chooses Arrow chunk sizes within the currently available managed-memory share while
preserving partial order and the original Flink flush boundary. If even one output row cannot
be admitted, execution fails for recovery. A constrained-memory regression covers both hot
and distinct keys in a 1 MiB share; no deployment option or memory bypass is added.

The selected-graph path now runs a reused global COUNT through one native owner with its
Calc and attached local MAX consumer. The raw COUNT and local partial exits use separate Arrow
outputs feeding the existing exchanges. Generated SQL comparisons with the benchmark's binary
MultiJoin setting match Flink on both backends, for 6-second/10-second HOP windows and parallelism
one/two. These fixtures require ordinary whole-plan admission. The release comparison uses the
same production path with the official Nexmark RowData source and Flink blackhole sink.

Global slice merging reserves workspace from the logical partial bytes it decodes and the
number of rows it processes. Incoming Arrow buffers retain their producer's accounting; a small
slice does not reserve its entire parent allocation again. A constrained-memory regression on
both backends retains a 16,384-row parent, merges a 32-row slice, and verifies every overlapping
window result plus the producer's lifetime after the window closes.

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
control sequences and every partial from a 180,000-row pressure fixture, using the public fragment
builder. Generated grouped/ungrouped attached MAX/COUNT tests use Flink's SQL-generated local
stage with retained start values that assignment must ignore, and compare every serialized partial, timestamp/RowKind and
logical I/O counter across input batches, watermarks and checkpoint pre-barriers. Native tests
also verify signed-long wrapping when deriving an attached start. Shared runtime startup tests
serialize the actual operator factory, resolve the local capacity from Flink resources, and compare
pressure/control changelogs for local-only and local/global trees on both backends. Direct global HOP COUNT
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
The same network matrix now also covers both exits of a shared global window, including an
attached local MAX/COUNT on the second branch with its original buffer allowance. Separate
production sink adapters and network writers capture each branch. Both exit changelogs,
watermarks and checkpoint barriers match Flink after aligned and unaligned input-channel replay
on each backend. The local Flink oracle receives the same global output and checkpoint flushes.

These checks support ordinary admission of the verified subset. Full-query release measurements
and mixed JVM/native profiles are recorded for both backends on the Q5 comparison page.
Unsupported state/backend metric settings and sampled latency routing retain precise fallback
reasons; a measured Q5 result does not admit the remaining window families.

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
composition coverage and ordinary admission for the verified subset above.
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
transitive merging and keep Flink's merged namespace when a bridging row retracts. Lateness is checked
against the merged session end: an older event whose own window has expired is accepted when it
joins a live session, including inclusive boundary contact. Arrival order is preserved within each
Arrow batch, so a later event cannot retroactively rescue an earlier dropped event. Generated
comparisons against Flink's SQL-created session operator cover these cases and the late-record
counter on both backends. This retained implementation is broader than production admission;
only the shared SESSION COUNT subset described above is selected.

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

The retained compatibility local kernel is state-free across Arrow batches and is not the admitted
buffered implementation described above. Its
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

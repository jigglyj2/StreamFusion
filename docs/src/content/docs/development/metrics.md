---
title: Metric compatibility
description: How accelerated operators preserve Flink's metric contract.
---

StreamFusion treats metrics as part of Flink runtime compatibility. An accelerated
physical operator publishes every metric that its Flink counterpart publishes with the
same name, type, unit, scope, lifecycle, and meaning. Given the same records, control
events, checkpoints, and terminal path, deterministic counters and values must also
match. Runtime-dependent timings, rates, and physical byte counts keep Flink's
definitions but measure the actual accelerated execution; they are not forged to look
like an unaccelerated run. A metric whose semantics cannot be reproduced exactly is a
fallback condition rather than a reason to silently omit or reinterpret it.

Flink's operator runtime continues to own the standard record-rate, watermark, latency,
busy-time, idle-time, and backpressure metrics. StreamFusion operators emit through the
normal Flink `Output` and retain Flink's input lifecycle, so these metrics keep their
normal registration and update paths. Internal Arrow batches do not count as records.
The native exchange similarly corrects its internal IPC-frame counts back to the logical
row counts that the corresponding Flink network edge reports.

Production eligibility is controlled by [architecture admission](/StreamFusion/development/architecture-admission/).
Persistent stateful implementations and native combinations other than admitted Calc/UNION trees are
currently gated. The table below describes retained implementation behavior exercised by direct
tests; it does not establish complete metric parity or production eligibility for those paths.

For admitted fused Calc/UNION trees, Java reads cumulative `(plan_node_id, input_rows, output_rows)`
snapshots after draining the native stream. Each stage counts logical records once, including
filter and projection implementations with multiple DataFusion nodes. All physical stages, including
the output/root stage, publish
standard Flink IO counters/meters, watermark gauges, and latency statistics under operator scopes.
Missing or duplicate plan IDs and malformed or decreasing metric snapshots are rejected.
The shared runtime owner is a separate transport/lifecycle scope: its input count covers decoded
external records and its output count covers records emitted at the region edge. It no longer
reports the root stage's post-filter input count as the runtime input count. Physical-stage counts
come from the native metric tree. SQL UNION is a Flink wiring transformation rather than an operator: its native counts remain
in diagnostic snapshots, without an invented Flink UNION operator scope. UNION channel watermarks
use Flink's network valve; real operator stages keep their own watermark/latency semantics.

Virtual physical stages observe input and output watermarks independently. Each input gauge is
updated on arrival at that physical stage; `currentInputWatermark` uses Flink's minimum over those
gauges, including an idle input's last value. `currentOutputWatermark` records only emitted
watermarks. The input gauge therefore need not advance when idleness allows the output to advance.
Multi-input native stages also publish Flink's `currentInput1Watermark`,
`currentInput2Watermark`, and subsequent per-port gauges, using the same input frontiers.
The shared metric tree derives these from physical arity, not a join-specific registration list.
Generated binary-inner-MultiJoin tests compare the actual Flink operator and generated Calc
against the common native keyed region on memory and RocksDB: full default metric surfaces,
logical counts, stage identities, latency histograms, all four RowKinds, cleared record timestamps,
idle/reactivation and pre-barrier events. A 5,000-row multiplicity exercises output beyond one
batch while requiring native batches to stay within 4,096 rows and memory credit to release on close.
This scoped conformance is not a production-admission or benchmark claim.
The control and metric trees derive physical input arity from the same protobuf traversal rather
than an operator-pair table. Generated tests compare all four stages of nested binary/unary trees
with real Flink control operators and Flink's gauge implementations across idle/reactivation events.
This does not establish complete planner-derived scope metadata, state-specific metrics,
or timer-driven operators' watermark semantics; those remain admission requirements.

On execution failure the shared runtimes attempt to publish the available native stage snapshot
without replacing the original exception. A failure before physical lowering has no stage snapshot;
an additional metric-read failure is suppressed onto the execution error. Runtime I/O adjustments
occur at input receipt/output emission, so a downstream exception does not leave batch-count units
in those counters. Focused unary and multi-input tests inject sink failures and verify the separate
boundary/root-stage counts. This describes actual vectorized records received/produced, not a claim
that every terminal-path metric of every Flink row-at-a-time operator has been verified.

`StreamFusionPlannerFactory.nativePlanBatchCount()` is a process-local diagnostic counting
successfully opened shared native-plan streams, including empty inputs. It is neither a logical
record count nor a per-stage execution count, and is reset by the existing benchmark/test metric
reset. Legacy per-family batch counters do not count stages nested inside this shared path.

The retained compatibility streaming regular-join/Calc region uses the same native identity tree. Its Java
lifecycle owner publishes Join I/O (including rows later filtered by Calc), while each output
Calc has its own Flink operator scope, watermark/latency metrics, and logical I/O counts.
Native Calc invocation diagnostics count incoming batches even when a Calc filters away its
entire input. Generated tests check stage record counts and the complete serialized changelog
multiset per input batch against Flink on both native backends. Complete metric-surface and
allocation admission remain prerequisites for production selection.

Selected regular-join, deduplication, and synchronous aggregation fragments now bind to the generic keyed-region owner instead
of that compatibility owner. The shared native identity tree supplies logical stage counts, but
complete state-specific metric/lifecycle publication in the shared owner remains unfinished;
the planner's persistent-state admission gate is not removed by this migration.

A synchronous aggregation test now enumerates the complete registered operator metric subtree
of Flink's SQL-generated handler and the shared native aggregate stage on both default memory
and RocksDB configurations. It compares names/types, deterministic counters and gauges, and
rate-meter counts/implementation while also comparing serialized changelogs for insertions and
retractions. The reference harness supplies Flink's task/chain-owned watermark gauges and counting
wrapper updates, which a bare operator harness does not install; output watermarks advance only
on actually emitted marks. This establishes the default synchronous operator surface, not optional
state-latency/RocksDB metric configurations, task-level latency scopes, or every failure/terminal path.

The common region also discovers a versioned native gauge schema once at open. Descriptors carry
physical plan-node identities, metric subgroup paths, names and scalar types; one packed snapshot
updates all gauges after each data/control drain, including a best-effort sample after failure.
Reporters read a published Java snapshot without entering JNI or individual Rust operators.
Integer, Long and Double values preserve their Java types (Double uses raw IEEE-754 bits).
Invalid versions, unknown stages, duplicate/reserved metric names and malformed samples are rejected.
Native schema/value storage is admitted before allocation and released after the JNI copy.
Version 2 of this same scalar channel adds typed counters and clock-based gauges; version 1
remains gauge-only. A counter descriptor may request a Flink `MeterView` over that counter,
using Flink's default rate interval and lifecycle. Counter updates apply batched deltas with
Flink's long-overflow semantics. Watermark-latency descriptors carry the native timer watermark;
the reporter reads Flink's processing-time service and computes zero for a negative watermark,
otherwise processing time minus watermark. The value can be negative and changes while the
operator receives no input. No additional JNI invocation or per-record metric callback is added.
Typed values require version 2 and INT64 samples. Meter aliases share duplicate/reserved-name
validation, and clock-based descriptors require a Flink processing clock before registration.

Shared HOP and attached-HOP global windows expose `numLateRecordsDropped` as a Counter,
`lateRecordsDroppedRate` as its `MeterView`, and `watermarkLatency` as a Long Gauge. Generated
parity tests discover and compare the complete default Flink global-window metric surface,
including logical I/O, changelogs, pre-barriers, end-input, and live processing-clock changes on
both state backends. Restore tests also compare these three window-specific metrics after
canonical, aligned/unaligned operator snapshots and union-clock rescaling. Counters restart
with a new operator; the watermark gauge uses the restored clock. Optional state/backend metric
settings and final selected-topology latency scopes remain separate admission requirements.
This is common metric-tree plumbing, not an operator-family or fusion-pair dispatch mechanism.

Raw mini-batch aggregation supplies `bundleSize` (Integer) and `bundleRatio` (Double), matching
Flink's pending element count and elements-per-pending-key ratio. Generated tests on both backends
compare the complete default registered operator metric subtree and ordered serialized changelogs
at Arrow/control boundaries across three count triggers, six seeds, empty arrivals, nullable keys,
all four RowKinds, watermark/checkpoint flushes and completion. Separate recovery tests compare
bundle gauges after canonical cross-backend restore and aligned/unaligned state restoration.
Native tests verify independent gauges for adjacent aggregates, snapshot allocation release and
the zeroed count after a cancelled flush. This is batch-edge observability, not per-row sampling
inside a vectorized operation. An injected downstream failure after the first bounded flush
batch now matches Flink after the same 2,048 emitted records on both backends: partial serialized
changelog, the complete default metric surface, and absence of an output watermark are compared.
The common runtime preserves the sink exception, requires recovery and releases native memory
when closed without finish. This does not prove arbitrary row-interior failure equivalence.
Shared aggregate fragment admission no longer rejects raw mini-batching wholesale. It explicitly
rejects enabled keyed-state latency histograms and enabled RocksDB property/statistics metrics;
Flink's option resolution is used, including the deprecated keyed-state latency option alias.
Full production persistent-memory admission remains gated.

The task-local aggregate factory also publishes `bundleSize` and `bundleRatio` through that common
gauge schema, without inventing keyed-state metrics for a replayable buffer. Native composed-tree
tests check independent local/global bundle values and each Calc/aggregate stage's logical counts
through bounded control drains on both global backends. The local Java fragment now uses the common
one-input Arrow owner, whose gauge and control plumbing shares the multi-input scheduler. A generated
runtime matrix compares the complete default metric surface with Flink's SQL-generated local
`MapBundleOperator`: three triggers, nullable Unicode keys/BIGINT payloads, all four RowKinds, empty
arrivals, watermarks, pre-barriers and finish. Harness counter updates represent Flink's task counting
wrappers, not fabricated accelerated values. Two-phase conversion also retains the original local
physical ID/name/UID. Full-type and arbitrary failure-path parity remain outside this coverage.

The operator-specific implementation audit is:

| StreamFusion operator | Flink reference | Operator-specific metric handling |
| --- | --- | --- |
| Calc | generated Flink Calc operator | No additional reference metrics; standard Flink metrics are retained. |
| Expand | generated Flink Expand operator | No additional reference metrics; standard Flink metrics are retained. |
| Array/Map/Multiset Unnest | generated Flink Correlate operator | No additional reference metrics; standard Flink metrics are retained. |
| Set-operation Row Replication | generated Flink Correlate operator for `$REPLICATE_ROWS$1` | The reference function adds no operator-specific metric. Standard IO counters are corrected to logical input and repeated output rows; managed-memory gauges cover the gather selection and output. The enclosing set plan retains the native aggregate or semi/anti-join state, changelog, checkpoint, and backend metrics described below. |
| Union All | Flink `UnionStreamOperator` | No additional reference metrics; standard Flink metrics are retained. |
| Values | Flink `ValuesInputFormat` source | No additional reference metrics; standard Flink source metrics are retained. |
| Aligned Window TVF | Flink `AlignedWindowTableFunctionOperator` | Publishes `numNullRowTimeRecordsDropped` and increments it at the same per-record decision point. In a fused Calc → TVF plan the native TVF node counts post-Calc null timestamps and Java propagates the metric after draining each output stream. |
| Session Window TVF | Flink `UnalignedWindowTableFunctionOperator` | Corrects IO counters to logical rows and publishes null/late-row, watermark-latency, state/timer, pending event/processing timer, changelog, checkpoint, restore, and failure diagnostics. |
| Changelog Normalize | Flink `KeyedProcessOperator` / `ProcTimeMiniBatchDeduplicateKeepLastRowFunction` surface for the selected synchronous path | Corrects IO counters to logical rows and publishes state batches, TTL expirations, backend/memory, checkpoint, restore, watermark, changelog, and failure diagnostics. |
| Drop Update Before | Flink `StreamFilter` with `DropUpdateBeforeFunction` | No additional reference metrics; standard Flink metrics are retained. |
| Watermark Assigner | Flink `WatermarkAssignerOperatorFactory` | Uses Flink's generated watermark expression and state machine over Arrow-backed row views, including backpressure-aware idleness and matching lifecycle metrics. |
| Hash/Singleton Exchange | Flink `PartitionTransformation` | Network transport remains Flink-owned; operator/task record counters report logical rows rather than native IPC frames. |
| Group Aggregate | Flink `GroupAggFunction`, `MiniBatchGroupAggFunction`, local/global mini-batch operators, and `MiniBatchIncrementalGroupAggFunction` | The immediate shape has no additional Flink operator counter. One-phase, two-phase, and split-DISTINCT local/incremental/global stages publish Flink's `bundleSize` and `bundleRatio` gauges from their native pending bundles. Bounded local hash aggregation exposes Flink's `memoryUsedSizeInBytes` and `numSpillFiles`; only the final hash phase owns Flink's fallback sorter and therefore adds `spillInBytes`. Standard IO counters are corrected from Arrow batches to logical rows; each stateful stage reports its processor's actual batched state calls rather than inferring calls from Arrow batches, and changelog/checkpoint diagnostics are additive. The incremental stage retains its own transformation identity and `incremental group aggregate` native state/memory identity instead of being folded into the global node. Independent local/global invocation counters make two-phase benchmark execution explicit. |
| Window Aggregate | Flink window aggregate functions and trigger operators | The planner preserves Flink's one- or two-phase shape exactly. Every selected phase publishes its own logical-record IO counters and managed-memory usage. A stateful one-phase or global phase additionally publishes native state, changelog, late-row, event/processing timer, pending-timer, checkpoint, and restore diagnostics at the corresponding state or timer decision; the state-free local phase reports its own batch and allocation activity. |
| OVER Aggregate | Flink streaming and bounded batch OVER functions | Standard logical-record IO plus Flink's `numOfIdsNotFound`/`numOfSortKeysNotFound` or `numLateRecordsDropped`, according to the selected Flink function. Bounded batch OVER retains the absorbed sort's `memoryUsedSizeInBytes`, `numSpillFiles`, and `spillInBytes` metrics while native state, checkpoint, restore, allocation, and invocation diagnostics remain additive in the StreamFusion subgroup. |
| Window Deduplicate | Flink `RowTimeWindowDeduplicateOperator` | Standard IO and watermark metrics remain Flink-owned. `numLateRecordsDropped`, its rate meter, native state/timer counters, pending event-time timers, and checkpoint diagnostics are updated at the equivalent decisions. |
| Window Top-N | Flink `WindowRankOperator` | Standard IO and watermark metrics remain Flink-owned. Late-row, state/timer, pending event-time timer, changelog, output, and checkpoint diagnostics cover the native lifecycle. |
| Non-window Top-N | Flink `AbstractTopNFunction` family | `topn.invalidTopSize`, `topn.cache.hitRate`, and `topn.cache.size` retain Flink names. Comparator calls, state groups loaded/committed/expired, invalid retractions, changelog, state-batch, managed-memory, and checkpoint/restore diagnostics cover the native lifecycle. |
| Temporal Sort | Flink `RowTimeSortOperator` / `ProcTimeSortOperator` | The reference operators add no operator-specific metric, so standard logical IO and watermark metrics are retained. Native state batches, row-kind output, late event-time drops, registrations/firings, pending event/processing timers, backend/memory, checkpoint/restore, and failure diagnostics are additive. |
| Bounded full Sort | Flink `StreamExecSort` / bounded sort operator | Standard logical IO and task metrics are retained. Bounded batch sort gauges report actual native managed memory, spill count, and spilled bytes; bounded-sort rows loaded/committed/emitted, invalid retractions, comparator calls, state batches, backend/memory, checkpoint/restore, and failures are additive. The operator's internal-sort attribute prevents Flink from creating a second runtime sorter. |
| Bounded SortLimit / Rank | Flink `BatchExecSortLimit` and paired local/global Sort/Rank stages | Logical IO counts rows, not Arrow batches. Bounded Rank retains Flink's sort gauges with actual managed-memory use and zero native spill values. Loaded/committed rows or groups, comparator calls, invalid retractions, emitted rows, native invocations, backend/memory, and checkpoint/restore diagnostics are additive. |
| Window Join | Flink `WindowJoinOperator` | Standard two-input IO and watermark metrics remain Flink-owned. Per-side late-row counters/rates, coalesced-watermark latency, join-condition evaluations, state/timer calls, pending event-time timers, checkpoint/restore, and failure diagnostics cover the native lifecycle. |
| Regular Join | Flink `StreamingJoinOperator` / `StreamingSemiAntiJoinOperator` | The eligible timer-free path has no additional Flink operator counters. Standard two-input IO and watermark metrics remain Flink-owned; native state batches, changelog kinds, backend/memory, checkpoint/restore, and failure diagnostics are additive. Zero-valued pending-timer gauges make the timer-free contract explicit. |
| Bounded Hash/Adaptive/Sort-Merge/Nested-Loop Join | Flink bounded hash, sort-merge, or nested-loop join operators | Standard logical two-input IO remains Flink-owned. Hash/adaptive/sort-merge replacements publish `memoryUsedSizeInBytes`, `numSpillFiles`, and `spillInBytes`; memory measures the actual managed native reservation and the spill values remain zero because the native counted-state algorithm does not create Flink sorter/hash-table spill files. Native state batches, changelog kinds, backend/memory, checkpoint/restore, terminal output, and failures remain additive. |
| Interval Join | Flink `TimeIntervalJoin` / `RowTimeIntervalJoin` | Standard two-input IO and coalesced-watermark metrics remain Flink-owned. The native operator publishes logical changelog counts, state batches, timer registrations/deletions/firings, pending event/processing timers, backend/memory, checkpoint/restore, and failure diagnostics under `StreamFusion`; deterministic values are checked against the corresponding Flink transitions. |
| Synchronous Deduplicate | Flink `RowTimeDeduplicateFunction`, `ProcTimeDeduplicateKeepFirstRowFunction`, and `ProcTimeDeduplicateKeepLastRowFunction` | These supported timer-free/no-TTL shapes have no additional Flink operator counter. Standard IO counters are corrected to logical rows. Complete INSERT/UPDATE_BEFORE/UPDATE_AFTER accounting, native state batches, zero-valued timer counters, backend, managed-memory, checkpoint, and restore diagnostics are tested as additive metrics. |

StreamFusion-specific diagnostics are additive and use distinct names; they do not replace Flink
metrics. Native keyed operators publish these metrics under their operator's `StreamFusion` group:

| Area | Metrics |
| --- | --- |
| Processing | `processedBatches`, `processedRows`, `emittedRows`, `processingFailures` |
| Changelog | `emittedInserts`, `emittedUpdateBefores`, `emittedUpdateAfters`, `emittedDeletes` |
| State calls | `stateReadBatches`, `stateWriteBatches`, `rocksDbBackend` |
| Window lifecycle | `lateRecordsDropped`, `timerRegistrations`, `timerDeletions`, `timersFired`, `pendingEventTimeTimers`, `pendingProcessingTimeTimers` |
| Snapshot | `checkpoints`, `alignedCheckpoints`, `unalignedCheckpoints`, `canonicalSavepoints`, `incrementalCheckpoints`, `checkpointBytes`, `checkpointDurationNanos`, `checkpointFailures` |
| Incremental RocksDB | `incrementalUploadedBytes`, `incrementalReusedBytes` |
| Restore | `restores`, `restoreBytes`, `restoreDurationNanos`, `restoreFailures` |
| Native RocksDB memory | `rocksDbSharedManagedMemoryReserved` |

The state-call counters describe the one native multi-get and one atomic mutation batch attempted
for each successfully processed Arrow batch; they are not estimates of individual key lookups.
Checkpoint byte counters describe StreamFusion's native payload. Duration counters accumulate
native snapshot/restore work and incremental upload completion, not the whole distributed Flink
checkpoint. Existing managed-memory gauges expose used, peak, and assigned operator bytes;
`rocksDbSharedManagedMemoryReserved` exposes the process-shared native database lease charged to
Flink's separate state-backend consumer fraction. Fused plans carry stable protobuf
`plan_node_id` values for every internal stage, assigned on the Java side and validated by
Rust. The common planner collector now derives each selected fragment's identity from its original
Flink physical node ID, in a separate positive ID range. Replacement retains that origin without
translating a Flink runtime operator, and keyed-state bindings use the same identity as the metric
tree. The protobuf `metric_name` carries Flink's transformation name, including
`table.exec.simplify-operator-name-enabled` and persisted-node configuration precedence. Internal
stages and the root publish that original name rather than guessing from a native operator enum.
Directly constructed selected nodes use their own identity and configured name. Compatibility
plans without `metric_name` retain the previous generic naming behavior. Extending a region within that physical graph
does not renumber existing stages. Java and Rust reserve explicit IDs before assigning anonymous
synthetic input IDs from the lowest available positive values and reject duplicates or IDs outside
Java's positive metric range. Java walks the protobuf message structure rather than maintaining an
operator-family identity switch, including repeated children without manufacturing absent inputs.
Legacy anonymous plans still receive deterministic traversal IDs. These identities are the metric-tree correlation keys used when a native tree reports
stage metrics, following Comet's `CometMetricNode`/plan-ID model; they are not derived from
batch order or DataFusion display text. Every lowered DataFusion stage is wrapped in a transparent
identity boundary. A metric read for one ID includes the DataFusion nodes implementing that stage
and stops at nested identity boundaries, so a parent's value cannot double count its children.
Java samples cumulative native values after the output stream for the incoming batch has been
drained, then applies only the delta to the corresponding Flink counter. This mirrors Comet's
[`CometMetricNode` metric-tree propagation](https://github.com/apache/datafusion-comet/blob/4897161704b7b8b7dfa909f4bf897c6508b11117/spark/src/main/scala/org/apache/spark/sql/comet/CometMetricNode.scala#L192-L205),
adapted to Flink's metric groups and operator lifecycle.

When the original planner assigns an explicit UID, the protobuf's optional `metric_uid` preserves
it as well. This uses Flink's original transformation metadata, compiled-plan status,
`table.exec.uid.generation`, and `table.exec.uid.format`; persisted node settings take precedence.
An explicitly empty UID remains distinct from no UID and is rejected, as in Flink's JobGraph
validation. Virtual metric groups and latency statistics
derive their `OperatorID` using Flink's own `StreamGraphHasherV2`, and tests compare the resulting
IDs to real Flink job graphs. These IDs no longer depend on which runtime region contains the stage.
Duplicate UIDs among selected native stages reject graph replacement, and duplicate UIDs in a
supplied native metric tree are rejected before registering groups. Nodes producing multiple
transformation UIDs need individual physical-stage metadata before this contract can admit them.

This does not assign the shared runtime owner's checkpoint UID, migrate operator state across
changed region boundaries, or reproduce Flink's topology-generated IDs when no explicit UID exists.
Those cases, full task-scope equivalence, and collision validation against source/sink boundaries
remain part of the outstanding metric/recovery audit.

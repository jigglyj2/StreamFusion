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

The current operator-specific audit is:

| StreamFusion operator | Flink reference | Operator-specific metric handling |
| --- | --- | --- |
| Calc | generated Flink Calc operator | No additional reference metrics; standard Flink metrics are retained. |
| Expand | generated Flink Expand operator | No additional reference metrics; standard Flink metrics are retained. |
| Array/Map/Multiset Unnest | generated Flink Correlate operator | No additional reference metrics; standard Flink metrics are retained. |
| Union All | Flink `UnionStreamOperator` | No additional reference metrics; standard Flink metrics are retained. |
| Values | Flink `ValuesInputFormat` source | No additional reference metrics; standard Flink source metrics are retained. |
| Aligned Window TVF | Flink `AlignedWindowTableFunctionOperator` | Publishes `numNullRowTimeRecordsDropped` and increments it at the same per-record decision point. |
| Session Window TVF | Flink `UnalignedWindowTableFunctionOperator` | Corrects IO counters to logical rows and publishes null/late-row, watermark-latency, state/timer, pending event/processing timer, changelog, checkpoint, restore, and failure diagnostics. |
| Changelog Normalize | Flink `KeyedProcessOperator` / `ProcTimeMiniBatchDeduplicateKeepLastRowFunction` surface for the selected synchronous path | Corrects IO counters to logical rows and publishes state batches, TTL expirations, backend/memory, checkpoint, restore, watermark, changelog, and failure diagnostics. |
| Drop Update Before | Flink `StreamFilter` with `DropUpdateBeforeFunction` | No additional reference metrics; standard Flink metrics are retained. |
| Watermark Assigner | Flink `WatermarkAssignerOperatorFactory` | Uses Flink's generated watermark expression and state machine over Arrow-backed row views, including backpressure-aware idleness and matching lifecycle metrics. |
| Hash/Singleton Exchange | Flink `PartitionTransformation` | Network transport remains Flink-owned; operator/task record counters report logical rows rather than native IPC frames. |
| Group Aggregate | Flink `GroupAggFunction`, `MiniBatchGroupAggFunction`, local/global mini-batch operators, and `MiniBatchIncrementalGroupAggFunction` | The immediate shape has no additional Flink operator counter. One-phase, two-phase, and split-DISTINCT local/incremental/global stages publish Flink's `bundleSize` and `bundleRatio` gauges from their native pending bundles. Standard IO counters are corrected from Arrow batches to logical rows; each stateful stage reports its processor's actual batched state calls rather than inferring calls from Arrow batches, and changelog/checkpoint diagnostics are additive. The incremental stage retains its own transformation identity and `incremental group aggregate` native state/memory identity instead of being folded into the global node. |
| Window Aggregate | Flink window aggregate functions and trigger operators | Standard IO/watermark metrics remain Flink-owned. Native state, changelog, late-row, event/processing timer, pending-timer, and checkpoint diagnostics are additive and updated at the corresponding state or timer decision. |
| Window Deduplicate | Flink `RowTimeWindowDeduplicateOperator` | Standard IO and watermark metrics remain Flink-owned. `numLateRecordsDropped`, its rate meter, native state/timer counters, pending event-time timers, and checkpoint diagnostics are updated at the equivalent decisions. |
| Window Top-N | Flink `WindowRankOperator` | Standard IO and watermark metrics remain Flink-owned. Late-row, state/timer, pending event-time timer, changelog, output, and checkpoint diagnostics cover the native lifecycle. |
| Non-window Top-N | Flink `AbstractTopNFunction` family | `topn.invalidTopSize`, `topn.cache.hitRate`, and `topn.cache.size` retain Flink names. Comparator calls, state groups loaded/committed/expired, invalid retractions, changelog, state-batch, managed-memory, and checkpoint/restore diagnostics cover the native lifecycle. |
| Window Join | Flink `WindowJoinOperator` | Standard two-input IO and watermark metrics remain Flink-owned. Per-side late-row counters/rates, coalesced-watermark latency, join-condition evaluations, state/timer calls, pending event-time timers, checkpoint/restore, and failure diagnostics cover the native lifecycle. |
| Regular Join | Flink `StreamingJoinOperator` / `StreamingSemiAntiJoinOperator` | The eligible timer-free path has no additional Flink operator counters. Standard two-input IO and watermark metrics remain Flink-owned; native state batches, changelog kinds, backend/memory, checkpoint/restore, and failure diagnostics are additive. Zero-valued pending-timer gauges make the timer-free contract explicit. |
| Interval Join | Flink `TimeIntervalJoin` / `RowTimeIntervalJoin` | Standard two-input IO and coalesced-watermark metrics remain Flink-owned. The native operator publishes logical changelog counts, state batches, timer registrations/deletions/firings, pending event/processing timers, backend/memory, checkpoint/restore, and failure diagnostics under `StreamFusion`; deterministic values are checked against the corresponding Flink transitions. |
| Row-time Keep-last Deduplicate | Flink `RowTimeDeduplicateFunction` | Flink has no additional operator counter in the supported insert-only/no-TTL shape. Standard IO counters are corrected to logical rows; native state/changelog/checkpoint diagnostics are additive. |

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
Flink's separate state-backend consumer fraction. As fused plans
gain more stateful DataFusion operators, native plan-node metrics will be mapped back to their
corresponding Java physical nodes using stable protobuf identities, following Comet's metric-tree
model.

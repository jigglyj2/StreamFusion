---
title: Nexmark query checkpoints
description: Query-ordered production admission and performance milestones.
---

Production coverage advances in increasing numbered-query order. Verify earlier admission,
then finish the next query on both state backends before expanding to later queries.
An implemented native kernel is not an unlocked query.

## Current checkpoint

Q3 is the next target. A September 7, 2026 diagnostic run of the existing local build used
10,000 deterministic RowData events, streaming mode, parallelism four, and separate Flink
and StreamFusion JVMs. Each engine ran both backend settings. These were short admission
checks, not warmed-up, alternating-fork performance measurements.

| Query | Memory setting | RocksDB setting | Output records | Sorted changelog hash versus Flink |
| --- | --- | --- | --- | --- |
| Q0 | Accelerated; native activity observed | Accelerated; native activity observed | 9,200 | Matches |
| Q1 | Accelerated; native activity observed | Accelerated; native activity observed | 9,200 | Matches |
| Q2 | Accelerated; native activity observed | Accelerated; native activity observed | 42 | Matches |
| Q3 | Whole-plan Flink fallback | Whole-plan Flink fallback | 62 | Matches, but does not exercise native join |

Q0–Q2 are stateless: their RocksDB-labelled runs establish configuration compatibility, not
RocksDB state performance. The hashes include serialized RowKinds and payloads, sorted to remove
cross-subtask arrival-order differences. They do not establish exact control-event or global
arrival-order parity. Generated controlled-input tests remain required for those contracts.

These observations describe the local working build, including uncommitted shared-runtime
changes, not a released or clean-commit benchmark. Raw diagnostic output is retained locally in
`streamfusion-nexmark-benchmarks/target/validation/query-order/initial-admission.log` and
`flink-reference.log`. Existing performance tables are historical evidence, not proof that the
current planner admits a query.

## Q3 work boundary

The actual RowData Q3 plan uses Flink's binary `StreamExecMultiJoin` for the auction/person join.
The existing semantic lowering can represent that binary shape as the native regular join;
it must not be confused with the separate native multi-way join algorithm. The early architecture
gate now uses the same binary-shape decision as semantic lowering. It retains persistent-memory
and composition restrictions for the binary regular-join path without incorrectly reporting the
multi-way algorithm's separate integration restriction. Genuine multi-way and non-lowerable
binary shapes still require their paged-state/output cursor to join the common execution, metric,
and checkpoint lifecycle. This diagnostic correction does not
unlock Q3 or remove its remaining memory and metric requirements.

The next work is limited to this production path:

1. Preserve shape-aware admission and precise whole-plan fallback for unsupported shapes.
   Do not disable Flink's optimizer to select
   a more convenient benchmark plan or add an admission bypass.
2. Establish the shared two-input region's Flink metric/control parity and memory ownership,
   including direct Arrow handoff, bounded join output and dirty-page state writes. Reuse the
   common execution tree and state lifecycle; do not add a Q3-specific runtime or fusion driver.
3. Verify generated join changelogs, key-group rescaling, canonical cross-backend restore,
   aligned/unaligned recovery and incremental RocksDB checkpoints through that same path.
4. Require normal EXPLAIN acceleration and non-zero native activity, then compare and profile
   release/native-CPU builds of both engines on both backends. Address measured bottlenecks,
   document results, and commit the completed query checkpoint before starting Q4.

Small independently testable prerequisites are committed before the full query checkpoint.
Compilation and focused tests are batched before check-in; an earlier diagnostic check should
answer a specific blocking question. Unrelated findings stay outside the current query's scope.

## Runtime configuration contract

The permitted runtime configuration surface is Flink's existing settings and semantics, plus
enabling StreamFusion and explicit opt-in for non-identical operators. Separate StreamFusion
tuning parameters, memory budgets and admission bypasses are not permitted. Benchmark-only
measurement controls do not become deployment settings. Unsupported Flink settings require
an explicit fallback rather than silent substitution with native defaults.

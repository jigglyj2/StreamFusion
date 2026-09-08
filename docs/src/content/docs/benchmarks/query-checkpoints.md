---
title: Nexmark query checkpoints
description: Query-ordered production admission and performance milestones.
---

Production coverage advances in increasing numbered-query order. Verify earlier admission,
then finish the next query on both state backends before expanding to later queries.
An implemented native kernel is not an unlocked query.

Performance work uses the [RowData-to-blackhole harness](/StreamFusion/benchmarks/rowdata-blackhole/),
with separate collecting-sink runs for parity. The blackhole path has passed Q0–Q2 admission
and native-activity integration checks with both backend settings after the sortable-state
changes. Q3 now passes ordinary admission, complete collected-changelog comparison,
and native-activity checks through blackhole with in-memory and default RocksDB state.
Incompatible native artifacts or unsupported backend options retain whole-plan fallback.
These short integration runs are not performance measurements.

## Current checkpoint

Q3's supported synchronous binary inner equi-join path is delivered on both in-memory and
default RocksDB backends. Ordinary admission, complete collected changelog comparison, the
default metric surface, checkpoint/backend-switch/rescaling/channel-replay contracts, and
corrected release measurements/profiles have been verified. The
[Q3 release comparison](/StreamFusion/benchmarks/q3-rowdata/) reports small ten-million-event
median gains and slower one-million-event results, including dispersion and limitations.
Mini-batched joins and the other unsupported subsets remain explicit whole-plan fallbacks.

Q4 is delivered on both backends. Its binary range join and keyed MAX/AVG stages pass ordinary
admission on both backends. Controlled shared-runtime tests compare exact changelog bytes and
registered metrics, backend-switch savepoints, rescaling, and actual aligned/unaligned channel
replay. Opt-in integration compares materialized Q4 results at parallelism one and four and
requires native activity through blackhole. Separate jobs can interleave the two join inputs
differently, so those end-to-end materialized results supplement the deterministic changelog tests.
The [Q4 release comparison](/StreamFusion/benchmarks/q4-rowdata/) records a general batched
join-predicate optimization, approximate in-memory parity and a 15.5% RocksDB median throughput
gain at one million events, with overlapping ranges. Separate longer profiles cover both engines
and backends. Q5's ordinary plan now accelerates local/global
HOP COUNT, attached MAX and a reused aggregate with two Arrow exits on both backends. Generated
SQL and channel-recovery tests cover that shared ownership and control path. Opt-in official
Nexmark tests match complete changelog and materialized results at 10,000 events, parallelism one
and four, on both backends. The [Q5 release comparison](/StreamFusion/benchmarks/q5-rowdata/)
reports approximate in-memory parity and a 10.8% RocksDB median throughput gain at one million
events, including dispersion, separate longer profiles and larger-workload memory limitations.
General fixes reduce duplicate retained keys and inflated batch/state reservations. The optimizer
and whole-plan fallback for unsupported subsets stay intact. Q6 has the upstream limitation below;
Q7 is the next executable query checkpoint.

Q7's demonstrated blockers are two-phase TUMBLE and a timestamp-arithmetic join residual. The
TUMBLE COUNT/MIN/MAX path now has shared-runtime Flink parity, metrics and recovery coverage;
its generated SQL tests require ordinary admission on both backends. The computed join predicate
remains gated. Q7 is not yet delivered or measured.

## Q6 has no Flink streaming baseline

The upstream Nexmark Q6 query computes a bounded ordered AVG after winning-bid rank selection.
After correcting the upstream SQL's alias/filter scopes, Flink 2.3.0 rejects its physical plan:
`Non-time attribute sort is not supported for bounded OVER window.` The join/rank result's order
column is a regular timestamp. Flink's non-time OVER support accepts unbounded preceding frames,
but cannot execute this bounded frame. This is an upstream planning restriction before native
execution, rather than a StreamFusion gate that can be removed while preserving the baseline.

The opt-in `NexmarkQ6PlanningIT` uses the Nexmark RowData source schema/views and the same multi-join
setting as the benchmark. It verifies the exact Flink failure on both backends, with StreamFusion
disabled and enabled, and zero native activity. Its scoped SQL fixture preserves the upstream join,
ranking and `10 PRECEDING` boundary; it does not substitute a different frame or a batch-mode query.
The upstream reference is Nexmark commit `6b3646c3baec701f1fa74baf938d235f742e5d3c`,
`nexmark-flink/src/main/resources/queries/q6.sql`.

Q6 therefore has no acceleration or throughput result and remains outside the runnable RowData
query catalog. Implementing native bounded non-time OVER alone would not provide an unmodified
Flink comparison. Resume Q6 when an equivalent supported upstream plan is available; proceed to
Q7 without claiming that Q6 is accelerated or that fallback can execute it successfully.

## Initial diagnostic baseline

Q3 was the next target at the start of this work. A September 7, 2026 diagnostic run of the existing local build used
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
gate uses the same binary-shape decision as semantic lowering. The verified pure equi subset
is admitted with in-memory and default RocksDB state. Computed residual operands retain a workspace restriction; direct column/literal comparisons
now have separate generated conformance coverage; non-default RocksDB settings and incompatible native artifacts
retain their specific backend restrictions. Genuine multi-way and
non-lowerable binary shapes still require their paged-state/output cursor to join the common
execution, metric and checkpoint lifecycle.

The binary **inner** join and its downstream generated Calc now have shared runtime conformance
for both backends, including the full default metric surface, per-input watermarks, latency,
all four RowKinds and bounded hot-key fan-out. Outer binary MultiJoin is deliberately not lowered
to the regular join: its retraction/null-padding sequence differs. It remains on the separate,
unadmitted MultiJoin path. Q3 is an inner join and is unaffected by that semantic restriction.

The shared binary join also passes generated 64-key changelog checks while rescaling 1 → 2 → 1
through Flink's key-group repartition APIs. Canonical savepoints switch between memory and
RocksDB; aligned and unaligned snapshot options preserve state on each backend, and successive
RocksDB checkpoints verify incremental file reuse. The harness now uses the production frame
key selector after rescaling. These tests cover keyed-state snapshots, not in-flight network
channel recovery. A separate mailbox-task test now verifies aligned restore and unaligned Arrow
frame replay on both backends through Flink's channel-state writer/reader, followed by exact
retraction comparison. Its test input channel inserts captured serialized frames explicitly;
it does not claim distributed failover or in-flight rescaling coverage. Binary join/Calc composition
is therefore admitted, including in-memory persistent state for the pure equi subset. RocksDB
configuration equivalence remains a prerequisite for the second backend.

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

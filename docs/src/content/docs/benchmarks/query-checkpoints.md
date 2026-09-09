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
Q7's supported execution and remaining capacity limits are recorded below.

Q7's demonstrated blockers are two-phase TUMBLE and a timestamp-arithmetic join residual. The
TUMBLE COUNT/MIN/MAX path now has shared-runtime Flink parity, metrics and recovery coverage;
its generated SQL tests require ordinary admission on both backends. Literal day-time offsets on
`TIMESTAMP(3)` join columns now have Flink-generated metric/changelog and both-backend recovery
coverage, with bounded native workspace checks. Other computed predicates retain their gate.
The opt-in `NexmarkQ7ProductionIT` verifies ordinary whole-plan admission and exact complete
changelog/materialized-result parity for the official RowData query at 10,000 events, parallelism
1 and 4, on both state backends. Native plan counters are positive and standalone local-window
counters remain zero. The first million-event release attempt exhausted the in-memory join's
retained-state allowance. Small join keys now use one compact backend entry to reduce storage
and lookup overhead; the same Flink budget applies. The
[Q7 release comparison](/StreamFusion/benchmarks/q7-rowdata/) records three alternating pairs at
250,000 events and separate longer 500,000-event mixed profiles on both backends. Median throughput
is 9.4% lower in memory and 13.6% higher on RocksDB, whose ranges overlap substantially. The
million-event in-memory attempt still exhausts join workspace; no large-input capacity or general
speedup is claimed. General state/buffer improvements and that remaining limit are documented.
Q7's supported semantic path is delivered within these limits; Q8 is the next query checkpoint.

Q8's DISTINCT-only TUMBLE prerequisites now cover local VARCHAR buffer geometry and pressure
flushes, global nullable/composite keys, complete registered metrics, canonical cross-backend
restore, rescaling and aligned/unaligned channel replay. Global wide-key memory admission includes
logical key payloads and avoids a duplicate-key index. Ordinary selection admits this verified
DISTINCT TUMBLE subset; VARCHAR aggregate calls and DISTINCT HOP remain gated. Official Q8
parity passes at 10,000 events, parallelism one and four, on both backends: complete collected
changelog bytes, materialized results, positive native plan activity and no standalone local-window
JNI batches. The [Q8 release comparison](/StreamFusion/benchmarks/q8-rowdata/) records one- and
ten-million-event measurements and separate two-/twenty-million-event profiles on both backends.
At ten million, median throughput is 12.4% below Flink in memory and 26.3% above on RocksDB, with
wide overlapping timing ranges. All twenty-million-event profiles finish without capacity failure;
profile timings are excluded from results. Q8 is delivered within these documented limits; Q9 is next.

Q9's ordinary plan now accelerates its binary range join and append-only partitioned Top-1.
The append-only Top-1 compute prerequisite uses DataFusion sort and cumulative MIN with fixed-width
ordinals and per-arrival changelog parity. Its shared Calc → Top-1 → Calc binding now verifies
native ownership/lifecycle, canonical cross-backend restore and the complete Flink stage metric
and control surface. Top-1 now batches point-state reads and writes only changed winners, with
migration from older ordered state. Generated managed-checkpoint/backend-switch/rescaling and
actual Arrow channel-replay tests pass on both backends. Generated Top-1 SQL compares complete
collected changelog bytes with ordinary selection. Official Q9 integration compares final keyed
result bytes at 10,000 events, parallelism one/four, on both backends and requires positive shared
plan activity with zero standalone Top-N invocations.

Q9's independent jobs do not have a deterministic transient changelog: repeated unmodified Flink
runs in the scheduling diagnostic emitted 1,537 and 1,495 changelog records at parallelism one,
while each ended with the same 593 rows. All sixteen diagnostic runs (two per engine/backend/
parallelism combination) matched final result bytes within their configuration. Identical-arrival
operator tests still compare every changelog transition.

[Q9's release report](/StreamFusion/benchmarks/q9-rowdata/) records six alternating measured pairs
per backend at 250,000 events, separate 500,000-event CPU profiles, and an additional RocksDB
wall-clock diagnostic. The combined median throughput ratio is 0.766× Flink in memory and 3.107×
on RocksDB, but the RocksDB sets disagree (3.620× then 0.950×), so no reliable speedup is established.
General improvements bound wide predicate workspace, shrink candidate chunks on budget denial,
and drain admitted output prefixes while preserving state-transition order. The million-event
in-memory run still fails during dirty join-state encoding: another 262,416 bytes are denied with
7,700,204 already reserved and 139,125 available. Earlier failed attempts are retained in the report;
no million-event comparison or RocksDB result is claimed. Q9 is delivered within these explicit
limits. Q10 is next.

Q10's SELECT path is delivered through ordinary whole-plan selection. Numeric `DATE_FORMAT`
over timezone-free `TIMESTAMP(3)` uses DataFusion kernels with full-range calendar adaptation,
a direct path for AD years 1–9999 and one coarse memory owner. Generated ordered changelog,
managed-memory and boundary tests cover its contract; unsupported variants keep precise fallback.
Official SELECT integration verifies complete collected results at 10,000 events, parallelism
one/four, on both backend configurations with positive native plan/Calc activity. The
[Q10 release comparison](/StreamFusion/benchmarks/q10-rowdata/) records three alternating pairs at
one and ten million events, plus separate two-/twenty-million-event mixed profiles. Ten-million-event
throughput ratios are 0.994× Flink in memory and 0.979× with RocksDB configured, with overlapping
ranges; million-event results remain slower. This stateless workload does not establish RocksDB
state performance or filesystem sink partition-commit/rolling behavior. Q11's session-window
plan is the next checkpoint. Its initial ordinary EXPLAIN falls back at `StreamExecWindowAggregate`.
The retained SESSION kernel now checks lateness after merging, with controlled-arrival Flink
comparisons; this is a correctness prerequisite, not an admission or performance result. Its shared
COUNT kernel now uses DataFusion grouped computation and ordered per-session state, with generated
Flink metric/changelog comparison and native cross-backend/legacy-state restore checks. SESSION
managed-checkpoint, backend-switch, rescaling and real-barrier channel-replay tests now pass;
ordinary selection now binds the verified single-key BIGINT SESSION COUNT subset to the shared
native region. Generated SQL and Arrow topology checks cover that binding. Official Q11 results
match Flink byte-for-byte for 10,000 generated events at parallelism one/four on both backends,
with ordinary acceleration and positive native plan/Calc activity. The
[Q11 release comparison](/StreamFusion/benchmarks/q11-rowdata/) records three alternating pairs
at one and ten million events, plus separate two-/twenty-million-event mixed profiles on both
backends. At ten million the median throughput ratios are 1.181× Flink in memory and 2.001× on
RocksDB, with disjoint ranges; Flink's RocksDB times vary from 17.9 to 42.9 seconds, so this is not
a stable general speedup claim. Million-event memory remains slower and RocksDB ranges overlap.
General improvements amortize decoded-state budget calls and eliminate empty terminal RocksDB
scan probes while retaining ABI-8 compatibility. All twenty-million-event profiles complete.
Q11's supported SESSION COUNT path is delivered within these limits.

Q12 is the active checkpoint. Its ordinary plan contains a `PROCTIME()` Calc, exchange and
single-stage processing-time TUMBLE COUNT. Both backends currently retain whole-plan fallback:
the shared processing-time resource binding, lifecycle and recovery parity contract is incomplete.
The logical `PROCTIME()` Calc attribute now lowers to DataFusion's typed null expression, matching
Flink's code generator without reading or storing a clock value. Generated physical-Calc tests
compare complete ordered changelog bytes and record timestamps for every RowKind, nullable keys,
empty inputs and different batch sizes. Actual `PROCTIME_MATERIALIZE` clock reads remain gated.
EXPLAIN inspects the original nodes instead of hiding rejected inputs through legacy shape folding;
it does not mislabel Q12 as an unsupported SESSION window.

A SQL-generated Flink clock oracle now checks nullable keys, generated counts, one-/ten-/37-second
UTC windows, live clock transitions, terminal watermarks, bounded finish and restoration of pending
processing-time timers on both backends. The physical PROCTIME input slot is null; the consuming
window reads Flink's clock. Only processing-time progress fires these windows. Terminal event time
and finishing input do not close the final open window. These are prerequisite reference tests,
not native parity or admission evidence. The next implementation must preserve this lifecycle in
the shared Arrow tree; substituting one timestamp per batch or a private native clock is not proven
equivalent. Shared control protocol 2 now carries a separate, capability-negotiated processing-time
timer event through the normal native Arrow tree. Existing event-time operators reject that event
before mutation and keep protocol-one capabilities. The shared region edge now registers the earliest
negotiated native deadline with Flink's processing-time service, including overdue restored timers,
and cancels callbacks on finish/close without firing open windows. Focused lifecycle tests cover stale
callbacks, tied owners, signed timestamp boundaries, invalid descriptors, and failed timer drains.
Capability protocol 3 now connects clock ports to the shared tree/region execution edge. It exports
one Arrow Int64 clock vector per negotiated input through the existing C Data call, uses header-only
row-count inspection for IPC, and attaches metadata after native payload decoding. Tests cover
per-record values, rollback/boundaries, direct/decoded input ownership, invalid bindings and
schema/length failures. Clock owners must directly consume an external edge and remove clock
metadata before output. No production window factory enables the capability yet; the native
processing-time factory/resource binding and full parity/recovery proof remain pending. This is still a
prerequisite, not Q12 admission.
The existing DataFusion grouped window buffer now has a processing-time mode. It assigns each
row from its supplied clock while retaining Flink's buffer capacity and flush boundaries; watermarks
and EOF do not flush it. Flink reference tests on both backends establish an observable edge case:
a repeated or rollback timer can emit COUNT zero while new records remain buffered. A checkpoint
publishes those records, so the next repeated timer sees that count. Native buffer tests preserve
these pending/published boundaries, including negative clocks and null PROCTIME placeholders.
The native buffer/state component now reuses ordered Arrow-keyed slice storage and DataFusion
partial merging for direct UTC TUMBLE COUNT(*). Timers register at raw arrival and fire in bounded
frontiers even when no accumulator was published; flushing state does not recreate fired timers.
Component tests match the established Flink clock cases and verify cross-backend absolute timer
restore with one-to-two rescaling, bounded state I/O, input-plan fingerprinting and memory denial.
Factory resource binding and full Flink lifecycle/recovery parity remain required before admission.

No Q12 performance result is claimed from empty/partial max-speed bounded output.

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
is admitted with in-memory and default RocksDB state. Direct column/literal comparisons and
literal day-time offsets on `TIMESTAMP(3)` columns have separate generated conformance coverage;
other computed residual operands retain a workspace restriction. Non-default RocksDB settings and incompatible native artifacts
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

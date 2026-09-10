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

The September 10 audit covers the pinned upstream suite, Q0–Q23. There is no Q24 in that
checkout. Forty-four fresh-JVM EXPLAIN checks at production code `78414b06` use Flink's
default `table.optimizer.multi-join.enabled=false`, mini-batching disabled and both backends.
That audit admitted twenty query plans. Q5 and Q8 selected `StreamExecWindowJoin` and fell back;
their earlier delivery evidence used the benchmark's enabled multi-join preset.

The subsequent inner-window integration admits Q5 with the default optimizer setting. Its
original SQL, generated SQL parity, Arrow exchange topology, original memory bindings, complete
metrics, aligned/unaligned channel replay and rescaling are verified. Collecting-sink bytes and
unmodified-blackhole record counts match Flink at 20,000 events, parallelism one and four, on
both backends. The enabled multi-join path is rechecked too. Release measurement and profiling
for the default WindowJoin path are still pending, so the Q5 delivery checkpoint is incomplete.
Q8's full query validation follows Q5; this is not a new full-suite admission or performance audit.

Eight focused integration cases recheck Q6 and Q14 on current code: Q6 still fails Flink's
bounded non-time OVER planning; Q14 executes its original Java UDF through whole-plan Flink
fallback with positive output and zero native activity. Its proposed batch-callback exception
is not approved. Q0–Q2 have admission/parity evidence but still lack the later checkpoints'
complete standalone release-report/profile evidence. The full goal is not complete.

Raw EXPLAIN results and blocker checks are retained under
`streamfusion-nexmark-benchmarks/target/measurements/suite-audit/`. These checks do not rerun
the full suite's runtime parity or performance on one common commit. The next implementation
target is Q5's default window-join performance; the historical checkpoints below retain their stated scope.

## Delivery history and scope

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
and backends. Q5's plan with the enabled multi-join optimizer accelerates local/global
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
parity with the enabled multi-join optimizer passes at 10,000 events, parallelism one and four,
on both backends: complete collected
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

Q12's supported processing-time TUMBLE COUNT path is delivered. Its operator supports ordinary
whole-plan selection with UTC, one nullable or non-null BIGINT key, unfiltered COUNT(*), start/end
properties, synchronous state and disabled mini-batching. The original `PROCTIME()` Calc and
exchange remain in the selected graph. Other time zones, window kinds, aggregate/key shapes and
clock-sensitive placements retain precise whole-plan fallback. Official Nexmark RowData collecting
checks pass at 50 million events, parallelism four, on both engines and state backends. They require
non-empty INSERT output, positive counts, unique bidder/window pairs, ten-second UTC alignment and
positive native plan/Calc activity for StreamFusion. The
[Q12 release comparison](/StreamFusion/benchmarks/q12-rowdata/) records three alternating pairs at
50 million events and separate 100-million-event mixed profiles on both backends. Median throughput
ratios are 1.303× Flink in memory and 1.720× on RocksDB, with disjoint ranges. The third RocksDB pair
is nearly tied, so its median advantage is not a stable general speedup. Every fork emits windows;
independent processing clocks still produce different window labels and output counts.
Borrowed Arrow keys eliminate repeated timer registration within each raw batch while preserving
all DataFusion counts, absolute timers and Flink's original buffer allowance. Registration's CPU
share falls from 8.94% / 8.11% to 2.08% / 1.92% in memory / RocksDB; cross-run shares do not isolate
a throughput gain. All 100-million-event profiles finish without capacity failure. Q13 is documented below.

The logical PROCTIME attribute lowers to DataFusion's typed null. Arrow/protobuf schemas allow
that physical null even when Flink's logical attribute is NOT NULL; ordinary timestamp constraints
are unchanged. Actual `PROCTIME_MATERIALIZE` clock reads remain gated. The receiving window reads
one Flink clock value per record at its external Arrow edge, through the existing C Data call or
after a single IPC decode. Flink owns mailbox scheduling and checkpoint coordination; native state
stores absolute timers. A private native clock or one timestamp per batch is not substituted.

The reusable DataFusion buffer preserves Flink's original paged capacity, pressure/checkpoint
flushes and strictly advancing timer progression. Repeated or rollback timers can emit COUNT zero
while new records remain buffered. Only raw arrivals register timers; publishing partials cannot
recreate fired timers. Ordered Arrow-keyed slice state and DataFusion merging batch backend access
and emit bounded timer frontiers. Large buffers, retained state and growing workspaces remain
under Flink's managed-memory reservations.

Generated controlled-clock tests compare complete changelog/control bytes and registered metrics
on both backends, including nullable keys, varying batches, one-/ten-/37-second windows, rollback,
terminal controls and checkpoint restore. Pressure tests cover 300,000 records at two batch sizes.
Recovery covers one-to-two-to-one rescaling, canonical backend switches, incremental RocksDB SST
reuse and actual aligned/unaligned IPC replay into a later processing-time window. Only Flink's
unspecified order among independent keys tied at one timer deadline is canonicalized.

Ordinarily selected factories retain the original physical identity, Arrow input/output and
complete-pipeline resource binding through serialization, and match the Flink clock oracle.
Original capacity/page bytes also match Flink job graphs with weighted boundaries and distinct
slot-sharing groups. Live SQL tests produce non-empty windows through the actual exchange on both
backends. Those separate wall-clock jobs check execution invariants; exact bytes are established
with identical controlled clocks. Terminal watermarks and bounded finish do not emit an open
processing-time window. No Q12 performance result is claimed from empty/partial max-speed output.

## Q13 lookup delivery

The original upstream Q13 SQL now plans and executes with native lookup acceleration.
`NexmarkQ13PlanningIT` loads `/queries/q13.sql` directly from the upstream Nexmark JAR,
substitutes only the temporary side-file path and blackhole sink name, and writes the same
10,000 integer/string pairs as upstream `SideInputGenerator`. Four cases cover HashMap and
RocksDB with StreamFusion disabled and enabled. At 10,000 source events, each execution emits
exactly the bid count measured by Q0 through the unmodified blackhole sink. StreamFusion reports
`Accelerated: yes` and positive native plan batch counters; Flink reports no native activity.

The lookup uses DataFusion hash/probe/equality computation and Arrow gathers in the same native
tree as adjacent Calcs. The original configured CSV reader loads all splits at task open;
duplicates retain file order and recovery reloads the file. Generated tests against Flink's
actual lookup code generator compare full changelog bytes, timestamps, metric surfaces, control
events, and Calc composition. Aligned/unaligned operator snapshots contain no cache state, as in
Flink. Native memory tests cover shared-buffer accounting, bounded probe/output work, and full
credit return on close or failure. See [joins](/StreamFusion/operators/joins/) for the admitted
subset and precise fallback conditions, including regions mixing lookup and keyed state.

Q13 is also available in the collecting and blackhole benchmark catalog. Both engines create
the same temporary 10,000-row CSV fixture within end-to-end setup and remove it during cleanup.
A separate integration check verifies that the catalog SELECT is unchanged from upstream and
compares complete enrichment changelog hashes and blackhole counts at 50,000 source events,
parallelism 1 and 4, and both configured backends.

The [Q13 release comparison](/StreamFusion/benchmarks/q13-rowdata/) records three alternating
fresh-JVM pairs at one and ten million events and separate mixed profiles at two and twenty
million. At ten million, median throughput is 1.113× Flink with HashMap and 1.149× with RocksDB
configured, with disjoint timing ranges. One-million-event runs remain slower. Configuring RocksDB
for this stateless query does not establish RocksDB state-performance behavior. The original legacy source
remains the baseline; replacing it with the modern scan-only filesystem connector would change it.

## Q14 scalar prerequisites

Q14 is in progress and retains whole-plan fallback. Its timezone-free `TIMESTAMP(3)` clock
extraction and mixed-width integer comparisons now have DataFusion execution, generated Flink
changelog/metric parity, full signed-millisecond coverage and managed-buffer checks. Rechecking
the original Q14 SQL on both backends now reaches its original `count_char` Java UDF as the
reported blocker. Widening the nested CASE result from `VARCHAR(9)` to the sink's
`VARCHAR(2147483647)` is supported through value forwarding, with separate generated changelog
and metric parity. The decimal range expressions now pass generated SQL changelog parity,
including full signed-integer inputs and non-empty selected results. Mixed DECIMAL/integer and
different-scale decimal comparisons use lossless DataFusion coercion, including Decimal256 only
when required, with Flink-generated metric/changelog and managed-buffer checks.
Java UDF execution remains unsupported;
the [Arrow-batch callback proposal](/StreamFusion/development/jvm-udf-boundary/) awaits an explicit
architecture exception. No Q14 acceleration or performance result is claimed.

## Q15 admission and correctness

The latest aggregate prerequisite stores presence bits for newly created append-only COUNT
DISTINCT groups, following Flink's append-only data-view behavior. It retains DataFusion COUNT
computation and batched state I/O, skips writes for unchanged membership, and preserves legacy
counted groups during restore. Generated insert-only Flink parity covers metrics, checkpoints,
channel replay and rescaling. The [Q15 release comparison](/StreamFusion/benchmarks/q15-rowdata/)
now includes packed point-state entries at `158ff8b3`: 1.221× RocksDB and 0.676× in-memory
median throughput at one million events, and 1.748× RocksDB throughput at ten million. The
120,000-entry storage fixture fits a 15 MiB share that cannot hold the previous unpacked directory.
Separate longer profiles cover both backends. The ten-million-event in-memory attempt still
exhausts retained-state capacity. No large in-memory result or incremental throughput gain over
the preceding StreamFusion build is claimed; the report retains dispersion and every failed case.

While Q14's JVM UDF architecture decision is pending, original Q15 now passes ordinary
whole-plan selection with its filtered DISTINCT aggregates unchanged. The benchmark catalog
uses the exact upstream SELECT and its thirteen output columns, without adding a primary key.
The native plan composes DataFusion-based Calc and keyed aggregation through Arrow batches.
Pure nested-field projections keep unrelated source payloads out of conditional gathers.

BIGINT COUNT(DISTINCT) delegates counting to DataFusion while retaining Flink's signed membership
state. Generated tests compare the original Flink aggregate handler's per-key changelog bytes,
record envelopes and registered metrics on both backends. Recovery coverage includes canonical
backend switches, aligned/unaligned restore, 1→2→1 repartitioning across all 16 test key groups,
and captured Arrow channel replay. Tests preserve duplicate counts and unmatched retractions
through restore and then delete the final members. Ordinary SQL admission tests additionally
compare complete serialized changelog multisets for nullable arguments and independent FILTERs.
See [group aggregation](/StreamFusion/operators/group-aggregation/) for the admitted subset.

The opt-in Q15 integration tests use 50,000 RowData source events with both backends and
parallelism one and four. Collecting-sink validation compares final materialized bytes and
logical changelog counts on each configuration, plus the complete changelog multiset at
parallelism one. Independently scheduled parallel channels can produce different transient
counts; the controlled runtime/recovery tests establish exact transitions for identical input
order. Blackhole output counts are compared separately between engines because that sink does
not request UPDATE_BEFORE records. Selected runs require positive native plan batch counters.

These tests use the campaign's existing 1 GiB Flink managed-memory setting and 90:10 operator/state
consumer weights on both engines. The smaller embedded defaults are not a supported capacity
claim: the RocksDB case exhausted aggregate batch scratch/output credit after conditional input
pruning, requesting 7,718,512 bytes with 7,498,960 available. State storage and output remain
subject to Flink's assigned allowance. The initial admission tests establish correctness; the
release measurements below track performance and the remaining state-capacity limitation.

The first one-million-event release attempt at `8679f83b` also exhausted memory before returning
a StreamFusion timing: aggregate scratch/output requested 15,017,456 bytes with 12,339,736
available. That failed attempt is retained under the benchmark's `target/measurements/q15/`
directory and is not a throughput result. Investigation found duplicate decoded-state allowances
and decode headroom retained after the decoded maps were freed. Synchronous aggregation now
keeps one historical-state allowance and releases unused decode credit before building output.

On clean release commit `19beb78f5ffda3e0940bc66e010768f84fb657af`, three alternating fresh-JVM
pairs at one million events completed on HashMap. Flink's median was **5.469674 s**
[5.285552, 5.577082], MAD 0.107408 s; StreamFusion's was **7.601704 s**
[7.540897, 7.955219], MAD 0.060807 s. The throughput ratio is **0.719533×**, so this is a
regression against Flink, not a performance win. Every engine fork emitted 920,000 blackhole
records; each selected fork reported acceleration, 200 native plan batches and 136 Calc batches.
No measured forks were discarded.

Timing is end-to-end, including SQL/EXPLAIN, native initialization, cluster startup, execution
and cleanup, but excluding JVM launch, argument parsing and builds. Both engines used parallelism
four, mini-batching disabled, one-second exactly-once checkpoints, UTC, 1 GiB heap, 2 GiB direct
memory, and four active JVM processors. The machine was the WSL2 Intel Core i7-12650H host with
16 logical CPUs and Java 24.0.2. Native artifacts used release optimization, native CPU features,
frame pointers and profiling symbols. The core artifact SHA-256 was
`a6d8d8ac6d25b69d0513f2841b0e530f8451ab4a59c080d328c999804c376a8d`; the RocksDB artifact was
`118e0d6c10fb0ed24ef99d44e0bc8eaf7d3f81402554bf5b9848ba806cf2e85a`.

The RocksDB StreamFusion fork still failed at one million events: historical-state decoding
requested 56,542,920 bytes with 55,699,897 available. There is no RocksDB median or speedup.
Separate 500,000-event diagnostic profiles completed for both engines/backends, retaining JFR,
CPU/allocation collapsed stacks, flame graphs and differential graphs. They are smaller than the
measured workload and dominated by JVM compilation (46.4–48.2% of process CPU samples); they do
not satisfy the final longer-workload profiling checkpoint or establish an optimization ceiling.
The next work is historical-state capacity and then representative profiling. Artifacts, flags,
upstream revisions/patches and raw runs remain under
`streamfusion-nexmark-benchmarks/target/measurements/q15/19beb78f/`.

### Q15 measured state-decoding improvements

Clean release commit `32bc32d9e0aaf9bcf24025033b4cd6f0ac48814d` completed three alternating
fresh-JVM pairs on **both backends at one million events**, using the same end-to-end method,
machine and Flink settings above. No measured forks were discarded. The implementation sizes
historical-state workspace from its serialized entries, builds canonical sorted membership maps
in bulk using the Rust standard library, and bounds new map growth by non-null arguments that
pass each aggregate's FILTER. It retains DataFusion COUNT and all per-record changelog transitions.

| Backend | Flink median seconds [min, max]; MAD | StreamFusion median seconds [min, max]; MAD | Throughput ratio SF/Flink |
| --- | --- | --- | --- |
| HashMap | 5.667826 [5.577019, 6.637873]; 0.090807 | 8.183829 [7.149841, 8.285988]; 0.102159 | **0.692564×** |
| RocksDB | 8.244501 [8.204193, 8.435007]; 0.040308 | 7.532215 [7.291340, 7.987163]; 0.240875 | **1.094565×** |

The ranges are disjoint: StreamFusion is slower on HashMap and about 9.5% faster on RocksDB
in these forks. Every engine emitted 920,000 records into the original blackhole sink. Every
StreamFusion EXPLAIN selected the complete native plan. In measured order, native plan/Calc
batch counts were **200/136, 224/152, 212/144** on HashMap and **208/140, 232/156, 223/150**
on RocksDB; Flink counters were zero. Batch counts can vary with runtime flushing even for
identical source events. Core artifact SHA-256:
`1e84248ef4aaf9a3f796b5dfee68f913b255bef93fa3ae89e1ac112cb0119d77`;
the RocksDB artifact hash is unchanged from above. Both used release/native-CPU optimization,
frame pointers and profiling symbols. The recorded Flink revision is
`c0f8d1a1e09f209885a88f9c19ceb9d9e9870283`, with only the approved planner/class-loading and
managed-share callback patches; the original Flink engine path and operator algorithms remain
the baseline. Upstream Nexmark is the clean revision
`6b3646c3baec701f1fa74baf938d235f742e5d3c`. No Kafka services or connectors were exercised.

Separate two-million-event HashMap profiles completed for both engines, each emitting 1,840,000
blackhole records. StreamFusion reported 395 native plan batches and 266 Calc batches. The larger
RocksDB profiles did **not** complete: StreamFusion requested 59,281,427 bytes of decoded-state
and mutation workspace with 58,702,103 available at two million events, and 59,099,765 with
58,735,253 available at 1.5 million. These are failures in those profiled forks, not measured
throughput or a universal event-count limit. Flink completed both sizes. A separate one-million-
event RocksDB diagnostic profile pair completed with 920,000 output records and 229/154 native
plan/Calc batches. That diagnostic pair is the same size as the measured workload and **does
not satisfy the required longer-workload profiling checkpoint**.

The completed async-profiler runs use 10 ms CPU sampling, Java non-safepoint sampling, native
DWARF unwinding and JFR output. They retain CPU/allocation collapsed stacks, per-engine flame
graphs and differential graphs. The table gives inclusive percentages of all process CPU samples; categories
overlap. In particular, JNI includes downstream native work, and DataFusion includes state work
below its execution-plan frames rather than only numerical kernels. A zero sampled share does
not establish absence of work. Profiled elapsed times are excluded from the benchmark results.

| CPU category | HashMap 2M Flink / SF | RocksDB 1M diagnostic Flink / SF |
| --- | --- | --- |
| Samples | 2,267 / 2,553 | 2,075 / 1,988 |
| Row copying | 15.704% / 7.207% | 8.337% / 4.427% |
| RowData-to-Arrow writing | 0% / 1.802% | 0% / 1.559% |
| Arrow C Data / JNI, inclusive | 0% / 19.585% | 0% / 11.368% |
| Native plan lowering | 0% / 0% | 0% / 0.050% |
| DataFusion execution, inclusive | 0% / 18.371% | 0% / 9.809% |
| Native aggregate state codec | 0% / 7.756% | 0% / 2.817% |
| Arrow-backed output access | 0% / 2.977% | 0% / 1.358% |
| Source polling, inclusive | 31.363% / 21.661% | 15.566% / 15.946% |
| RocksDB, inclusive | 0% / 0% | 11.952% / 3.169% |
| Budget callbacks | 0% / 0.157% | 0% / 0% |
| JVM compilation | 35.862% / 32.432% | 39.663% / 39.537% |

The previous `d809379f` two-million-event HashMap profile spent 19.840% in the aggregate state
codec, including repeated B-tree insertion; that share fell to 7.756% after bulk decoding.
This is profile evidence for the implementation change, not an isolated throughput attribution.
Q15's performance checkpoint remains incomplete: loading and rewriting each group's whole
membership history still creates workspace proportional to historical cardinality. The next
structural work was batched access to individual membership entries. That path is now implemented
for synchronous COUNT(DISTINCT), including shared filtered counts, Arrow keys, group cleanup and
old-state migration; see [group aggregation](/StreamFusion/operators/group-aggregation/).
The table above measures the earlier whole-map implementation. The next section measures the
per-member implementation; neither establishes a reasonable optimization ceiling.

All raw runs, profiles, metadata, build flags and upstream patch hashes are retained under
`streamfusion-nexmark-benchmarks/target/measurements/q15/32bc32d9/`, including the failed larger
RocksDB attempts. The earlier `d809379f` measurements and profiles remain in their own directory.

### Q15 per-member state results and checkpoint capacity

Clean release commit `9fc0c19c9fab5fc1fc5ccfe454a97e69c94a4e6a` replaces whole-map DISTINCT
history with Arrow-encoded membership keys, one batched read of incoming members and one atomic
write of changed members and group headers. Shared filtered counts and signed retract counts
remain exact; DataFusion still computes the aggregates. Legacy inline snapshots migrate when
next touched. Generated changelog, metrics, restore and rescaling tests passed on both backends.
The production admission subset remains synchronous BIGINT COUNT(DISTINCT).

Three alternating fresh-JVM pairs used original Q15, RowData input and the original blackhole
sink, with the same end-to-end timer, host, JVM limits, parallelism and checkpoint settings above.
Both engines used 1 GiB Flink managed memory and consumer weights
`OPERATOR:90,STATE_BACKEND:10,PYTHON:30`. No measured forks were discarded and no Kafka was used.

| Backend / events | Flink median seconds [min, max]; MAD | StreamFusion median seconds [min, max]; MAD | Throughput ratio SF/Flink |
| --- | --- | --- | --- |
| HashMap / 1M | 8.214385 [6.265439, 8.409952]; 0.195567 | 11.255587 [8.676581, 13.898180]; 2.579006 | **0.729805×** |
| RocksDB / 1M | 8.191732 [8.102087, 8.355158]; 0.089645 | 7.026451 [6.994387, 7.026959]; 0.000508 | **1.165842×** |
| RocksDB / 10M | 40.186194 [38.639833, 57.047206]; 1.546361 | 31.910713 [30.185959, 38.937901]; 1.724754 | **1.259332×** |

HashMap remains slower, with substantial variation in its StreamFusion forks. RocksDB is about
16.6% faster at 1M with disjoint ranges. Its 10M ranges overlap slightly and the median speedup
is not a uniform per-fork improvement. All successful engines emitted 920,000 / 9,200,000
blackhole records at 1M / 10M. Every successful StreamFusion EXPLAIN selected the whole native
plan. Measured native plan/Calc batch counts were **203/138, 200/136, 200/136** for HashMap 1M;
**220/148, 208/140, 226/152** for RocksDB 1M; and **1899/1268, 1904/1272, 1876/1252** for
RocksDB 10M. Flink native counters were zero.

Separate 2M profiles completed for both engines on both backends, each emitting 1,840,000
records. StreamFusion reported 404/272 native plan/Calc batches on HashMap and 388/260 on
RocksDB. They retain JFR, CPU/allocation collapsed stacks, per-engine flame graphs and
differential graphs, using 10 ms CPU sampling, Java non-safepoint sampling and native DWARF
unwinding. These satisfy the longer-workload profile requirement for the **1M** comparison.
Profiled elapsed times are excluded from the benchmark table. Inclusive CPU shares below use
all process samples; categories overlap, JNI includes downstream native work and DataFusion
includes state work beneath execution-plan frames. Zero samples do not establish absence.

| CPU category | HashMap 2M Flink / SF | RocksDB 2M Flink / SF |
| --- | --- | --- |
| Samples | 2,224 / 2,352 | 2,737 / 2,495 |
| Row copying | 14.029% / 7.015% | 10.011% / 6.172% |
| RowData-to-Arrow writing | 0% / 1.573% | 0% / 1.884% |
| Arrow C Data / JNI, inclusive | 0% / 13.946% | 0% / 14.228% |
| Native plan lowering | 0% / 0% | 0% / 0% |
| DataFusion execution, inclusive | 0% / 12.585% | 0% / 11.503% |
| DISTINCT membership state access | 0% / 1.658% | 0% / 2.084% |
| Native aggregate state codec | 0% / 0.043% | 0% / 0.040% |
| Arrow-backed output access | 0% / 2.253% | 0% / 2.405% |
| Source polling, inclusive | 31.520% / 22.364% | 21.008% / 21.723% |
| RocksDB, inclusive | 0% / 0% | 17.647% / 3.367% |
| Budget callbacks | 0% / 0.043% | 0% / 0.240% |
| JVM compilation | 35.432% / 35.799% | 32.042% / 35.271% |

The HashMap aggregate-codec share fell from 7.756% in the previous 2M profile to 0.043%.
This supports the removal of repeated whole-map serialization, rather than isolating its
throughput contribution. Source work and JVM compilation remain substantial.

Two larger attempts exposed remaining capacity limits:

- **HashMap 10M failed** in its first StreamFusion fork when retained state-table growth
  requested 34,770,984 bytes with 61,712,205 already reserved and 7,967,496 available. There
  is no HashMap 10M median or speedup; the failure was retained-state growth, rather than
  decoding the aggregate's whole membership history.
- **RocksDB 20M profiling failed** at checkpoint 24 while requesting 106,147,343 bytes for a
  canonical RocksDB snapshot, with 4,096 already reserved and 96,079,934 available. Flink
  completed with 18,400,000 output records. The StreamFusion fork did not complete, so the
  **10M timing comparison still lacks a successful longer paired profile**. Whole-key-group
  canonical checkpoint buffering is a demonstrated remaining limit.

These are the observed failures under the recorded managed-memory shares, not universal
maximum event counts. Q15 is production-admitted but its scalability/performance work remains
open. Retained-state growth and checkpoint buffering need improvement; a larger memory budget,
disabled checkpoints or smaller replacement forks would not resolve those implementation limits.

Core release/native-CPU artifact SHA-256:
`5e5b54cf842bb95f0dd53d1803efea905fb4a24b49fbb91bc19f903c9a28d62f`.
The RocksDB artifact remains
`118e0d6c10fb0ed24ef99d44e0bc8eaf7d3f81402554bf5b9848ba806cf2e85a`.
Both retain frame pointers and profiling symbols without reducing optimization. Machine/runtime
metadata, complete commands, upstream revisions and patch hashes, raw runs and failed-fork logs
are retained under `streamfusion-nexmark-benchmarks/target/measurements/q15/9fc0c19c/`.

### Q15 full RocksDB checkpoints follow Flink's file strategy

Clean commit `54a5e5c00689d989fd6956d9ca0db57c01074e52` fixes the full-checkpoint mismatch:
with Flink's default `execution.checkpointing.incremental=false`, StreamFusion now uploads native
RocksDB checkpoint files privately instead of serializing each key group's whole state into a
buffer. This matches Flink's `RocksNativeFullSnapshotStrategy`; incremental SST reuse remains
controlled by the existing Flink setting. Canonical savepoints retain the portable raw-keyed format.
The change passed 84 distinct focused unit/integration tests, including generated changelog,
aligned/unaligned restore, rescaling, cancellation, durable checkpoint metadata, metric-surface
checks and Q15 ordinary planner/production tests. No Rust compute or native artifact changed.

Fresh RocksDB runs used three alternating fresh-JVM pairs per size, followed by separate 2M and
20M mixed JVM/native profiles. The source, original blackhole sink, end-to-end timer, Flink memory
shares, parallelism, one-second exactly-once checkpoints, JVM limits, native CPU build and host
were identical to the preceding campaign. Every fork is retained; no Kafka was used.

| Events | Flink median seconds [min, max]; MAD | StreamFusion median seconds [min, max]; MAD | Throughput ratio SF/Flink |
| --- | --- | --- | --- |
| 1M | 11.166225 [9.560942, 12.658226]; 1.492001 | 9.823816 [7.016843, 13.616004]; 2.806973 | **1.136648×** |
| 10M | 39.129748 [38.615214, 39.683834]; 0.514534 | 23.869675 [23.556482, 27.642661]; 0.313193 | **1.639308×** |

The 1M ranges overlap widely, so that median does not establish a consistent win. At 10M the
ranges are disjoint and all three paired StreamFusion forks are faster. The before/after campaigns
are separate measurements, not an isolated attribution of the entire timing difference to
checkpointing. Both engines emitted 920,000 / 9,200,000 records at 1M / 10M. Every StreamFusion
EXPLAIN selected the complete native plan. Measured native plan/Calc counts were **229/154,
208/140, 232/156** at 1M and **1943/1298, 1921/1282, 1925/1286** at 10M; Flink counters were zero.

The **20M profile now completes on both engines**, each emitting 18,400,000 records. StreamFusion
reported **3809/2542** native plan/Calc batches. The separate 2M pair also completed with
1,840,000 output records and **391/262** StreamFusion batches. These supply longer paired
profiles for both measured sizes, resolving the prior checkpoint-buffer failure in the 20M run.
They retain JFR, CPU/allocation collapsed stacks, per-engine flame graphs and differential graphs.
Sampling is 10 ms CPU with Java non-safepoint sampling and native DWARF unwinding; profile elapsed
times are excluded from the throughput table.

Inclusive percentages below use all process CPU samples. Categories overlap: JNI includes native
computation below it, and DataFusion includes state work below execution-plan frames. Zero sampled
share does not prove absence of work.

| CPU category | RocksDB 2M Flink / SF | RocksDB 20M Flink / SF |
| --- | --- | --- |
| Samples | 2,761 / 2,388 | 14,167 / 10,692 |
| Row copying | 10.757% / 6.910% | 21.847% / 11.308% |
| RowData-to-Arrow writing | 0% / 2.010% | 0% / 3.217% |
| Arrow C Data / JNI, inclusive | 0% / 15.452% | 0% / 38.571% |
| Native plan lowering | 0% / 0% | 0% / 0% |
| DataFusion execution, inclusive | 0% / 14.196% | 0% / 27.815% |
| DISTINCT membership state access | 0% / 2.554% | 0% / 4.115% |
| Native aggregate state codec | 0% / 0.042% | 0% / 0.075% |
| Arrow-backed output access | 0% / 2.303% | 0% / 4.433% |
| Source polling, inclusive | 21.659% / 22.529% | 40.248% / 41.545% |
| RocksDB, inclusive | 17.168% / 2.764% | 30.649% / 11.532% |
| Budget callbacks | 0% / 0.042% | 0% / 0.112% |
| JVM compilation | 32.090% / 35.092% | 7.376% / 10.615% |

The larger profile makes the source and native aggregation more visible as JVM compilation's
share falls. Source polling accounts for about 41.5% of StreamFusion process CPU; native group
aggregation accounts for 24.3%, including 11.6% under its DISTINCT/row-kernel adapters. State codec
work remains small. These observations do not establish a reasonable optimization ceiling.
Flink's append-only DISTINCT code uses presence bits and avoids rewriting unchanged memberships;
StreamFusion still keeps signed counts on that path. That is a general remaining state/write
optimization, not a Q15-specific SQL rewrite.

HashMap was not remeasured for this RocksDB checkpoint change; its prior 1M result and 10M
retained-table growth failure remain the available evidence. Large canonical savepoints still
require whole-key-group buffers. Physical file restore also currently imports each key group
through a canonical buffer, so successful 20M processing/checkpointing is **not** proof of restore
capacity at that size. The focused tests establish recovery semantics at their tested state sizes;
these capacity limits remain open, as detailed in [native state](/StreamFusion/development/native-state/).

Subsequent shared group-aggregate recovery work replaces whole-group physical-file imports with
admitted Arrow key/value pages. An 8 MiB group now restores within a 4 MiB test budget including
2 MiB of cache leases, while the old whole-group snapshot is denied. Generated parity tests cover
an 8,192-member hot group, full/incremental files and aligned/unaligned restore followed by complete
retraction. This is restore correctness/capacity evidence, not a new throughput measurement or a
20M Nexmark restore result. Canonical savepoints and retained HashMap growth remain open limits;
other operator factories retain their existing canonical import adapters. Both native libraries
must now implement state-component ABI 9; see [native state](/StreamFusion/development/native-state/).

Artifacts and complete machine/runtime/command metadata are under
`streamfusion-nexmark-benchmarks/target/measurements/q15/54a5e5c0/`. The native artifact hashes,
release/native-CPU flags and upstream Flink/Nexmark revisions are unchanged from `9fc0c19c` above.

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

## Q16 admission and correctness

Original Q16 adds a VARCHAR maximum and composite channel/day grouping to the filtered
BIGINT DISTINCT counts verified for Q15. Ordinary planning now admits its complete plan
on both backends. String extrema continue to use DataFusion's MIN/MAX kernels. Admission
requires append-only inputs directly after the original Flink HASH exchange, whose binary
string comparison matches DataFusion; a Calc after the exchange, retractable strings and
DISTINCT string extrema retain precise whole-plan fallback. This is a general physical
boundary rule, without a Nexmark expression or character-set special case.

[Group aggregation](/StreamFusion/operators/group-aggregation/) records the generated Unicode,
FILTER, composite-key, full changelog/metric, memory-denial and both-backend recovery checks.
The RowData catalog preserves Q16's original SELECT and result schema. Its opt-in integration
checks pass with 50,000 events, parallelism one/four and both backends. They compare the complete
collected changelog at parallelism one, materialized results at both parallelisms, and blackhole
record counts negotiated independently of the collecting sink. All accelerated runs require
positive native activity. Independent parallel jobs may interleave channels differently; controlled
runtime tests remain the byte-parity evidence for identical input ordering. The
[corrected release comparison](/StreamFusion/benchmarks/q16-rowdata/) reports three alternating
pairs at 1M on both backends and 10M on RocksDB, plus separate longer profiles. Release
`94d996f0` gives the native database its intended Flink state-backend lease and removes the
unneeded Java RocksDB instance. Ratios are 0.487× in memory and 1.073× on RocksDB at 1M,
and 1.354× on RocksDB at 10M. The 10M ranges overlap; Flink wins one of the three pairs.
The long profile still identifies index reads/decompression as leading native CPU costs.
Historical measurements and failed memory cases remain linked from the report. This is Q16's
verified delivery checkpoint, not a claim of a performance ceiling or speedup on every backend.
Q17 is the next query checkpoint.

## Q17 original auction statistics

Original Q17 now passes ordinary whole-plan admission and is included in the RowData catalog
with its unchanged SELECT and original ten-column result schema. It groups by auction/day and
uses filtered COUNT plus MIN, MAX, AVG and SUM over BIGINT prices. All calls reuse the existing
DataFusion accumulators and the documented Flink integer-width/AVG adaptations. No operator
algorithm, query-shape special case, new configuration or admission bypass was added.

The opt-in original-SQL planning test passes for both engines/backends. Production checks use
50,000 events at parallelism one and four on both backends: exact single-source collecting
changelogs, identical parallel materializations, and independent blackhole output counts, with
positive native activity for every accelerated run. Independent parallel jobs do not establish
identical input ordering; the generated common-aggregate fixtures provide same-order byte parity.
Those fixtures also pass full metric comparisons, topology checks, canonical/backend-switch
restore, aligned/unaligned checkpoints, channel replay and rescaling. Nullable filters/arguments,
integer overflow and truncating averages are covered by the existing generated and Flink SQL
aggregate harnesses. See [group aggregation](/StreamFusion/operators/group-aggregation/).

The [release comparison](/StreamFusion/benchmarks/q17-rowdata/) completes this checkpoint at
`88d4e066`. Three alternating measured pairs per case give 0.892× / 0.939× median throughput
at 1M and 1.104× / 1.130× at 10M (hashmap / RocksDB). The 10M in-memory ranges overlap,
including a retained slow native fork; all three 10M RocksDB pairs favor StreamFusion. Separate
20M profiles on both backends identify source/copy and aggregate execution costs, with relatively
small native RocksDB costs. No query-specific algorithm or additional optimization was introduced.
This is measured local evidence, not a performance ceiling. Q18 is the next query checkpoint.


## Q18 original last bid

Original Q18 now passes ordinary whole-plan admission on both backends. Its unchanged SELECT
uses row-time `ROW_NUMBER` with `rank_number <= 1`, grouping by bidder/auction. The retained
handwritten timestamp-extremum calculation was replaced by DataFusion cumulative MIN/MAX
windows; StreamFusion supplies the Flink-specific state and per-arrival changelog adaptation.
Equal timestamps replace the winner in keep-last mode; keep-first requires strict improvement.

The compute prerequisite passes 21 Rust and 136 Java checks covering coarse workspace/history
admission, key/envelope parity, shared metrics, Arrow topology, canonical cross-backend restore,
aligned/unaligned checkpoints, 1-to-2-to-1 rescaling, incremental SST reuse and actual channel
replay. Ordinary admission separately retains precise fallback for processing-time SQL,
timer-backed insert-only row-time output, unsupported field types, TTL, async state and mini-batching.
The supported flat field types are BIGINT, INTEGER, VARCHAR and TIMESTAMP(3).

Original-SQL planning/blackhole checks and collecting validation use 50,000 events on both
backends, at parallelism one and four. One-source jobs compare the complete ordered changelog
and final result digest exactly. Parallel jobs can choose different last payloads for equal
timestamps from different readers: this source configuration has 167 such keys among 13,670
final keys. The parallel validator replays the original seeded generators and normalization,
retains each reader's final candidate, and checks every complete result payload against the
legal per-reader winners at the greatest timestamp. It checks key coverage and one row per key.
It does not discard arbitrary mismatches or claim byte-identical results across different
input-channel interleavings. Controlled shared-runtime tests retain complete byte parity for
identical arrival order. Independent original blackhole counts match at this validation size.

The [release comparison](/StreamFusion/benchmarks/q18-rowdata/) at `80df432a` completes three
alternating measured pairs at 1M events on both backends, 2M in memory and 10M with RocksDB,
plus separate longer mixed profiles. The 10M RocksDB median throughput ratio is 4.431×, with
all native forks faster and disjoint ranges. The 1M comparisons and 2M in-memory comparison
favor Flink at the median. The first 10M Flink in-memory fork failed with a TaskManager heartbeat
timeout before any native fork; no ratio or native capacity claim is made for that case.
This completes Q18's bounded admission, correctness and performance checkpoint, without claiming
a performance ceiling. Q19 is the next query checkpoint.


## Q19 original auction Top-10

Original Q19 orders bids within each auction by `price DESC` and returns ranks one through ten.
The RowData catalog now preserves that SELECT exactly. The previous catalog added timestamp,
bidder and string tie-breakers that changed which tied bids survive; those extra order keys
have been removed. The original blackhole schema includes the rank number and has no primary key.

Ordinary planner selection now accelerates the verified append-only constant-range subset,
including Q19's `[1,10]`, with the existing type/configuration restrictions. The five
`NexmarkQ19PlanningIT` checks preserve the original SELECT/schema and run the original blackhole
SQL with StreamFusion selected and unselected on both backends. Native activity is required only
for accelerated execution. Four `NexmarkQ19ProductionIT` cases compare collecting and blackhole
execution at 50,000 events on both backends and parallelism 1/4. Complete materialized result
bytes match in all cases; parallelism one also matches every ordered changelog byte and blackhole
record count. Parallel input channels can change intermediate rankings, so independent parallel
jobs do not assert identical transient changelogs. Fixed-arrival operator tests compare the
complete changelog and timestamp envelopes. The [release comparison](/StreamFusion/benchmarks/q19-rowdata/) reports 2.530× median throughput
at 2M in-memory events (wide overlapping ranges) and 3.798× at 4M RocksDB events (disjoint
ranges). The failed 4M Flink in-memory baseline and native 3M profile budget limit are explicit;
complete longer profiles use 2.5M in-memory and 8M RocksDB events.

The retained append-only constant-range computation now uses DataFusion batch ordering and
per-arrival bounded selection over fixed-width priorities. Twenty Rust checks and generated
comparisons against Flink's `AppendOnlyTopNFunction` verify this compute prerequisite, including
complete changelog bytes across ranges `[1,2]`, `[1,10]`, `[2,5]` and `[1,64]`, both backends and
three Arrow batch sizes. The subsequent output-admission prerequisite reserves descriptor
workspace in coarse chunks and actual repeated payloads before gathering or writing state;
23 focused Rust checks include both-backend refusal before writes and output ownership.
The shared binding now supports constant append-only ranges and composes with adjacent native
Calc stages. Ordered state writes encode only new final candidates; losing-only batches perform
no writes. Native coverage includes migration, retained-history admission and cross-backend
restore. Generated Java checks compare complete metrics/changelogs and exercise aligned/unaligned
checkpoints, canonical backend switches, 1-to-2-to-1 rescaling, incremental SST reuse and actual
channel replay with full Top-10 candidate sets. The final shared prerequisite passed 27 native
checks and 109 focused Java checks; ordinary admission adds one planner test and nine original-query
integration cases. These establish correctness and admission, not a performance claim.

## Q20 filtered bid/auction join

Q20 now passes ordinary admission with both Flink's default `StreamExecJoin` and its binary
MultiJoin representation. The existing shared native runtime executes both; outer, semi/anti,
cross, unique-key, unsupported residual and unsupported state/configuration subsets retain
precise whole-plan fallback. Active async-state, mini-batch and changelog-state settings are
checked even when absent from the node's persisted configuration.

The catalog preserves the original projection and category-10 predicate, adding only column
labels for the positional sink and redundant qualifications. Nine original-SQL planning cases
check catalog equivalence and execute the original blackhole schema/SELECT with both planners,
both backends and both optimizer settings. Eight collecting/blackhole cases at 50,000 events
compare complete result/changelog bytes and record counts on both backends, parallelism 1/4,
and both join choices. Independent join inputs may interleave output differently; fixed-arrival
operator tests compare the ordered changelog and timestamp envelopes.

The shared binary-join matrix runs against Flink's actual `StreamingJoinOperator` and MultiJoin
functions. All 76 metric, changelog, rescaling and checkpoint/channel-replay cases pass. Default
SQL topology guards verify one native join/Calc state owner behind two Arrow IPC input edges.
The admission checkpoint passed 19 focused unit checks and 17 original-query integration cases.
The [release comparison](/StreamFusion/benchmarks/q20-rowdata/) completes three alternating
1M-event pairs on both backends and separate longer mixed profiles. RocksDB reaches 3.928×
median throughput with disjoint ranges; in-memory throughput is 0.394× with overlapping ranges.
Native in-memory profiles at 1.5M and 2M exhaust their existing Flink allowance, while both
engines complete at 1.1M. The bounded Q20 checkpoint is documented with those limits; Q21 is next.

The benchmark preserves its established multi-join-enabled preset. The existing Flink option
can override that preset for benchmark-only validation: `-Dtable.optimizer.multi-join.enabled=false`.
This chooses Flink's default two-input join in both engines. It is not a StreamFusion deployment
option or an acceleration bypass.

## Q21 channel identifier extraction

Original Q21 now passes ordinary whole-plan admission with the restricted `REGEXP_EXTRACT`
implementation described under [projections](/StreamFusion/operators/select-where/projections/).
Its original CASE, LOWER, extraction predicate, SELECT and blackhole schema are preserved.
No channel name, URL layout or query shape is recognized specially by the implementation.

The planner projects exactly one capture, avoiding Arrow's omission of unmatched groups shifting
Java's capture indices. Rust validates the projected grammar and composes actual DataFusion
regex matching and element extraction under one coarse memory reservation. Unsupported Java
regex syntax retains precise whole-plan fallback. The initial differential probe's 46,440
matching results are supplemented by permanent generated Flink/native SQL changelog tests,
Flink-generated shared Calc metric/control harnesses and adapted upstream regex SQL cases.
Native tests cover large-buffer refusal before computation, scalar broadcasts, conditional
input selection, grammar validation and output accounting through the last slice release.

The implementation checkpoint passes 13 native checks, 32 focused Java unit checks and nine
original-query integration cases. Planning and collecting/blackhole validation exercise both backend configurations
and parallelism 1/4 at 50,000 events. Q21 is stateless, so these are not RocksDB state-performance
or recovery-capacity results. The [release comparison](/StreamFusion/benchmarks/q21-rowdata/) completes three alternating
measured pairs at 1M and 10M events and separate 20M profiles on both configurations. At 10M,
StreamFusion reaches 1.315× / 1.172× median throughput (hashmap / RocksDB configured), with
all pairs faster and disjoint ranges; both 1M comparisons favor Flink. All planned forks complete.
The bounded Q21 checkpoint is delivered; Q22 is next.

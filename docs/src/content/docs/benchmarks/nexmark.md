---
title: Nexmark benchmarks
description: The north-star Kafka-in/Kafka-out comparison.
---

The north-star benchmark compares StreamFusion with native Flink using the Nexmark streaming workload. Both engines consume the same officially generated Nexmark events from Kafka and write results back to Kafka with exactly-once delivery.

## Benchmark matrix

| State backend | Mini-batching |
| --- | --- |
| In-memory hash map | Off |
| In-memory hash map | On |
| RocksDB | Off |
| RocksDB | On |

Each case reports input-record throughput for native Flink and StreamFusion. Optimization work must keep the implementation simple and remain close to Flink's architecture while preserving result parity.

## Query suite

SQL definitions live in `streamfusion-nexmark-benchmarks/src/main/resources/nexmark/queries`. The suite tracks the queries supported by the targeted Flink release, including queries that produce updating results and therefore require an upsert-capable Kafka sink.

## Code-owned integration

The integration command provisions Kafka with Testcontainers, generates official Nexmark events, invokes both engines, reads only committed output, validates the result, and renders a throughput table. This keeps broker setup, workload generation, execution, and measurement in one reproducible path.

Run the Docker-backed integration profile with:

```shell
mvn -pl streamfusion-nexmark-benchmarks -am -Pbenchmark-integration verify
```

The benchmark is intentionally opt-in and does not run in the normal pull-request or push workflow. It provisions Kafka and runs both engines, so it should be invoked for deliberate performance and integration validation rather than for unrelated changes.

## RowData boundary variant

The Kafka-free variant uses the generator and `RowData` deserializer from the local
[`nexmark`](https://github.com/nexmark/nexmark) checkout. A thin benchmark-only table-source adapter
marks finite `events.num` runs as bounded and signals completion after assigning all four upstream
Source V2 splits; it does not replace the upstream generator or reader checkpoint state. Those
splits emit Flink internal `RowData`, and a black-hole table sink consumes `RowData` after the SQL
plan. Both source and query run at parallelism four, and checkpointing uses `EXACTLY_ONCE`. This
variant isolates planner and native-operator throughput from broker and JSON costs. The benchmark
result sink serializes and hashes both the complete sorted changelog and its final materialized
table rather than discarding it. For sinks with a declared primary key, updates are applied as
ordered upserts; keyless sinks retain multiset semantics. The raw hash detects changes to every
emitted update. The
materialized hash is also reported because processing-time checkpoint triggers may flush a
mini-batch at different input boundaries in otherwise equivalent runs, producing different valid
intermediate updates but the same final table. Controlled SQL parity tests remain the authority for
byte-for-byte changelog parity. The
bounded adapter seeds each source split deterministically and restores the upstream
generator position, so Flink and StreamFusion receive identical events. This remains a Kafka-free
operator benchmark and does not replace the Kafka-in/Kafka-out north-star benchmark.

Current all-or-nothing coverage can fully accelerate q0 (pass-through), q1 (decimal currency
conversion), q2 (selection), q3 (regular streaming join), q4 (residual join plus grouped
aggregation), q5 (nested hopping aggregates plus residual join), q7 (tumbling maximum plus a
timestamp-interval residual join), q8 (tumbling aggregates plus Flink's selected Window Join or
binary MultiJoin), q9 (residual join plus Top-1), q11
(event-time session aggregation), q19 (Top-N), q20 (regular join), q22 (URL directory extraction),
and q23 (two regular joins). A deterministic `interval-join` workload exercises
Flink's constant-bound event-time interval physical operator.
q13, q15, q16, and q17 still require an
unsupported join shape or surrounding operator. q10
uses unsupported `DATE_FORMAT`; q14 uses a Java UDF, mixed decimal
arithmetic beyond the q1 conversion shape, and timestamp calendar extraction; q21 uses Java-regex
semantics. Q12's processing-time SQL is catalogued, but a max-speed bounded source completes before
its first ten-second timer and therefore produces an empty result; it is not counted as result or
acceleration evidence. Those queries remain whole-plan Flink fallback.

Build the local Nexmark connector against this project's Flink version:

```shell
mvn -f /root/data/nexmark/pom.xml -pl nexmark-flink \
  -Dflink.version=2.3.0 -DskipTests package
```

Then run the complete currently accelerable query set through both Flink and StreamFusion:

```shell
mvn -pl streamfusion-nexmark-benchmarks -am \
  -Pbenchmark-integration,rowdata-nexmark-integration \
  -Dnexmark.generator.jar=/root/data/nexmark/nexmark-flink/target/nexmark-flink-0.3-SNAPSHOT.jar \
  -Dit.test=LocalRowDataNexmarkBenchmarkIT verify
```

`LocalRowDataNexmarkBenchmark` also accepts an event count, comma-separated query list, and an
engine selector (`flink`, `streamfusion`, or `both`) for standalone measurements. It reports
end-to-end elapsed time, input-event throughput, native calc batches, native group-aggregate
batches, native local-group-aggregate batches, native window-aggregate batches, native Window Join
batches, native regular-join batches,
native multi-join batches, native interval-join batches, native temporal-join batches, native
OVER-aggregate batches, native Temporal Sort batches, native bounded full-Sort batches, and row
counts plus SHA-256 hashes for the full result changelog and
final materialized multiset. An additional ordered SHA-256 retains sink arrival order for operators
whose ordering is semantic.
Set `-Dstreamfusion.nexmark.batch-mode=true` to run the finite RowData source through Flink's
bounded SQL planner. The harness disables periodic checkpoints in that mode. This is an execution-
mode control applied identically to both engines; it does not create a StreamFusion-only runtime.
The `aggregate-modifiers`, `incremental-group-aggregate`,
`group-aggregate`, `global-aggregate`, `grouping-sets`, `interval-join`, `over-aggregate`,
`over-aggregate-event-time`, `over-aggregate-processing-time`,
`over-aggregate-bounded-rows`, `over-aggregate-bounded-range`, `select-distinct`,
`set-intersect-all`, `top-n`,
`limit`, `bounded-limit`, `bounded-sort`, `bounded-sort-limit`, `bounded-rank`,
`legacy-window-aggregate`, `temporal-join`, and `temporal-sort` cases
exercise both native state backends over the bounded bid stream; they are focused operator workloads
rather than numbered Nexmark queries. `over-aggregate` deliberately casts the timestamp to a
regular value to exercise ordered non-time state, while `over-aggregate-event-time` retains the
rowtime attribute and exercises watermark-driven native timers and late-record handling. The
processing-time case retains Nexmark's bid filter and nested-row projection below its synthetic
`PROCTIME()` field. The bounded cases exercise a 100-row processing-time suffix and an inclusive
ten-second event-time range respectively.
`temporal-sort` orders the bounded bid stream by ascending event time and secondary price/auction
keys. Its integration case compares both the changelog multiset and global arrival-order digest on
memory and RocksDB, requires an accelerated EXPLAIN, and requires non-zero native sort batches.
`bounded-sort` exercises `StreamExecSort` with a total price/auction/bidder/timestamp/payload order.
Because Flink's bounded full-sort task rejects periodic checkpoints for its sorted input, this one
case disables the checkpoint interval for both engines; the operator recovery suite independently
covers aligned, unaligned, canonical cross-backend, and incremental RocksDB checkpoints.
`set-intersect-all` uses the three-BIGINT auction/bidder/price identity so the measured pipeline is
the complete native UNION, hash exchange, grouped-count state, Calc, and row-replication rewrite
without conflating set execution with rowtime-attribute scheduling.
`temporal-join` derives a versioned auction table with row-time deduplication and probes it from the
bid stream using an event-time left temporal join plus a residual predicate. Its integration case
compares the complete raw and materialized changelogs on both state backends, requires an
accelerated EXPLAIN, and requires non-zero native temporal-join batches.
Official q4 and q9 exercise Flink's binary `StreamExecMultiJoin` physical form. StreamFusion lowers
that node to its regular native join, preserving the expiry-column residual predicate. Their
integration cases run at parallelism one on both native state backends, compare the primary-keyed
final table with Flink, require accelerated EXPLAIN output, and require native regular-join plus
group-aggregate or Top-N activity. Controlled generated regular-join SQL fixtures separately compare
the complete byte changelog for residual INNER and SEMI shapes.
Official q5 exercises three window-aggregate stages. The maximum-over-count branch consumes the
first branch's attached start/end columns as exact namespaces, and the final residual join remains
in the fused native tree. Official q7 carries Calcite's ten-second interval as a typed protobuf
literal inside the join residual. Their integration cases compare complete raw and materialized
results on memory and RocksDB, require accelerated EXPLAIN output, and require nonzero native
window-aggregate and regular-join batches.

On the September 4, 2026 local bounded q1 run based on `5737f54` plus the bounded
VALUES/UNION/CALC working change, an excluded 100,000-event warmup preceded three alternating
fresh-JVM forks per engine over two million events at parallelism four with a 3GB heap. Flink and
StreamFusion median elapsed times were 12.971s and 13.530s, equivalent to 154,193 and 147,819
events/s or 95.9% throughput parity. Median absolute deviations were 0.236s and 0.079s, with
elapsed ranges of 12.732–13.207s and 13.195–13.609s. Every fork emitted 1,840,000 rows with final
multiset SHA-256 `2dc67c3298d35ddb49512cfd8467e86b6e1ec6d2cc550f78a006e0d5fc96b913`;
every StreamFusion fork reported a fully accelerated bounded plan and executed 124 native Calc
batches. q1 is stateless, so a RocksDB-labelled repeat would exercise no RocksDB state and is not
reported as a distinct backend result.

Separate two-million-event mixed JVM/native CPU, Java-allocation, and native-allocation profiles
used Java non-safepoint sampling, native DWARF/frame-pointer unwinding, JFR, collapsed stacks,
per-engine flame graphs, and differential flame graphs. Profiler timings were excluded from the
measurements above. The native DataFusion Calc accounted for 0.22% of CPU samples and Arrow C/JNI
transport for 1.25%; the required RowData-to-Arrow source boundary and Arrow-backed output access
accounted for 13.8% and 11.5% respectively. Sampled Java allocation volume was 0.11% lower than
Flink's. Native profiles attributed the additional StreamFusion bytes to the computed decimal and
filter output buffers, not to a defensive whole-batch handoff copy. Those buffers and the reusable
DataFusion plan are admitted through the existing Flink task managed-memory pool. Profiles are
retained under `streamfusion-nexmark-benchmarks/target/profiles/batch-q1/`; these are local
diagnostic results, not portable performance guarantees.

On the September 4, 2026 local release/native-CPU run, q5 used an excluded 50,000-event warmup and
three alternating fresh-JVM forks per engine/backend over 250,000 events at parallelism one, with a
3GB heap and one-second exactly-once checkpoints. In memory, Flink and StreamFusion median elapsed
times were 6.864s and 6.921s (99.2% throughput parity), with MADs of 0.013s and 0.032s and ranges of
6.851–6.934s and 6.712–6.953s. RocksDB medians were 7.441s and 6.908s, a 7.7% StreamFusion gain;
MADs were 0.112s and 0.037s and ranges were 7.286–7.553s and 6.871–7.009s. Every fork emitted the
same five rows with SHA-256 `2abc9d4941381e8824e2ae1bcb08465adaa8aca4ae87254c7a8103c01a10275d`.

Q7 used a larger excluded 250,000-event warmup before three alternating 500,000-event in-memory
forks. Flink and StreamFusion medians were 7.548s and 8.065s, equivalent to 66,243 and 61,997
events/s or 93.6% throughput parity. MADs were 0.024s and 0.083s and ranges were 7.455–7.572s and
7.853–8.148s. The three RocksDB forks had medians of 11.242s for Flink and 8.997s for StreamFusion,
a 25.0% native gain. Their ranges were 9.368–16.982s and 8.976–23.308s; the corresponding MADs
were 1.874s and 0.021s, and the full ranges retain one storage/scheduling outlier per engine. All
forks emitted the same row with SHA-256
`0e9bdea3e9fba68cd92431e33df2085908404c745bd732a1c80cdf80a0fd0dde`, reported full acceleration,
and executed nonzero native Calc, window-aggregate, and regular-join batches. Profiler timings were
excluded from these results.

The first benchmark-scale q5 native memory run exposed an undersized Flink managed-memory share:
stateful window aggregates had inherited a stateless weight of one while the equivalent native
deduplicate, grouped-aggregate, join, and Top-N stages use eight. Both modern and legacy window
aggregate planners now request the established stateful weight, and an opt-in 250,000-event
regression test covers the standard managed-memory envelope. Mixed JVM/native CPU and wall-clock
profiles, Java allocation captures, native-allocation captures, collapsed stacks, per-engine flame
graphs, and differential flame graphs cover q5 and q7 on both backends. StreamFusion Java
allocation samples were 32–41% lower than Flink's. In CPU samples, q7's native window aggregate was
0.27–0.33% and its regular join was 2.30% in memory and 5.55% on RocksDB; q5's native window
aggregate was 4.33% and 8.51%. Native allocation samples under those stateful paths were dominated
by canonical durable row/state encoding plus expected RocksDB buffers, with no per-record JNI or
whole-batch copy loop. The direct RocksDB operators retain one `multi_get` and one atomic
`WriteBatch` per incoming Arrow batch. Profiles are retained under
`streamfusion-nexmark-benchmarks/target/profiles/q5-q7-attached-window/`. These measurements are
local diagnostics, not portable performance guarantees.

The bounded source adapter also replaces the upstream generator's process-random reusable payload
and URL caches with stable, size-preserving values. Consequently, fresh JVM forks consume
byte-identical events rather than relying on same-process parity.
`aggregate-modifiers` includes ordinary and counted-distinct AVG plus an explicit
`BIGINT`-to-`DECIMAL` AVG, so release profiles cover both fixed-width integer and exact decimal
sum/count state rather than only the aggregate modifier dispatch.
`incremental-group-aggregate` enables Flink's split-DISTINCT optimization and exercises the native
local aggregate, row expansion, incremental aggregate, and final aggregate pipeline. Its
integration test suppresses wall-clock checkpoint flushes so only deterministic count-triggered
bundle boundaries affect the comparison.
Performance reports
must come from separate, unprofiled JVM forks built with release-mode native code; profiler runs
are diagnostic artifacts rather than benchmark measurements.

On the September 4, 2026 local `legacy-window-aggregate` run based on `b0a3b97` plus the
legacy-window working change, three alternating fresh-JVM release/native-CPU forks processed
500,000 deterministic events at parallelism one with a 2GB heap and one-second exactly-once
checkpoints. In memory, Flink and StreamFusion medians were 83,173 and 81,043 events/s
respectively, or 97.4% throughput parity; median absolute deviations were 0.011s and 0.213s, with
elapsed ranges of 6.001–6.755s and 5.953–6.383s. RocksDB medians were 79,011 and 80,887 events/s,
a 2.4% StreamFusion gain; median absolute deviations were 0.026s and 0.011s, with elapsed ranges
of 6.228–6.354s and 6.171–6.372s. Every fork materialized the same 9,920-row multiset with
SHA-256 `495d281e404c8e1cbd44d4ba9bb275c84ea61f635148c2702a5a50d83f8a45da`.
StreamFusion EXPLAIN reported full acceleration and every fork executed 31–32 native window
batches. The host was a 12th Gen Intel Core i7-12650H under x86-64 WSL2 with OpenJDK 24.0.2 and
Rust 1.94.0.

Separate one-million-event mixed JVM/native profiles covered both engines and both backends with
Java non-safepoint sampling, DWARF/frame-pointer unwinding, collapsed stacks, per-engine flame
graphs, differential flame graphs, Java allocation recordings, and native allocation recordings.
Profiler timings were excluded from the measurements above. The complete native window path
accounted for 2.6% of in-memory CPU samples and 3.5% with RocksDB; RowData-to-Arrow conversion
accounted for 2.5% and 2.8%. Sampled Java allocation volume was 48% of Flink's in memory and 44%
with RocksDB. Native allocation profiles identified a temporary BinaryRow key and assigned-window
list per input row. Reusing those two buffers across each Arrow batch reduced their post-change
shares to 0.0001% and less than 0.0001% of sampled native allocation volume. Owned canonical
state keys, accumulators, output Arrow buffers, and RocksDB allocations remain charged through
the operator's Flink managed-memory reservations.

On the September 4, 2026 local q23 multi-join run based on `980201e` plus the multi-join working
change, three alternating fresh-JVM release/native-CPU forks processed 500,000 deterministic events
at parallelism four with a 3GB heap and one-second exactly-once checkpoints. In memory, Flink and
StreamFusion medians were 46,077 and 46,649 events/s respectively, a 1.2% StreamFusion gain; median
absolute deviations were 0.723s and 0.440s, with elapsed ranges of 10.128–14.170s and
10.278–19.050s. RocksDB medians were 39,992 and 45,011 events/s, a 12.6% StreamFusion gain; median
absolute deviations were 0.093s and 0.248s, with elapsed ranges of 12.409–17.358s and
10.527–11.357s. The wide ranges, including one storage/scheduling outlier, are retained rather
than discarded. Every run emitted and materialized 515,539 rows with SHA-256
`2b5f85f35733140d63a71f8dba79235f56152e97867c5728d203456504e52110`; StreamFusion EXPLAIN
reported full acceleration and each fork executed 388–400 native multi-join batches. The host was a
12th Gen Intel Core i7-12650H under x86-64 WSL2 with OpenJDK 24.0.2 and Rust 1.94.0.

Separate longer 500,000-event, parallelism-one mixed JVM/native profiles covered both engines and
backends with Java non-safepoint sampling, DWARF/frame-pointer unwinding, CPU, Java-allocation, and
native-allocation JFRs, collapsed stacks, per-engine flame graphs, and differential flame graphs.
Profiler timings were excluded from the throughput measurements. They found Flink's streaming
multi-join path at 50.8% of RocksDB CPU samples, including 45.1% in its Java RocksDB path, while
StreamFusion's complete native multi-join and direct RocksDB work accounted for 21.0% and 9.4%.
In memory, those complete join paths accounted for 17.3% and 13.4% respectively. The profiles led
to removal of a redundant identity gather after Arrow row decoding and recursive deep clones of
candidate rows. The native operator performs one distinct multi-get and one atomic write batch per
incoming Arrow batch, with no per-row JNI state access. Its state, scratch, exported Arrow, RocksDB
cache, and write-buffer allocations remain admitted through Flink managed memory. These are local
diagnostic results, not portable performance claims.

On the September 4, 2026 local `temporal-join` run based on `b3fb8ed` plus the temporal-join
working change, three alternating fresh-JVM release/native-CPU forks processed 500,000
deterministic events at parallelism four with a 3GB heap, one-second exactly-once checkpoints, and
the same 8GB Flink managed-memory setting for both engines. In memory, Flink and StreamFusion
medians were 75,156 and 71,295 events/s respectively, or 94.9% throughput parity; median absolute
deviations were 0.019s and 0.087s, with elapsed ranges of 6.372–6.672s and 6.780–7.101s. RocksDB
medians were 60,677 and 69,773 events/s, a 15.0% StreamFusion gain; median absolute deviations were
0.588s and 0.134s, with full elapsed ranges of 7.653–10.911s and 7.032–11.125s. The final RocksDB
fork for each engine encountered the same local storage/scheduling outlier, which is retained in
the ranges. Every run emitted and materialized 460,000 rows with SHA-256
`fe511cd37719f5a419bf980d276ec0cbdbc0ae193bd7e535a88be5bbd6d69c0c`; StreamFusion EXPLAIN
reported full acceleration and each fork executed 260–268 native temporal-join batches. The host
was a 12th Gen Intel Core i7-12650H under x86-64 WSL2 with OpenJDK 24.0.2 and Rust 1.94.0.

Separate mixed JVM/native CPU profiles for both engines and backends used Java non-safepoint
sampling, DWARF/frame-pointer unwinding, JFR, collapsed stacks, per-engine flame graphs, and
differential flame graphs. Java and native allocation recordings were captured separately; all
profiler timings were excluded from the measurements above. An all-candidates-pass fast path
removed an unnecessary residual-condition output copy, reducing that bridge from 3.0%/2.4% to
0.6%/0.1% of CPU samples on memory/RocksDB. In the final profiles, native temporal processing plus
timer firing accounted for 2.5%/2.2%, while the shared RowData-to-Arrow input boundary accounted
for 3.8%/4.1%. Java allocations remained led by Flink binary-row copying and the benchmark result
collector; temporal residual materialization accounted for 1.6%/0.7%. Native allocation samples
were concentrated in the required watermark-triggered output batch and, on the RocksDB path, the
database itself. Incoming batches perform one distinct native multi-get and one atomic write batch,
and no per-row JNI state access or decoded-object cache was introduced. Native state, timer,
scratch, exported Arrow, RocksDB cache, and write-buffer allocations remain admitted through the
operator's existing Flink managed-memory reservation. These are local diagnostic results, not
portable performance claims.

The RowData harness uses 1,024 MiB of Flink managed memory by default and sets Flink's standard
managed-memory consumer weights to `OPERATOR:90,STATE_BACKEND:10,PYTHON:30` for both engines. The
bounded workloads have small RocksDB working sets but retain multiple Arrow batches and native
operator scratch buffers, so the default Flink 50/50 split between the two active consumers is not
representative. Pass the ordinary
`-Dtaskmanager.memory.managed.consumer-weights=OPERATOR:...,STATE_BACKEND:...,PYTHON:...` property to
override the harness choice; StreamFusion does not introduce a separate deployment memory budget.

On the September 4, 2026 one-million-event `incremental-group-aggregate` run for commits `00d5bfe`
and `c946c63`, three alternating fresh-JVM forks at parallelism one measured 94.6% in-memory
throughput parity and a 9.8% StreamFusion RocksDB gain. Every fork materialized the same 19,913-row
final multiset and SHA-256
`43fc431b4c20fd8c9cf6aacae7f0c4b7332e6f470b4386cf910d66301081ad49`. One checkpoint/storage
outlier appeared for each engine on RocksDB, so the full elapsed ranges—13.760–19.417s for Flink
and 12.745–28.031s for StreamFusion—are retained rather than hidden. The detailed environment,
throughput, and mixed-stack profile breakdown is recorded on the group-aggregation operator page.

On the September 3, 2026 local release/native-CPU run over one million deterministic events,
`global-aggregate` produced in-memory medians of 107,827 events/s for Flink and 103,550 events/s
for StreamFusion (96.0% parity), and RocksDB medians of 99,061 and 106,810 events/s (a 7.8%
StreamFusion gain). All forks emitted 1,839,999 rows with SHA-256
`def58ec236efbd1b8d4230f25681e86ef79a487155cd47791631558c0d9d299a`.

The `grouping-sets` workload produced in-memory medians of 75,963 events/s for Flink and 78,222
events/s for StreamFusion (a 3.0% gain), and RocksDB medians of 57,959 and 71,869 events/s (a 24.0%
gain). All forks emitted 3,650,083 rows with SHA-256
`25375393fce85edf36dd09d91a045ff75af3a0470896ff1d242ec447058a86ea`. Mixed JVM/native CPU,
Java-allocation, and native-allocation profiles cover both engines and backends. They led to a
global-key fast path that materializes the canonical empty key once per batch; the final native
aggregate path accounts for only 1.2–1.3% of global CPU samples. Profiler timings were excluded
from these measurements.

On the September 4, 2026 local release/native-CPU pre-AVG `aggregate-modifiers` run for
implementation commit `ba4fffb`, three alternating fresh-JVM forks processed one million
deterministic events at parallelism one with a 3GB heap and one-second exactly-once checkpoints.
In-memory Flink and
StreamFusion medians were 108,549 and 109,276 events/s respectively, a 0.7% StreamFusion gain;
elapsed-time ranges were 9.065–9.243s and 8.908–11.620s. RocksDB medians were 75,337 and 96,157
events/s, a 27.6% gain, with elapsed ranges of 12.820–16.063s and 8.966–10.672s. Every run emitted
984,827 changelog rows with SHA-256
`07c7088abc7c53ae58d9d84e8e5b7a1c6d5cee1279796889825ee3638cafa56a`; native Calc and
GroupAggregate counters were non-zero in every StreamFusion fork.

Mixed JVM/native CPU, Java-allocation, and native-allocation profiles cover both engines and both
backends. The complete native GroupAggregate path accounted for 7.0% of in-memory CPU samples and
7.5% with RocksDB; state encode/decode and ordered distinct-set work were each at or below 1.1%,
and RocksDB itself accounted for about 1.1%. Rebuilding canonical opaque accumulator values
accounted for 34–39% of sampled native allocation bytes, but those transient bytes are admitted by
the operator's Flink managed-memory scratch reservation. Java allocation remained dominated by
generated rows, binary materialization, and result collection. No decoded object cache was added:
the runtime state remains the same backend-neutral opaque-byte representation used by canonical
savepoints. Profiler timings were excluded from the throughput measurements. The machine was a
12th Gen Intel Core i7-12650H under x86-64 WSL2 with OpenJDK 24.0.2 and Rust 1.94.0.

The AVG-expanded workload at implementation commit `0e89128` adds ordinary integral AVG,
counted-distinct integral AVG, and decimal AVG to the same modifier mix. Three new alternating
fresh-JVM forks over one million events reached in-memory medians of 99,298 events/s for Flink and
94,383 events/s for StreamFusion (95.0% parity), with elapsed ranges of 9.879–11.613s and
10.453–10.681s. RocksDB medians were 44,910 and 91,263 events/s respectively, a 103.2%
StreamFusion gain, with elapsed ranges of 15.224–26.728s and 10.928–11.026s. All twelve runs
emitted 1,820,087 byte-identical changelog rows with SHA-256
`f6b14f624b1151e518c892de4c0cf9c78d907a3c59db2c4f7ab37d0906d0f2f0`; every StreamFusion
fork reported nonzero native Calc and GroupAggregate batches.

Mixed CPU, Java-allocation, and native-allocation JFR profiles were retained for both engines and
backends, with DWARF native unwinding and differential flame graphs. They identified repeated
arbitrary-precision power/scaling work in decimal AVG; replacing the common path with precomputed
Arrow `i256` powers raised the in-memory median from 83,820 to 94,383 events/s while preserving an
exact BigInt fallback. In the final profiles the complete native GroupAggregate path occupied
9.4–10.0% of CPU samples, native decimal arithmetic occupied 1.9%, and the BigInt fallback had no
samples. Decimal arithmetic contributed only 0.12–0.17% of sampled native allocations. Total
sampled Java allocation volume was about 54% of Flink's in memory and 49% with RocksDB; native
state, Arrow, and RocksDB allocations remained charged through the existing managed-memory and
scratch reservations. Profiler timings were excluded from the throughput numbers above.

On the September 3, 2026 local release/native-CPU `over-aggregate` run over 100,000 deterministic
events at parallelism one, three alternating fresh-JVM forks produced in-memory medians of 20,908
events/s for Flink and 20,759 events/s for StreamFusion (99.3% parity). Elapsed-time ranges were
4.677–4.930s and 4.791–4.875s. RocksDB medians were 16,887 and 19,843 events/s, a 17.5%
StreamFusion gain, with elapsed ranges of 5.879–6.037s and 5.017–5.175s. Every run emitted 92,000
rows with SHA-256 `9cdcca648552898214b792466466ca07ccf3af86de0663a881d9930e38036144`, and every
StreamFusion run reported seven native OVER batches.

The corresponding true event-time workload produced in-memory medians of 20,934 events/s for Flink
and 20,642 events/s for StreamFusion (98.6% parity), and RocksDB medians of 16,778 and 19,813
events/s respectively (an 18.1% StreamFusion gain). All forks again emitted the same 92,000 rows and
hash and reported seven native OVER batches. Its mixed CPU profiles put the combined native input
and watermark paths below 3.5%; timer registration was about 1%, and state encoding/decoding,
timer snapshots, and individual RocksDB calls were each below 0.4%. Allocation profiles were led by
required output materialization and sink serialization rather than a CPU-significant state-loop
allocation. Profiler timings were excluded from the measurements.

Mixed JVM/native CPU profiles over a longer 600,000-event run used Java non-safepoint sampling,
DWARF/frame-pointer unwinding, JFR, collapsed stacks, and differential flame graphs for both
backends. Prefix seeding removed the append-heavy full-state scan: full recomputation was 0.11% of
samples, suffix recomputation was 0.42–0.74%, and the complete OVER processor was 2.1–2.4%. The
required RowData-to-Arrow source boundary remained larger at 4.5–5.5%. Java and native allocation
profiles found no dominant OVER state-loop allocation. Profiler timings were excluded from the
benchmark above; these are local diagnostic results rather than portable performance claims.

The processing-time workload was measured over two million deterministic events at parallelism
one. Three alternating fresh-JVM forks produced in-memory medians of 175,965 events/s for Flink and
185,511 events/s for StreamFusion, a 5.4% gain; elapsed ranges were 10.677–11.500s and
10.398–10.833s. RocksDB medians were 151,893 and 152,452 events/s respectively (0.4% gain), with
wider elapsed ranges of 12.813–14.811s and 10.767–15.856s. All runs emitted 1,840,000 rows with
SHA-256 `667f3fe5bdcc5417f2aed194286f9ecfd983a245ce08a2e316b3f579705b2e55`; native
OVER and Calc counters were non-zero in every StreamFusion fork. The compact append-only
processing-time state removed the pre-optimization managed-memory exhaustion at this scale.

Separate mixed JVM/native profiles over three million events covered both engines and backends,
including Java and native allocation events. The native OVER call tree was 15.8% of memory-backend
and 19.1% of RocksDB CPU samples without a dominant leaf. Native RocksDB work was 4.2% versus
14.5% in Flink's RocksDB profile, and the source RowData-to-Arrow boundary was about 7%. Output and
Arrow materialization led allocation samples; historical input rows were no longer retained.
Profiler timings were excluded from the throughput measurements.

The bounded OVER workloads were measured over one million deterministic events in three
alternating fresh-JVM forks. `over-aggregate-bounded-rows` produced in-memory medians of 125,804
events/s for Flink and 125,718 events/s for StreamFusion (99.9% parity); elapsed ranges were
7.870–7.992s and 7.867–8.053s. RocksDB medians were 55,406 and 120,289 events/s, a 2.17x native
gain, with ranges of 17.709–18.231s and 8.193–8.337s. Every run emitted 920,000 rows with SHA-256
`66d6a3d626e845ebd5f0e05af7a4c8b44e099284000de995b309699f011fd037`.

`over-aggregate-bounded-range` produced in-memory medians of 116,211 events/s for Flink and
111,770 events/s for StreamFusion (96.2% parity), with ranges of 8.122–8.760s and 8.780–9.052s.
RocksDB medians were 35,446 and 108,597 events/s, a 3.06x native gain; elapsed ranges were
20.363–30.669s and 8.879–19.310s. The wide RocksDB ranges are reported because local storage
variance was substantial. Every run emitted 920,000 rows with SHA-256
`6efa4c099108291ada48a6bde539fffd227870454a8702cf4e19de8e5ea12108`. All native forks reported
non-zero OVER and Calc batch counters.

Separate 500,000-event mixed-stack profiles retained CPU, Java-allocation, and native-allocation
JFRs, collapsed stacks, flame graphs, and differential flame graphs. Native OVER accounted for
4.9–5.6% of CPU samples, timer work for 1.9–2.0%, and direct native RocksDB for 1.5%. The explicit
RowData/Arrow edges plus JNI accounted for approximately 8–9% in this operator-isolation topology.
Java allocation volume was 2.70 GB for StreamFusion versus 4.55 GB for Flink in memory and 2.71 GB
versus 30.06 GB on RocksDB. The final implementation keeps one earliest event timer per partition,
defers timer-state serialization to checkpoint boundaries, and uses a compact canonical codec that
restores older fixed-width versions. Profiler timings were excluded from throughput results.

On the September 1, 2026 local release/native-CPU `select-distinct` run over two million generated
events, the native in-memory path processed 365,779 events/s versus Flink's 377,002 events/s
(97.0% parity). Native RocksDB processed 351,723 events/s versus Flink RocksDB's 343,327 events/s
(a 2.4% gain). Separate JFR samples for both native backends were dominated by Nexmark generation,
RowData materialization, and exchange-boundary work; they did not expose an obvious
SELECT-DISTINCT state-loop allocation or Java hot spot. These are local diagnostic results, not
portable performance claims.

On the September 2, 2026 local release/native-CPU `top-n` run over one million generated events at
parallelism one, three alternating fresh-JVM forks produced in-memory medians of 107,862 events/s
for Flink and 102,250 events/s for StreamFusion (94.8% parity). The observed elapsed-time ranges
were 9.09–9.43s and 9.62–9.84s respectively. Fresh RocksDB forks produced medians of 76,981
events/s for Flink and 89,876 events/s for StreamFusion, a 16.8% throughput gain; their wider
elapsed-time ranges were 10.42–13.68s and 10.11–14.65s. Every run emitted 1,475,261 changelog
rows. The byte-for-byte dual-engine integration test passed on both backends; standalone hashes can
differ because the four deterministic source splits have nondeterministic network interleaving,
which changes valid intermediate Top-N updates.

Mixed JVM/native profiles used Java non-safepoint sampling, DWARF unwinding, allocation sampling,
and differential flame graphs. Replacing one IPC stream and gather per touched partition with one
vectorized reversible Arrow-row conversion removed the dominant state path: final in-memory samples
attribute 0.28% to Arrow-row conversion, 0.16% to output-only interleave, and no samples to state IPC
writers. RocksDB `multi_get` and direct batch writes remain small compared with the RowData/Arrow,
exchange, and result-sink boundaries. Allocation samples are led by result serialization, Nexmark
source string materialization, and Flink `RowData` copies; native state and scratch allocations are
covered by managed-memory admission tests. Raising the adaptive Arrow target from 8,192 to 16,384
halved native batch calls. A 32,768 target was rejected after fail-fast validation showed its
24.9MB state/scratch peak exceeded the memory left after the RocksDB cache reservation. These are
local diagnostic results rather than portable performance claims.

On the September 2, 2026 local release/native-CPU `limit` run over five million generated events at
parallelism one, three alternating separate-JVM forks produced in-memory medians of 242,268
events/s for Flink and 252,779 events/s for StreamFusion, a 4.3% throughput gain. Elapsed-time
ranges were 20.51–20.71s and 19.74–20.11s. RocksDB medians were 238,805 events/s for Flink and
245,162 events/s for StreamFusion, a 2.7% gain, with elapsed ranges of 20.84–21.39s and
19.71–20.55s. Every StreamFusion fork executed the native LIMIT batch once; the remaining roughly
620 native batches belong to the projected Calc before Flink's required singleton exchange.
Byte-for-byte same-run parity passed on memory and RocksDB.

Mixed JVM/native async-profiler runs used Java non-safepoint samples, DWARF native unwinding, JFR,
and separate Java/native allocation events. Profiling first found the generic Top-N loop consuming
14.2% of CPU and 7.6% of native allocation samples after the limit was already full. The final path
uses count-only state, avoids per-row selected-range copies, and bypasses Arrow C Data after
saturation. Current profiles attribute at most 0.1% of CPU and 0.2% of allocation samples to LIMIT
on memory. RocksDB attributes about 0.1% CPU, 0.2% Java allocation samples, and 0.4% native
allocation samples to LIMIT, while RocksDB itself remains below 0.7%. RowData-to-Arrow conversion,
exchange, source materialization, and GC are now the visible costs. These are local diagnostic
results, not portable performance claims; profiler timings were excluded from the benchmark.

On the September 4, 2026 release/native-CPU `temporal-sort` run at commit `1e1e98d`, three
alternating fresh-JVM forks used a 2 GiB heap, parallelism four, and one-second exactly-once
checkpoints. At 100,000 events, in-memory Flink and StreamFusion medians were 4.756 s and 5.260 s
respectively (90.4% throughput parity), with elapsed ranges of 4.668–5.240 s and 5.222–5.423 s.
At 20,000 events on RocksDB, medians were 24.669 s and 4.819 s, a 5.12x StreamFusion gain; ranges
were 22.975–40.518 s and 4.728–5.375 s, retaining Flink's storage outlier. All forks agreed on both
the unordered and global ordered SHA-256, and all StreamFusion forks reported native Temporal Sort
batches.

Mixed CPU and separate JVM/native allocation profiles cover both engines and backends. Temporal
Sort occupied 5.1% of in-memory and 2.8% of RocksDB StreamFusion CPU samples, while inclusive
Arrow/native boundary work occupied 9.2% and 6.1%. Flink RocksDB work occupied 62.8% of its CPU
samples versus 2.1% for direct native RocksDB. JVM allocation was 87.11 GB for Flink RocksDB and
0.65 GB for StreamFusion; native allocator samples were 89.35 GB and 3.65 GB. Profiling found and
removed repeated growth of the canonical row-state buffer; its native allocation share fell from
8.0% to 2.9% after exact pre-sizing. The complete artifacts are retained under
`streamfusion-nexmark-benchmarks/target/profiles/temporal-sort/1e1e98d/`. Profiler timings are not
benchmark results.

On the September 1, 2026 local release/native-CPU Q11 run over five million deterministic events,
StreamFusion's in-memory session aggregate processed 648,986 events/s versus Flink's 627,817
events/s. Direct native RocksDB processed 526,714 events/s versus Flink RocksDB's 361,980 events/s.
All four runs produced 99,930 result rows and SHA-256
`7bd08ff1fbf1713ed178f847657729a5b69e4fae28d0f7626a5fc71327217d62`. Native CPU profiling
found and removed two quadratic paths: replaying every historical session contribution and scanning
the complete timer set after every registration. Post-fix memory profiles are led by timer snapshot,
session merge, Arrow filtering, state-batch, and key encoding work rather than one dominant loop.
RocksDB samples are primarily skip-list lookup/insertion, comparison, compaction, and `MultiGet`.
JFR allocation profiles for both backends are dominated by the Nexmark generator, Flink `RowData`
and binary-string materialization, memory-segment wrapping/copying, and Arrow envelopes; all custom
Rust state, timer, scratch, RocksDB cache/write-buffer, and exported Arrow allocations remain charged
to Flink managed memory. These are local diagnostic results, not portable performance claims.

On the same machine, a parallelism-four Q8 release run over one million deterministic events
produced 10,562 rows and SHA-256
`5e77d99014bf46d7710eec84aaf795005ccdf71a9c023a2094c6e837a2207fab` in all four cases.
StreamFusion memory processed 169,345 events/s versus Flink's 180,375 events/s (93.9% parity).
Direct native RocksDB processed 157,744 events/s versus Flink RocksDB's 76,588 events/s (2.06x).
Separate JFR runs, excluded from those measurements, found no dominant Java Window Join loop: CPU
samples were led by Arrow/RowData field access, Nexmark generation, binary-row materialization, and
Flink unsafe-memory checks. Allocation samples were led by the required opaque `BinaryRowData`
copies, memory-segment wrappers, generated strings/rows, Arrow foreign-buffer wrappers, and source
deserialization. Native state, timer, scratch, output, RocksDB cache, and write-buffer allocations
are covered by host-memory reservation tests and remain charged to Flink managed memory. These are
local diagnostic results, not portable performance claims.

Flink 2.3 can normalize Q8's equality join over attached window bounds to a binary `MultiJoin`
instead of retaining `StreamExecWindowJoin`, depending on the optimizer shape. The native planner
lowers that binary form to the regular-join kernel while preserving the keys and condition.
Integration activity checks therefore accept either the native Window Join or native regular-join
counter, while still requiring exact output parity; they do not infer a physical node from the SQL
query name.

On the September 3, 2026 local Q3 run for implementation commit `f6acfb2`, three alternating,
separate-JVM release/native-CPU forks processed twenty million deterministic events at parallelism
four with a 3GB heap and one-second exactly-once checkpoints. In-memory Flink and StreamFusion
medians were 1,336,413 and 1,401,743 events/s respectively, a 4.9% StreamFusion throughput gain;
elapsed-time ranges were 14.40–17.90s and 14.03–16.70s. RocksDB medians were 1,225,336 and
1,358,909 events/s, a 10.9% gain, with elapsed ranges of 14.70–16.63s and 14.39–15.56s. Every run
emitted 118,854 changelog rows with SHA-256
`3cafabe988b835ccba74da12819f443503bbb08b33f101072d73b5169aa92060`; native regular-join batch
counters were non-zero in every StreamFusion fork. The machine used a 12th Gen Intel Core
i7-12650H under x86-64 WSL2, OpenJDK 24.0.2, and Rust 1.94.0. Profiler-instrumented timings were
excluded from these benchmark results.

Mixed JVM/native CPU profiles used Java non-safepoint sampling, DWARF/frame-pointer unwinding, JFR,
collapsed stacks, flame graphs, and differential flame graphs for both state backends. Reusing the
exchange envelope directly removed the join's second RowKind-vector construction and Arrow gather;
masking children of null nested rows removed repeated validity writes without corrupting variable-
width offsets. After those changes, in-memory samples attributed 25.7% to the shared source
RowData-to-Arrow boundary, 6.1% to JNI/Arrow transport, 1.6% to Arrow selection, 1.2% to the join
processor, and 0.6% to memory state. The RocksDB profile attributed 24.0%, 7.0%, 1.4%, 1.7%, and
4.0% to the corresponding paths. Native RocksDB performs one distinct `multi_get` and one write
batch per incoming Arrow batch.

Separate Java and native allocation profiles found no dominant regular-join allocation path. Java
samples were led by Flink's required `RowDataSerializer.copy` work (about 72% on both backends);
the join bridge was about 2%, while RocksDB itself was 0.16% in the Rocks run. In native samples,
the source conversion, Arrow selection, and JNI transport remained larger than join state;
in-memory state was 0.08% and RocksDB was 0.81%. The wider source boundary retains a 16,384-row
adaptive target and receives two shares of Flink's existing managed-memory budget so reusable and
in-flight exported buffers are both admitted. An 8,192-row experiment was rejected because it
doubled native invocations. These are local diagnostic results, not portable performance claims.

Official q4 and q9 residual-join measurements were run on September 4, 2026 from the working tree
based on `7543e04`. Release/native-CPU binaries, a 2 GiB heap, parallelism four, one-second
exactly-once checkpoints, alternating engine order, and separate JVMs were used throughout. Three
250,000-event q4 forks produced in-memory medians of 39,339 events/s for Flink and 39,559 events/s
for StreamFusion, a 0.6% native gain. Elapsed medians were 6.355 s and 6.320 s, MADs were 0.019 s
and 0.022 s, and ranges were 6.336–6.374 s and 6.298–6.374 s. RocksDB medians were 29,433 and
33,442 events/s, a 13.6% native gain; elapsed medians were 8.494 s and 7.476 s, MADs were 0.191 s
and 0.149 s, and ranges were 7.332–8.685 s and 6.449–7.625 s.

At 250,000 events, q9's in-memory medians were 38,564 events/s for Flink and 36,079 events/s for
StreamFusion, or 93.6% parity; one native fork stretched the elapsed range to 6.800–9.712 s versus
6.298–6.679 s for Flink. Because startup and checkpoint scheduling dominated that short run, the
same alternating comparison was repeated at 500,000 events. Flink's elapsed median was 14.872 s
(MAD 1.490 s, range 12.326–16.362 s) and StreamFusion's was 9.521 s (MAD 0.837 s, range
8.684–22.642 s), a 56.2% median native throughput gain while retaining the native storage outlier.
At 250,000 events on RocksDB, elapsed medians were 8.804 s for Flink and 7.306 s for StreamFusion,
a 20.5% native gain; MADs were 0.068 s and 0.199 s, with ranges of 8.736–12.210 s and
7.107–14.263 s. The wide ranges are reported rather than discarded.

Each same-size q4 or q9 comparison agreed on its primary-keyed final-table digest. Raw changelogs
can differ because independently scheduled bounded inputs may produce different legal intermediate
updates; controlled generated residual INNER and SEMI fixtures retain byte-for-byte changelog and
metric parity coverage. Every StreamFusion fork reported non-zero regular-join and surrounding
aggregate or Top-N batch counters, and EXPLAIN reported whole-plan acceleration.

Post-optimization mixed JVM/native CPU profiles cover q4 and q9 on both engines and backends. The
native regular-join path accounts for about 0.6% of q4 and 2.0–2.3% of q9 CPU samples; residual
DataFusion evaluation itself is about 0.06% after replacing per-record expression calls with one
candidate-pair evaluation per incoming Arrow batch. Direct native RocksDB is 1.5–1.9% of the
StreamFusion profiles versus 10.0–10.4% in Flink's profiles. The remaining visible costs are
source/result materialization, Arrow-row conversion, exchange, checkpoint scheduling, and GC.

Separate 250,000-event JVM-allocation profiles measured 2.03 GiB for StreamFusion versus 3.31 GiB
for Flink on in-memory q4 and 1.99 versus 3.61 GiB on RocksDB. Q9 measured 2.98 versus 4.69 GiB in
memory and 3.00 versus 5.26 GiB on RocksDB. Native-allocation profiles show q9's join volume in
required Arrow-row output/state conversion and, on RocksDB, the single batched key/value and write-
batch construction; repeated residual evaluation is not visible. Both the memory and direct
RocksDB paths issue one distinct batch fetch and at most one atomic batch write per incoming Arrow
batch. Profiling also found and fixed decoded exchange accounting that charged one shared IPC
payload once per sliced column. JFRs, collapsed stacks, flame graphs, and differential graphs are
retained under `streamfusion-nexmark-benchmarks/target/profiles/residual-join-q4-q9-final/` and
`residual-join-q4-q9-final-alloc/`. Profiler timings were excluded from throughput results. These
are local diagnostics, not portable performance claims.

On the September 3, 2026 local constant-bound `interval-join` run based on `c351db1` plus the
interval-join working change, three alternating separate-JVM release/native-CPU forks processed two
million deterministic events at parallelism four, with a 3GB heap and one-second exactly-once
checkpoints. In-memory Flink and StreamFusion medians were 198,696 and 194,481 events/s respectively,
or 97.9% parity; elapsed-time ranges were 9.46–11.36s and 8.59–14.39s. RocksDB medians were 60,592
and 222,020 events/s, a 3.66x StreamFusion gain, with elapsed ranges of 32.12–68.11s and
8.85–18.74s. The wide ranges, especially for RocksDB, are retained here because the local machine
showed substantial storage and scheduling variance. Every run emitted 1,839,265 changelog rows with
SHA-256 `3dc1434be6a76a8945a77e525f0536a8ba24cfdb8b70a63fae3ce956330cd1ba`, and every
StreamFusion fork reported 996–1,012 native interval-join batches.

Mixed JVM/native CPU profiles were captured separately for both engines and backends using Java
non-safepoint sampling, DWARF/frame-pointer unwinding, JFR, collapsed stacks, and differential flame
graphs; profiler timings were excluded from the results above. Moving timer-group serialization from
every input batch to checkpoint/savepoint boundaries reduced inclusive native timer samples from
roughly 4–5% to 1.0%; timer snapshotting itself is now 0.1% of the in-memory profile and 0.5% of the
RocksDB profile. The final StreamFusion profiles attribute 5.7–5.8% to RowData-to-Arrow conversion,
8.1%/12.8% to the inclusive interval bridge on memory/RocksDB, 6.0–6.1% to the processor within that
bridge, and 2.1% to the native RocksDB wrapper. RocksDB symbols account for about 6.0% of the native
RocksDB run, while Flink's RocksDB interval profile places roughly 45% inclusively under both the
interval operator and RocksDB state path. Remaining visible StreamFusion costs are shared Arrow/JNI
boundaries and result materialization rather than an obvious repeated state-loop allocation. Native
rows, timers, state scratch, exported batches, RocksDB cache, and write buffers remain subject to
Flink managed-memory admission and focused failure/release tests. These are local diagnostic results,
not portable performance claims.

## Bounded full-Sort RowData target

The Kafka-free production harness includes `bounded-sort`, which filters the bid stream and applies
a deterministic global order over price, auction, bidder, timestamp, and payload. The integration
test runs both engines on memory and RocksDB, requires byte-identical raw, ordered, and materialized
digests, verifies an accelerated EXPLAIN, and requires non-zero native bounded-sort batches. Periodic
checkpoints are disabled for this isolated query on both engines because Flink rejects checkpointing
its bounded sorted input; aligned, unaligned, canonical cross-backend, and incremental RocksDB
recovery are covered by dedicated operator tests.

On the September 4, 2026 local release/native-CPU run, three fresh-JVM forks per engine/backend
processed 250,000 deterministic events at parallelism four upstream of the required singleton
exchange. Every fork emitted 230,000 rows with raw SHA-256
`be936c9af5af60dff75147361e571e97284a7c2c5837d47e77758391d60d9a85` and ordered SHA-256
`218058bfcf41f3863c6b7c5522f7d4bb1f4b484feab12332e43acdd68a439852`. Memory medians were
5.642 s for Flink and 6.154 s for StreamFusion, or 91.7% end-to-end throughput parity; MADs were
0.051 s and 0.071 s, with ranges of 5.488–5.693 s and 5.970–6.225 s. RocksDB medians were 5.466 s
and 7.392 s, or 73.9% parity; MADs were 0.074 s and 0.245 s, with ranges of 5.392–6.034 s and
7.148–8.124 s. Flink's full-sort executor is not keyed-state-backed, while the StreamFusion RocksDB
case deliberately persists its counted multiset, so the latter comparison includes direct native
backend durability work. StreamFusion reported 64 native batches in memory and 124 with RocksDB.

Mixed JVM/native CPU profiles with non-safepoint Java samples and DWARF/frame-pointer unwinding
cover both engines and backends. They exposed Flink's runtime inserting `SortingDataInput` before
the keyed native sorter; marking the replacement as internally sorted removed the duplicate full
sort, and final captures contain no such stack. Separate Java and native-allocation JFRs show
source deserialization and result materialization dominating Java allocation, while native sort
allocation is primarily durable opaque row keys and chunked terminal Arrow output. RocksDB adds
its expected write-batch and memtable traffic and still performs one `multi_get` plus one
`WriteBatch` per incoming Arrow batch. JFRs, collapsed stacks, per-engine flame graphs, and
differential flame graphs are retained under
`streamfusion-nexmark-benchmarks/target/bounded-sort-profiles-postfix/`; profiler timings are
excluded from throughput results. These are local diagnostics, not portable performance claims.

## Bounded Limit, SortLimit, and partitioned Rank targets

The bounded planner harness additionally includes `bounded-limit`, `bounded-sort-limit`, and
`bounded-rank`. The last workload is a NEXMark Q18-shaped latest-bid selection expressed as SQL
`RANK`: Flink plans local Sort/Rank, a bidder hash exchange, and global Sort/Rank. StreamFusion's
EXPLAIN must show the retained native hash exchange and one bounded rank selector, with no remaining
Flink Sort/Rank descendants. The integration test compares Flink and StreamFusion materialized
results over all four source/keyed partitions on memory and RocksDB and requires non-zero native
Top-N and bounded-rank batch counters. Generated SQL fixtures separately cover every supported
ordering type, null placement, ties and rank gaps, LIMIT/OFFSET ranges, all four RowKinds, and
canonical memory/RocksDB restore.

On the September 5, 2026 local release/native-CPU run from the bounded-operator working tree, three
alternating fresh-JVM forks per engine/backend processed one million deterministic events with a
2 GiB heap and parallelism four. Every fork materialized 162,946 rows with SHA-256
`80aee0004e60ec7af9a435c398407ea6c7ebab8ca4dd0cd872d1101cb251d43f`. In memory, Flink's median
was 7.815 s (MAD 0.552 s, range 7.263–11.814 s) and StreamFusion's was 6.782 s (MAD 0.035 s, range
6.747–7.892 s), a 15.2% end-to-end throughput gain. RocksDB medians were 6.596 s for Flink (MAD
0.075 s, range 6.520–7.395 s) and 6.703 s for StreamFusion (MAD 0.083 s, range 6.619–6.953 s), or
98.4% parity. StreamFusion executed 272 bounded-rank batches and no longer materialized Flink's
263 MB upstream full-sort buffer.

Longer five-million-event mixed CPU profiles produced the same result on both engines and backends.
Inclusive native-rank CPU was 10.7% in memory and 11.8% on RocksDB; Arrow/native boundary work was
14.8% and 15.2%. At two million events, sampled Java allocations fell from 8.150 GB to 5.416 GB in
memory and from 8.249 GB to 5.460 GB on RocksDB, roughly 34% lower. Native allocation traffic was
higher because durable Arrow/Rust row state and direct RocksDB buffers move out of the JVM; focused
stacks found no per-row JNI or defensive whole-batch copy loop, and backend access remains one
batched read plus one atomic mutation batch per input Arrow batch. CPU and allocation JFRs,
collapsed stacks, and flame graphs are retained under
`streamfusion-nexmark-benchmarks/target/profiles/bounded-rank-select-working/` and
`streamfusion-nexmark-benchmarks/target/benchmarks/bounded-rank-select-working/`. Profiler timings
are excluded from the medians above.

## INTERSECT ALL RowData target

The Kafka-free production harness includes `set-intersect-all`, which intersects two filtered bid
streams on auction, bidder, and price. The integration test runs both engines on memory and RocksDB,
compares the final materialized multiset, requires an accelerated EXPLAIN, and requires non-zero
native grouped-count and Calc/replication batches. Generated SQL parity tests separately compare the
complete changelog for DISTINCT and ALL forms of INTERSECT and EXCEPT, including retractions, nulls,
and every Arrow-representable Flink equality-key type.

On the September 4, 2026 local release/native-CPU run based on `1caf1b2` plus the set-operation
working change, three alternating fresh-JVM forks per engine/backend processed one million
deterministic events at parallelism one, with a 3GB heap and one-second exactly-once checkpoints.
Every fork materialized 195,079 rows with SHA-256
`811b5c5f19b551165db37bbe7365703923d2d126223c7b8d1dc2796cd0e43b97`. In memory, Flink and
StreamFusion median elapsed times were 12.136s and 10.490s, equivalent to 82,401 and 95,333
events/s and a 15.7% StreamFusion throughput gain. Median absolute deviations were 1.576s and
0.297s, with ranges of 9.209–13.712s and 9.564–10.786s. With RocksDB, medians were 9.553s and
9.293s, or 104,678 and 107,611 events/s and a 2.8% gain. Median absolute deviations were 0.069s
and 0.053s, with ranges of 9.484–9.717s and 9.240–9.438s. The wide Flink in-memory dispersion is
reported explicitly; profiler-instrumented timings were excluded.

The first implementation collected and concatenated every replicated output batch, allowing one
large duplicate count to request tens of MiB beyond the operator's assigned managed memory. The
final path exposes the DataFusion result as Arrow C Stream and limits replication chunks to 16,384
rows. Its release callback and input/output reservations have direct leak and double-close tests.
Mixed JVM/native CPU captures use Java non-safepoint samples, DWARF/frame-pointer unwinding, JFR,
collapsed stacks, per-engine flame graphs, and differential flame graphs for both backends. The
native replication kernel accounted for 0.01% of CPU samples on either backend and 0.24% of native
allocation samples; C Stream pulling accounted for 0.36%/0.34% of CPU and 5.66%/5.28% of native
allocation samples on memory/RocksDB. Java allocation sampling recorded 9,920/9,723 StreamFusion
samples versus 18,642/18,632 Flink samples at the same interval; Flink's row replicator alone was
5.05%/5.20%. The remaining visible StreamFusion costs are the shared source conversion, result
materialization, grouped state, and expected RocksDB traffic. The native RocksDB aggregate retains
one `multi_get` and one atomic `WriteBatch` per incoming Arrow batch. Profiles are retained under
`streamfusion-nexmark-benchmarks/target/set-intersect-all-profiles/`. These are local diagnostics,
not portable performance claims.

## Fixed-sequence MATCH_RECOGNIZE RowData target

The Kafka-free production harness includes `match-recognize`, a processing-time `A B C` sequence
partitioned by bidder with current-row definitions, three direct measures, and `AFTER MATCH SKIP
PAST LAST ROW`. The integration test runs Flink and StreamFusion with both hashmap and RocksDB state,
compares the complete changelog and materialized result, requires an accelerated EXPLAIN, and
requires non-zero native MATCH batch counts.

On the September 4, 2026 local run based on `3735b61` plus the MATCH_RECOGNIZE working change, each
engine/backend received an excluded 100,000-event warmup followed by three alternating fresh-JVM
release/native-CPU forks over one million deterministic events at parallelism one, with a 3GB heap
and one-second exactly-once checkpoints. In memory, Flink and StreamFusion medians were 92,574 and
115,942 events/s respectively, a 25.2% StreamFusion gain. Median absolute deviations were 0.279s
and 0.203s, with elapsed ranges of 10.523–11.231s and 8.357–8.827s. With RocksDB, the medians were
5,745 and 114,689 events/s, a 19.96x StreamFusion speedup. Median absolute deviations were 0.472s
and 0.003s, with elapsed ranges of 172.940–174.533s and 8.505–8.722s. Every measured fork emitted
299,959 rows with SHA-256
`3234a71e259bd405c027157d5fd036939f7df3fef6fae43880617a393a66aa86`; StreamFusion reported
62–64 native MATCH batches per fork. The machine was a 12th Gen Intel Core i7-12650H under x86-64
WSL2 with OpenJDK 24.0.2 and Rust 1.94.0. Profiler-instrumented timings were excluded.

Mixed JVM/native CPU captures retained JFRs, collapsed stacks, per-engine flame graphs, and
differential flame graphs for both backends, using Java non-safepoint samples and DWARF/frame-pointer
native unwinding. The full native MATCH bridge accounted for 1.50% of in-memory samples and 1.68%
of RocksDB samples; the Rust processor was 0.87%/0.80%, Arrow row encoding 0.36%/0.34%, and direct
native RocksDB 0.09% in the Rocks run. Flink's CEP operator accounted for 8.47% in memory and 71.89%
with RocksDB. Separate Java and native allocation JFRs and collapsed stacks found no dominant
StreamFusion allocation loop. Java allocation samples under the MATCH bridge were 11.82%/10.96%,
mostly required opaque key/output byte arrays, while Flink CEP was 43.51%/51.69%. Native MATCH
accounted for 6.07%/7.75% of native allocation samples, Arrow row conversion for 1.63% on both
backends, and direct RocksDB for 1.79%. Focused admission/release tests verify that partial-match
state, scratch, output, and RocksDB memory remain charged to Flink managed memory. The native
RocksDB path performs one `multi_get` and one atomic `WriteBatch` per incoming Arrow batch. No
profile-backed architectural shortcut was warranted; these are local diagnostics, not portable
performance guarantees.

## Q18 and synchronous deduplicate RowData targets

The Kafka-free production harness now includes `q18`,
`deduplicate-processing-time-keep-first`, and `deduplicate-processing-time-keep-last`. The generated
inputs and result sink use the same Flink `RowData` conversions as a non-native source and sink, while
every internal StreamFusion edge remains Arrow-backed. The integration test runs all three SQL
shapes against Flink and StreamFusion on heap and RocksDB, compares the complete raw changelog and
materialized result, requires an accelerated EXPLAIN, and requires non-zero native deduplicate batch
counts. Q18 also provides the release benchmark and mixed-profile workload below.

On the September 4, 2026 local release/native-CPU production-harness run based on `0006704` plus
the synchronous-deduplicate working change, each engine/backend received one excluded 100,000-event
warmup followed by alternating fresh-JVM forks over one million events at parallelism one, with a
3GB heap and one-second exactly-once checkpoints. The in-memory medians were 66,870 events/s for
Flink and 69,286 events/s for StreamFusion, a 3.6% StreamFusion gain. Median absolute deviations
were 2.287s and 0.354s, and elapsed ranges were 12.667–17.951s and 14.079–18.335s respectively.
RocksDB was extended to six forks after a storage outlier: the medians were 60,342 events/s for
Flink and 63,172 events/s for StreamFusion, a 4.7% gain, with median absolute deviations of 0.059s
and 0.280s and full elapsed ranges of 16.497–20.383s and 15.336–26.174s. Every StreamFusion fork
reported 62–64 native deduplicate batches. Each fork emitted 1,547,283 raw changelog rows and
materialized 292,717 final rows; the integration suite, rather than independently generated
benchmark forks, is the byte-for-byte parity authority.

Longer mixed JVM/native profiles used Java non-safepoint sampling, DWARF/frame-pointer unwinding,
JFR, collapsed stacks, and differential flame graphs for both engines and backends. In the final
StreamFusion profiles, RowData-to-Arrow conversion accounted for 1.18%/1.15% of CPU samples on
memory/RocksDB, the native deduplicate processor for 2.04%/1.78%, Arrow C transport for
0.49%/0.56%, and heap checkpoint key-group snapshotting for 0.43%; incremental RocksDB
checkpointing had no corresponding full-state scan. Reusing the already encoded selected rows
halved `RowConverter.convert_columns` from about 0.4% to 0.20–0.21%. The RocksDB bridge remains
inclusive of downstream work and storage at 7.04%, while the implementation performs one native
`multi_get` and one `WriteBatch` per incoming Arrow batch. Native allocation profiles confirmed
that required row encoding, checkpoint framing, and RocksDB write batches dominate the visible
state-path allocations; all are charged to the operator's Flink managed-memory reservations, with
focused admission-failure and release tests. Profiler timings were excluded from the throughput
measurements. These are local diagnostic results, not portable performance guarantees.

The earlier test-only RowData Q18 microbenchmark has been retired. It exercised a separate adapter
instead of the production Arrow operator topology and could therefore give misleading results.
Use the release production harness above for Q18 throughput and profiling comparisons.

## Bounded q20 join target

Bounded q20 exercises the production `RowData -> Arrow -> native hash exchange -> native bounded
join -> fused Calc -> Arrow-backed RowData` path. Its integration case runs on memory and RocksDB,
compares the exact final multiset against Flink, requires an accelerated EXPLAIN, and requires both
native regular-join and fused-Calc batch counters to be nonzero. The bounded join consumes IPC
frames directly; complex keys retain their opaque Flink BinaryRow sidecar through that native edge,
while primitive keys remain Rust encoded.

On the September 5, 2026 local release/native-CPU run based on `89043f4` plus the bounded-join
working change, each engine/backend received an excluded 100,000-event warmup followed by three
alternating fresh-JVM forks over 500,000 deterministic events at parallelism one. The JVM used a
2 GB fixed heap and 512 MiB of ordinary Flink managed memory. In memory, Flink and StreamFusion
median elapsed times were 7.309 s and 7.788 s, equivalent to 68,413 and 64,200 events/s or 93.8%
end-to-end throughput parity. Median absolute deviations were 0.071 s and 0.015 s, with ranges of
7.049–7.380 s and 7.665–7.803 s. RocksDB medians were 7.191 s and 7.737 s, equivalent to 69,535
and 64,625 events/s or 92.9% parity. Median absolute deviations were 0.055 s and 0.054 s, with
ranges of 7.135–7.252 s and 7.683–8.123 s. Every fork emitted 90,255 rows with SHA-256
`224695c393166d7de182f05ac38ac81adb9dcb06b39016eb2fda0322a1268def`; every StreamFusion fork
reported full acceleration and 69 native join batches plus 68 fused Calc stages. The machine was a
12th Gen Intel Core i7-12650H under x86-64 WSL2 with OpenJDK 24.0.2 and Rust 1.94.0.

The first fused implementation still decoded each exchange frame into Arrow Java and immediately
re-imported it into Rust, reaching about 88% memory and 87% RocksDB parity. Direct native frame
ingestion removed that duplicate boundary and the associated empty-output transfer. Longer mixed
JVM/native CPU profiles use Java non-safepoint sampling, DWARF/frame-pointer unwinding, JFR,
collapsed stacks, per-engine flame graphs, and differential flame graphs. The complete join path
accounted for 8.3%/8.4% of process CPU samples on memory/RocksDB, RowData-to-Arrow for 6.7%/5.3%,
native exchange for 4.9%/5.4%, and the Arrow-backed output/sink path for 4.0%/3.8%. Exclusive Java
allocation attribution placed only 0.2%/0.1% in the join and 1.6% in exchange; source conversion and
the result sink dominated. Native allocation was concentrated in durable encoded rows and decoding
keyed state for terminal output; IPC decode was about 0.2%, with no per-row JNI allocation loop.
All retained state, decode scratch, IPC ownership, and output batches remain charged to Flink
managed memory. Profiler timings were excluded from the benchmark result. Artifacts are retained
under `streamfusion-nexmark-benchmarks/target/profiles/bounded-q20-join/`; these are local
diagnostics, not portable performance guarantees.

## Bounded UNNEST and Window TVF targets

The Kafka-free harness includes `batch-unnest` and `batch-window-tvf`. Both use Flink's bounded
physical planner and the production RowData-to-Arrow and Arrow-to-RowData edges. The UNNEST case
expands bid-derived arrays; the TVF case filters/projects bids and assigns ten-second event-time
TUMBLE windows. They are stateless, so a RocksDB-labelled run would not exercise RocksDB and is not
reported as a backend comparison.

On the September 5, 2026 local release/native-CPU run based on `f2b3779` plus the bounded-expansion
working change, three alternating fresh-JVM forks per engine processed 500,000 deterministic events
at parallelism one. `batch-unnest` emitted 1,380,000 rows with SHA-256
`3db35c7af1d13c26f4d31c0123899905fc341f00139b20116eba1809a514f8c1`. Flink and StreamFusion
median elapsed times were 8.543 s and 8.312 s, a 2.8% StreamFusion throughput gain; MADs were
0.476 s and 0.011 s, with ranges of 7.931–9.019 s and 8.300–8.562 s.

The first 500,000-event TVF measurement exposed two native boundaries for `Calc -> TUMBLE` and a
median 8.5% StreamFusion deficit. Nesting the lower Calc below the native TVF cut invocations from
two to one per source batch. The post-fusion 500,000-event medians were 6.789 s for Flink and
7.092 s for StreamFusion, or 95.7% throughput parity; MADs were 0.121 s and 0.004 s. A longer
one-million-event repeat produced medians of 9.771 s and 9.821 s, or 99.5% parity, with MADs of
0.132 s and 0.312 s. Every one-million-event fork emitted 920,000 rows with SHA-256
`68e3b5214edc34c1ca5d3d4491f5e6dc5c3905abc193d813d594e7769e07ad46`, reported full
acceleration, and executed 62 native batches. The full ranges—9.640–11.534 s for Flink and
9.509–14.719 s for StreamFusion—retain one local scheduling outlier rather than silently dropping
it.

Separate mixed JVM/native CPU profiles used Java non-safepoint sampling, DWARF/frame-pointer native
unwinding, JFR, collapsed stacks, per-engine flame graphs, and a differential flame graph. In the
one-million-event capture, the complete fused StreamFusion operator was 5.91% of CPU samples and
the Rust Window TVF frame itself was 0.07%; Flink's aligned Window TVF was 5.87%. Java-allocation
captures at the same sampling interval recorded 3.28 GB for StreamFusion versus 3.75 GB for Flink,
12.5% less sampled allocation volume. A native-allocation capture attributed 14.1 MB, 0.77% of
process-native sampled bytes, beneath the complete StreamFusion operator; it found no repeated
per-row native allocation loop. Required Arrow output buffers remain admitted through Flink managed
memory. Profiler timings were excluded from the throughput results. Artifacts are retained under
`streamfusion-nexmark-benchmarks/target/bounded-expansion-profiles/` and
`streamfusion-nexmark-benchmarks/target/bounded-expansion-postfusion/profiles/`; these are local
diagnostics, not portable performance guarantees.

---
title: Q7 row-entry state release comparison
description: Reduced join-state write amplification, verified parity, and continuing timing and capacity limits.
---

Release `d4a3db14654790baae36cade567fca0c0059f8d6` preserves original Q7 acceleration with Flink's
default `table.optimizer.multi-join.enabled=false` on both backends. Regular joins now update
individual retained rows and load only the payloads required by each input batch. Captured
RocksDB flush-input bytes fall by approximately 74% against the previous layout. This fixes a
general storage inefficiency, but does **not** establish a throughput win: StreamFusion loses
all three measured pairs on each backend, with substantial native timing variation.

The [previous default-path report](/StreamFusion/benchmarks/q7-default-rowdata/) records
`f4886002` and the write-amplification diagnosis. Its timings remain historical; do not combine
forks across releases or substitute the faster diagnostic/profile runs for measured results.

## Measurements, September 10, 2026

Each backend has three alternating fresh-JVM pairs at one million events: Flink/StreamFusion,
StreamFusion/Flink, Flink/StreamFusion. All twelve jobs complete and emit one blackhole record.
MAD is median absolute deviation. Timing is end-to-end, including setup, EXPLAIN, native
initialization, cluster startup, execution and cleanup. JVM launch, argument parsing and builds
are excluded; runtime JIT compilation is included. These are not steady-state rates.

| Backend | Flink seconds: median [range]; MAD | StreamFusion seconds: median [range]; MAD | StreamFusion/Flink throughput |
| --- | --- | --- | ---: |
| In-memory | 6.741 [6.522–12.435]; 0.219 | 25.669 [7.070–34.339]; 8.670 | 0.263× |
| RocksDB | 8.140 [8.126–8.533]; 0.014 | 20.314 [8.289–26.344]; 6.030 | 0.401× |

Every fork is retained. The native minima are close to Flink, but the medians are much slower;
those minima are not evidence of a repeatable gain. The older report's faster in-memory ratio
also had wide variation. Cross-release ratios alone cannot attribute the timing difference to
this storage change.

Every StreamFusion EXPLAIN reports ordinary acceleration and positive native activity. Native
plan / Calc batch counts are 527/144, 520/142 and 499/136 in memory; 977/266, 999/268 and 935/258
on RocksDB. Flink has zero native activity. These diagnostics do not replace logical-record I/O
metrics. No profiler or build overlaps the measured forks.

## General implementation and validation

The former compact equality-key values and fixed 64-row pages rewrote old payloads when a batch
appended another row. Multi-row keys now use a versioned `SFJI/1` presence bitmap with separate
stable-row-ID payload entries. A small singleton can remain compact. Batched writes contain
only changed entries and the changed directory. Retractions and association updates retain
stable identities and order; Flink partition hashing is unchanged.

For INSERT/UPDATE_AFTER-only input batches, an existing row-entry directory retains the IDs of
historical rows on the accumulating side without loading their payloads. Opposite-side candidates
are loaded before computation. Retraction/mixed batches load the required history on both sides.
Transport keys and read buffers are bounded in bulk groups of 4,096 entries, while decoded state
retains its coarse reservation. Input staging still uses zero-copy Arrow slices of at most
1,024 rows. There are no per-row RocksDB calls or extra JNI handoffs. DataFusion residual
computation, Flink changelog transitions, memory shares and checkpoint boundaries are unchanged.

Legacy `SFJM/1` pages and multi-row `SFJC/1` compact values remain readable and migrate atomically
on the next touched write. Both backends use the same canonical representation. An older binary
cannot read the new layout; see the full [join state contract](/StreamFusion/operators/joins/).

Validation includes 64 focused native regular-join tests, 24 focused Java tests, and 16 official
Q7/Q8 production integration cases. Coverage includes generated changelog parity, topology,
complete metric surfaces, aligned/unaligned recovery, restore/rescaling, legacy encoding
migration on both backends, malformed directories, stable key framing, bounded read workspace,
and selective payload loading. A repeated-wide-append fixture verifies cumulative encoded
writes stay below twice the new payload size; it is storage evidence, not a throughput benchmark.
The integration cases cover both backends, parallelism one/four and both optimizer settings,
collecting bytes and unmodified-blackhole counts, ordinary admission, and no standalone
local-window JNI execution.

Separate fresh-JVM collecting runs at one million events pass on both engines and both backends.
Output and materialized hashes agree at
`e440ad7d261dd37d6c50696421a22f26b0073cfddf1ec5fbfe46ef553e3e6c92`, with one output record.
Blackhole timing itself is not correctness evidence.

## Longer profiles and remaining waits

Separate 1.25-million-event forks complete for each engine/backend. Async-profiler 4.5 records
CPU and wall samples at 10 ms, Java non-safepoint sampling, native DWARF/frame-pointer unwinding,
JFR and allocation samples at 2 MiB. Per-engine flame graphs, CPU/wall collapsed stacks and
CPU differential flame graphs are retained. Profile-only RocksDB symbol preloading is recorded
in metadata. Native plan / Calc counts are 647/176 in memory and 1,214/332 on RocksDB; all four
jobs emit one record. Instrumented elapsed times are excluded from the measurement table.

The following percentages are inclusive shares of **captured CPU samples**, Flink / StreamFusion.
Denominators are 3,109 / 2,742 in memory and 3,488 / 3,230 on RocksDB. Categories overlap; JNI
includes downstream native work, and DataFusion stacks can include StreamFusion adaptations.
Zero means no matching sample. These recordings establish mixed JVM/native task-path coverage,
but do not establish exhaustive coverage of standalone native RocksDB background threads:
no background-flush/thread-pool frames matched. Do not interpret the RocksDB category as its
complete process CPU cost or use it alone to explain flush stalls.

| CPU category | In-memory F / SF | RocksDB F / SF |
| --- | ---: | ---: |
| Source polling | 22.033% / 15.937% | 17.030% / 13.251% |
| Row copying | 10.100% / 3.647% | 6.995% / 2.910% |
| RowData-to-Arrow writing | 0.000% / 1.240% | 0.000% / 1.207% |
| Arrow C Data / JNI | 0.000% / 15.317% | 0.000% / 28.328% |
| Native plan lowering | 0.000% / 0.000% | 0.000% / 0.000% |
| DataFusion execution | 0.000% / 13.640% | 0.000% / 9.319% |
| Arrow-backed output access | 0.000% / 0.000% | 0.000% / 0.031% |
| Native regular join | 0.000% / 14.880% | 0.000% / 8.762% |
| RocksDB | 0.000% / 0.000% | 26.061% / 20.000% |
| Memory-budget callbacks | 0.000% / 0.182% | 0.000% / 0.124% |
| JIT compilation | 32.937% / 35.850% | 29.444% / 33.313% |
| Garbage collection | 14.924% / 3.246% | 3.096% / 2.724% |

The new RocksDB profile has 5,526 Flink wall samples under synchronous snapshotting, including
5,462 waiting for flushes; StreamFusion has 670 and 630 respectively. The previous release's
profile had the larger waits on StreamFusion instead. Checkpoint flush waiting therefore occurs
on either engine in these runs. Aggregate thread samples are not job elapsed time, and this
profile is not a causal explanation for every slow unprofiled fork. In memory, native raw
snapshot writing accounts for 137 wall samples in this profile; it does not establish the cause
of the much slower measured forks. Memory-budget callbacks remain below 0.2% of captured
native-job CPU samples.

## Actual RocksDB flush volume

A separate 1.25-million-event pair uses the same release/settings while an external observer
copies open LOG/OPTIONS files and samples SST sizes every 200 ms. Both jobs complete with one
output record; StreamFusion reports acceleration and 1,196 native-plan batches. These are
diagnostic runs, excluded from throughput results. Cleanup can race the observer and omit a
final log suffix, so totals describe captured flush events rather than exact lifetime writes.

| Captured flush events | Flink | StreamFusion |
| --- | ---: | ---: |
| Flush-input bytes | 171,843,770 | 303,776,479 |
| Flush-input entries | 1,068,326 | 1,647,247 |
| Started / completed flushes | 24 / 23 | 20 / 20 |

Both engines again have five observed databases. Native flush-input bytes are about 74% below
the prior unprofiled diagnostic's 1,165,188,941 bytes; Flink's 171,843,770 bytes are close to its
previous 172,060,230. Native entries increase because payload rows and directories are separate,
while repeatedly rewritten payload bytes fall substantially. Native bytes remain about 1.77×
Flink's. Captured native flushes in this pair are checkpoint-triggered (`Get Live Files`), with
no write-buffer-manager-triggered flush recorded. This supports the intended storage mechanism;
it does not prove the same percentage reduction in physical disk writes or elapsed time.

Neither engine skips the synchronous upstream RocksDB checkpoint flush. No memory-budget,
checkpoint, source, sink or upstream algorithm changes were used to obtain these results.

## Reproduction and limits

Both engines use original Q7 SQL, the deterministic official Nexmark RowData source and the
unmodified Flink blackhole sink, parallelism four, UTC, disabled mini-batching, one-second
exactly-once checkpoints, 1 GiB managed memory and weights
`OPERATOR:70,STATE_BACKEND:70,PYTHON:30`. JVM flags include `-Xms1g -Xmx1g`,
`-XX:MaxDirectMemorySize=2g`, `-XX:ActiveProcessorCount=4` and the required `java.nio` opening.
There is no Kafka interaction.

The clean measured checkout is `d4a3db14654790baae36cade567fca0c0059f8d6`. The machine is an Intel
Core i7-12650H, WSL2 Linux 6.18.33.2, sixteen reported logical CPUs, approximately 7.6 GiB RAM and
2 GiB swap; OpenJDK is 24.0.2+12-54. Rust 1.94 uses release optimization, native CPU features,
frame pointers and separate DWARF symbols without reducing optimization. CPU baseline is
`44dd0ad765af32a3`. Artifact SHA-256 values are:

- Core: `64c38cba15ab17529ddb03b65a8d515ddc38e7a06d9813f9fb4465cf475cfcf6`.
- RocksDB: `fe1af76cd4e48dc789eca1eb720d1fdea5b67d40f465956401c39a6653f08862`.

Metadata records the full commands and upstream revisions: Flink
`c0f8d1a1e09f209885a88f9c19ceb9d9e9870283`, Nexmark
`6b3646c3baec701f1fa74baf938d235f742e5d3c`. The permitted planner-installation and post-StreamGraph
hooks are recorded; source, sink and Flink operator algorithms are unchanged.

At two million events, a native-only RocksDB capacity probe completes with one blackhole record,
ordinary acceleration and 1,884 native-plan batches. The native in-memory probe still fails
mutation admission: 1,071,840 additional bytes are requested with 2,016,490 held by the in-flight
consumer and 1,064,224 available to it. The previous Flink two-million-event in-memory attempt
failed under heap/GC pressure; it was not repeated here. There is no two-million-event paired
throughput result, and the successful RocksDB blackhole probe is not separate collecting parity.
Finite retained state and hot-key history still have to fit Flink's allowances.

This is a verified storage improvement and performance checkpoint, not a claim that Q7 is
optimal. Timing variation remains unresolved; further work needs profiles that reproduce the
slow native forks and verified native background-thread coverage. Q8's default-path release
comparison remains pending. The full Nexmark goal is incomplete.

Artifacts remain under
`streamfusion-nexmark-benchmarks/target/measurements/q7-default/d4a3db14/`: `*-1m` measurements,
`*-1250k-profile` recordings and summaries, `collecting-validation.json`, and
`rocksdb-1250k-flush-diagnostic` logs/options and `flush-summary.json`. Native capacity probes are
in `*-2m-native-capacity`. Focused validation logs remain under
`target/measurements/q6-q8-default/row-entries-*`.

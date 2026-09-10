---
title: Q5 default WindowJoin release comparison
description: Default-optimizer Q5 measurements, profiles, and remaining in-memory capacity limits.
---

At release code `a47c734026ec5590f9c8dbc9e66d598f8789b017`, Q5 accelerates with Flink's default
`table.optimizer.multi-join.enabled=false`. Three alternating fresh-JVM pairs complete at one
million events on both backends. StreamFusion has 6.5% lower median throughput in memory and
5.8% higher median throughput on RocksDB. Larger in-memory attempts still exhaust retained-state
allowance. This is an intermediate performance checkpoint, not evidence that Q5 is fully
optimized or that the longer-profile requirement has been met on both backends.

The [older Q5 report](/StreamFusion/benchmarks/q5-rowdata/) uses the enabled multi-join optimizer
and different memory-consumer weights. Its measurements must not be combined with these results.

## Measurements, September 10, 2026

Each engine runs in three separate unprofiled JVMs. Order alternates Flink/StreamFusion,
StreamFusion/Flink, Flink/StreamFusion. MAD is median absolute deviation. Timing is end-to-end:
setup, EXPLAIN, native initialization, cluster startup, execution and cleanup; JVM launch,
argument parsing and compilation are excluded. These are local measurements, not steady-state
throughput guarantees.

| Backend | Flink seconds: median [range]; MAD | StreamFusion seconds: median [range]; MAD | StreamFusion/Flink throughput |
| --- | --- | --- | ---: |
| In-memory | 6.411 [6.392–6.420]; 0.008 | 6.859 [6.758–6.940]; 0.080 | 0.935× |
| RocksDB | 9.066 [8.786–9.947]; 0.280 | 8.570 [8.276–9.566]; 0.294 | 1.058× |

StreamFusion loses all three in-memory pairs, whose engine ranges do not overlap. It wins all
three RocksDB pairs, but their overall ranges overlap; the modest median gain is not a broad
performance guarantee. Every run emits five blackhole records. Every StreamFusion EXPLAIN
reports acceleration. Shared native-plan batch counts are 623/650/651 in memory and 763/771/743
on RocksDB; native Calc counts are 68/71/73 and 128/132/126 respectively. Flink reports zero native
activity. These invocation counters are not logical-record I/O metrics.

Both engines use the same deterministic Nexmark RowData source and original SQL, the unmodified
Flink blackhole sink, parallelism four, UTC, disabled mini-batching, one-second exactly-once
checkpoints, 1 GiB managed memory and `OPERATOR:70,STATE_BACKEND:70,PYTHON:30` consumer weights.
JVM settings are `-Xms1g -Xmx1g -XX:MaxDirectMemorySize=2g -XX:ActiveProcessorCount=4`, with the
required `java.nio` opening. No Kafka services or connector benchmarks are involved.

The machine is an Intel Core i7-12650H under WSL2, Linux 6.18.33.2, with OpenJDK 24.0.2. The clean
measurement checkout builds Rust with release optimization, native CPU features, frame pointers
and separate profiling symbols. CPU baseline is `44dd0ad765af32a3`. Core native SHA-256 is
`0096134f91d274495cd4e2b3985e4ad279b2e87903972fe8d44fee5c57f77d1a`; RocksDB native SHA-256 is
`fe1af76cd4e48dc789eca1eb720d1fdea5b67d40f465956401c39a6653f08862`.
Metadata retains complete JVM flags, upstream revisions and patches, classpath and artifact
properties. Flink's local patches are the permitted planner installation/class-loading and
post-StreamGraph resource-finalization hooks; operator algorithms, source and sink remain intact.

## General improvement and validation

The earlier `a4dcc33d` implementation exhausted whole-window decoding workspace at one million
events on both backends. The new close adapter retains one decoded right window, loads bounded
ordered left pages, and executes the actual DataFusion join for each page. It shares Arrow buffers
and reclaims acknowledged left payloads only after that page reaches DataFusion EOF. It preserves
Flink's left-major output order without an extra JVM/native crossing between pages. No memory
budget or tuning option changes. The complete right window still must fit its allowance.

Forty-five focused Rust window-join tests pass with both backends available, including a 20,003-row
left window with only 4 MiB remaining, exact pair order, output ownership and cancellation followed
by cross-backend recovery. Twenty-eight focused Java tests cover runtime parity, complete metric
surface, rescaling, aligned/unaligned channel recovery and selected SQL. Eight opt-in official Q5
cases compare collecting-sink bytes and blackhole counts at 20,000 events, both backends,
parallelism one/four and both multi-join optimizer settings. Profiling does not replace these tests.

## CPU profiles and remaining limits

Separate async-profiler 4.5 runs use 10 ms CPU sampling, Java non-safepoint sampling, native DWARF
unwinding and JFR output. RocksDB completes at two million events. In-memory attempts at two
million and 1.25 million events fail, so the completed in-memory diagnostic uses one million.
**That diagnostic does not satisfy the longer-workload profile requirement.** Profiled timings
are excluded from the measurement table. Completed pairs each emit five rows; native-plan/Calc
counts are 644/70 in memory and 1,429/254 on RocksDB.

The following inclusive shares use all process CPU samples as denominator. Categories overlap:
JNI includes downstream computation, and DataFusion includes stream execution. Sampling and
native unwinding can miss frames; zero means no matching sample, not zero execution cost.

| CPU category | Memory Flink / StreamFusion | RocksDB Flink / StreamFusion |
| --- | ---: | ---: |
| Source polling, including downstream calls | 16.65% / 15.16% | 18.18% / 16.90% |
| RowData copying | 8.16% / 3.50% | 8.26% / 3.90% |
| RowData-to-Arrow writing | 0% / 1.17% | 0% / 1.28% |
| Arrow C Data / JNI, inclusive | 0% / 9.05% | 0% / 31.63% |
| Native plan lowering | 0% / 0.05% | 0% / 0.03% |
| DataFusion execution | 0% / 7.42% | 0% / 12.63% |
| Arrow-backed output access | 0% / 0.05% | 0% / 0% |
| JVM JIT compilation | 41.57% / 43.84% | 29.36% / 30.99% |

The completed RocksDB profile attributes 20.60% of StreamFusion CPU samples to RocksDB frames,
7.27% to native global-window execution, 4.19% to the window-join adapter and 2.47% to its closing
path, including decoding and deletion. Corresponding in-memory global-window and window-join
shares are 5.04% and 1.49%. Some native stacks omit their Rust callers, so these are observed
inclusive shares rather than exhaustive operator attribution. Large JIT shares also limit what
these short end-to-end runs establish about steady-state compute performance.

Both failed in-memory attempts request another 225,475 bytes for native state node `4294967341`.
At two million events, 61,803,681 bytes are reserved and 27,091 remain; at 1.25 million,
61,814,302 are reserved and 16,244 remain. Flink completes both matching runs. This is a retained
native-state capacity limit, distinct from the earlier whole-window decoder reservation failure.
A diagnostic maps this owner to `WindowJoin[45]`. Its per-row payload entries repeat the
partition/window prefix and ordered-tree overhead. The subsequent storage change appends bounded
payload pages while retaining the separate ordered index and original memory allowance. A native
regression stores 10,000 rows as 40 entries with less than 1 MiB retained in memory. Old indexed
windows remain readable until they close; the [window-join contract](/StreamFusion/operators/window-join/)
describes encoding versions and recovery. These results do not change the `a47c7340` measurements
above: a release rerun, completed longer in-memory profile and further general optimization remain
outstanding.

Raw metadata, all runs, completed per-engine JFRs, CPU/allocation collapsed stacks, flame graphs,
differential flame graphs and category definitions are under
`streamfusion-nexmark-benchmarks/target/measurements/q5-default/a47c7340/`, in `hashmap-1m`,
`rocksdb-1m`, `hashmap-1m-profile`, and `rocksdb-2m-profile`. Failed diagnostic JFRs and logs remain
in `hashmap-2m-profile` and `hashmap-1250k-profile`; they are not completed comparison profiles.

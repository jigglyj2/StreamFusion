---
title: Q8 default-optimizer release baseline
description: Default WindowJoin measurements, collecting parity, profiles and the payload-deletion bottleneck.
---

At release `d4a3db14654790baae36cade567fca0c0059f8d6`, original Q8 accelerates on both backends
with Flink's default `table.optimizer.multi-join.enabled=false`. Its DISTINCT TUMBLE branches
feed the native WindowJoin. StreamFusion loses all three measured pairs at both one and ten
million events on each backend. This establishes a baseline and an optimization target, not
completion of Q8 performance work. The [older Q8 report](/StreamFusion/benchmarks/q8-rowdata/)
uses the enabled multi-join optimizer and different consumer weights; its results are separate.

The [subsequent state-access report](/StreamFusion/benchmarks/q8-state-access-rowdata/) records
the completed fixes and release comparison at `f66d0afb`. This page retains the original baseline.

## Measurements, September 10, 2026

Each size/backend has three fresh unprofiled JVMs per engine, alternating Flink/StreamFusion,
StreamFusion/Flink, Flink/StreamFusion. MAD is median absolute deviation. All 24 measured jobs
complete. End-to-end timing includes setup, EXPLAIN, native initialization, cluster startup,
execution and cleanup; it excludes JVM launch, argument parsing and builds. Runtime JIT is
included. No profiler or build overlaps a measured fork, and no instrumented timing appears here.

| Events | Backend | Flink seconds: median [range]; MAD | StreamFusion seconds: median [range]; MAD | StreamFusion/Flink throughput |
| ---: | --- | --- | --- | ---: |
| 1,000,000 | In-memory | 5.760 [5.725–5.765]; 0.006 | 7.224 [7.129–7.242]; 0.018 | 0.797× |
| 1,000,000 | RocksDB | 5.975 [5.885–6.901]; 0.090 | 8.646 [8.412–9.223]; 0.233 | 0.691× |
| 10,000,000 | In-memory | 12.718 [12.597–12.736]; 0.018 | 21.492 [19.805–21.690]; 0.197 | 0.592× |
| 10,000,000 | RocksDB | 16.032 [13.280–16.148]; 0.116 | 31.844 [31.700–35.044]; 0.144 | 0.503× |

All one-million-event jobs emit 10,562 blackhole records; all ten-million-event jobs emit 106,331.
Every StreamFusion EXPLAIN reports acceleration with positive native activity; Flink has zero
native activity. Native-plan / Calc counts are:

| Events | Backend | Native-plan / Calc counts, each measured fork |
| ---: | --- | --- |
| 1,000,000 | In-memory | 826/261, 832/266, 834/265 |
| 1,000,000 | RocksDB | 1308/504, 1324/504, 1394/520 |
| 10,000,000 | In-memory | 6799/2469, 6833/2474, 6868/2483 |
| 10,000,000 | RocksDB | 11734/4912, 11756/4918, 11608/4900 |

These diagnostics are distinct from Flink logical-record counters. The slowdown persists at the
larger size, so startup alone cannot explain it. These local end-to-end results are not
steady-state throughput guarantees.

## Configuration and correctness

The clean measured checkout and both native artifacts are the same `d4a3db14` release recorded
in the [Q7 row-entry report](/StreamFusion/benchmarks/q7-row-entry-rowdata/). Both engines use
original Q8 SQL, the deterministic official Nexmark RowData source, the unmodified Flink blackhole
sink, parallelism four, UTC, disabled mini-batching, one-second exactly-once checkpoints,
1 GiB managed memory and weights `OPERATOR:70,STATE_BACKEND:70,PYTHON:30`. JVM settings include
`-Xms1g -Xmx1g`, `-XX:MaxDirectMemorySize=2g`, `-XX:ActiveProcessorCount=4` and the required
`java.nio` opening. No Kafka services or connectors are involved.

The host is an Intel Core i7-12650H, WSL2 Linux 6.18.33.2, sixteen reported logical CPUs,
approximately 7.6 GiB RAM and 2 GiB swap; OpenJDK is 24.0.2+12-54. Rust 1.94 uses release
optimization, native CPU features, frame pointers and separate DWARF symbols. CPU baseline is
`44dd0ad765af32a3`. Core SHA-256 is
`64c38cba15ab17529ddb03b65a8d515ddc38e7a06d9813f9fb4465cf475cfcf6`; RocksDB SHA-256 is
`fe1af76cd4e48dc789eca1eb720d1fdea5b67d40f465956401c39a6653f08862`. Metadata records full commands,
classpath, machine/runtime information, permitted Flink patches and upstream revisions: Flink
`c0f8d1a1e09f209885a88f9c19ceb9d9e9870283`, Nexmark
`6b3646c3baec701f1fa74baf938d235f742e5d3c`. Source, sink and Flink operator algorithms are unchanged.

Separate collecting runs at 12.5 million events pass on both engines and both backends, emitting
133,078 records. Complete collected-output and materialized hashes agree at
`a275055412e0a03336506a7f9edf44091fa0bec4149024b9ebc284474dd51e59`.
Independent network-input interleaving does not define a total order for tied windows; the query
comparison preserves every record's bytes and multiplicity, while generated fixed-arrival operator
tests check ordered changelogs. Blackhole counts alone are not parity evidence.

The release also passes all eight `NexmarkQ8ProductionIT` cases, covering both backends,
parallelism one/four and both optimizer settings with collecting bytes and blackhole counts.
They require ordinary admission, positive native activity and zero standalone local-window JNI
execution. The [window-join contract](/StreamFusion/operators/joins/#attached-inner-window-joins)
records the generated SQL, memory/ownership, metric, checkpoint and rescaling coverage and
unsupported semantic subsets.

## Successful longer profiles

Each engine/backend has a successful 12.5-million-event recording using async-profiler 4.5,
CPU/wall sampling at 10 ms, Java non-safepoint sampling, native DWARF/frame-pointer unwinding,
JFR and Java allocation sampling at 2 MiB. Per-engine flame graphs, CPU/wall/allocated-byte
collapsed stacks and CPU differential flame graphs are retained. All four jobs emit 133,078
records. Native-plan / Calc counts are 8,568/3,092 in memory and 14,648/6,144 on RocksDB.

A diagnostic-only Java launcher calls the profiler's upstream `asprof_init` API through Java FFM
after JVM startup and before benchmark execution. This installs hooks for subsequently created
native threads while retaining normal JVM-agent recording. Both RocksDB profiles contain
background-flush/thread-pool frames (161 Flink samples, two native-job samples); small counts are
not proof of negligible background work in other forks. Native RocksDB symbol preloading remains
profile-only. Neither hook initialization nor preloading occurs in measured runs.

Percentages below are inclusive shares of captured CPU samples, Flink / StreamFusion. Denominators
are 6,254 / 9,712 in memory and 6,813 / 16,742 on RocksDB. Categories overlap: JNI includes downstream
native execution, and DataFusion frames can include StreamFusion adaptations. Zero means no
matching sample, and unwinding can omit callers. These profile shares are not elapsed-time ratios.

| CPU category | In-memory F / SF | RocksDB F / SF |
| --- | ---: | ---: |
| Source polling | 53.470% / 32.815% | 49.875% / 20.983% |
| Row copying | 22.322% / 8.773% | 21.180% / 5.418% |
| RowData-to-Arrow writing | 0.000% / 2.492% | 0.000% / 1.607% |
| Arrow C Data / JNI | 0.000% / 44.110% | 0.000% / 64.443% |
| Native plan lowering | 0.000% / 0.000% | 0.000% / 0.000% |
| DataFusion execution | 0.000% / 34.916% | 0.000% / 51.493% |
| Arrow-backed output access | 0.000% / 0.216% | 0.000% / 0.203% |
| Native WindowJoin | 0.000% / 27.028% | 0.000% / 45.855% |
| Window completion/decode | 0.000% / 10.513% | 0.000% / 35.414% |
| RocksDB | 0.000% / 0.000% | 14.634% / 25.582% |
| Memory-budget callbacks | 0.000% / 6.096% | 0.000% / 3.745% |
| JIT compilation | 18.116% / 13.015% | 17.731% / 7.992% |
| Garbage collection | 7.243% / 1.678% | 2.657% / 0.980% |

Native window completion/decode accounts for 35.4% of captured RocksDB-job CPU samples. The
native state `visit_range` path appears in 4,048 samples; `delete_rows` appears in 2,183. The code
rescans payload ranges to recover keys already validated during decoding, then scans again to
find the end. This is redundant storage work, independent of Q8's SQL. Retaining validated entry
ordinals permits direct bounded deletion after DataFusion output drains, preserving the final
extra-payload check and Flink's timer/checkpoint boundary.

DataFusion window compute setup appears in 5.7% of in-memory and 3.6% of RocksDB-job samples.
Its sampled children are mostly nested-loop plan creation/execution and Arrow lease registration;
function-registry cloning alone is not the leading finding. The first optimization targets the
larger, simpler redundant state reads while retaining DataFusion computation. New post-change
measurements are required before claiming any improvement over this baseline.

The subsequent source change retains validated payload-entry ordinals and deletes them directly
in bounded batches after DataFusion output drains. It preserves legacy state encodings,
checkpoint boundaries and output ownership. Completion also reuses the end-of-range proof when
initial decoding already reached EOF, retaining the tail lookahead for stopped scans. Focused
native fixtures verify that acknowledgement rereads no payload bytes and that extra payloads
are still rejected before mutation. The follow-up report above verifies the resulting release;
StreamFusion remains slower than Flink on both backends.

## Capacity and excluded profiling attempts

A twenty-million-event in-memory Flink profile completes with 212,686 output records. StreamFusion
fails retained-state admission before completion: native state node `4294967347` requests 110,450
additional bytes with 42,755,635 held and 76,991 available. Its partial recording is diagnostic
only. The successful 12.5-million-event pair replaces this attempt as the longer comparison;
the failure remains recorded and limits the demonstrated capacity under these settings.

Early profiler-preload experiments are also retained separately: allocation events could not
start before JVM initialization, and a RocksDB Flink recording was rejected as corrupt by the JFR
converter. No StreamFusion RocksDB job started in that failed pair. Neither failed setup supplies
performance or complete-profile evidence. The successful post-JVM hook recordings above are the
ones analyzed here.

Raw results and metadata remain under
`streamfusion-nexmark-benchmarks/target/measurements/q8-default/d4a3db14/`: `*-1m`, `*-10m`,
`*-12500k-profile`, `collecting-validation-12500000.json`, and `window-completion-cpu.json`.
The `*-20m-profile*` directories retain the capacity/profiler failures with `failure.json`.
Profile launchers and analysis helpers remain under the benchmark module's `target/` directory.
The full Nexmark goal remains incomplete.

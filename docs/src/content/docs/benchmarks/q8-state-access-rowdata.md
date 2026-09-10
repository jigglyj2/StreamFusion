---
title: Q8 WindowJoin state-access results
description: Default-optimizer release measurements after eliminating redundant payload deletion and tail scans.
---

Release `f66d0afbb6d96e0662c3039e79ac111acb1a08f8` preserves ordinary Q8 acceleration on both
state backends with Flink's default `table.optimizer.multi-join.enabled=false`. Two general
WindowJoin changes eliminate redundant storage reads while retaining DataFusion computation.
At ten million events, StreamFusion's RocksDB median falls from 31.84s in the
[earlier baseline](/StreamFusion/benchmarks/q8-default-rowdata/) to 26.52s, a 16.7% reduction.
It still loses every measured pair on both backends. These changes improve a demonstrated
bottleneck; they do not establish optimality or a speedup over Flink.

## Measurements, September 10, 2026

Each size/backend has three fresh unprofiled JVMs per engine, ordered Flink/StreamFusion,
StreamFusion/Flink, Flink/StreamFusion. All 24 jobs complete. Timings are end-to-end, including
setup, EXPLAIN, native initialization, cluster startup, execution, cleanup and runtime JIT;
they exclude JVM launch, argument parsing and builds. No build or profiler overlaps measurement.
MAD is median absolute deviation. Ratios divide Flink's median time by StreamFusion's median.

| Events | Backend | Flink seconds: median [range]; MAD | StreamFusion seconds: median [range]; MAD | StreamFusion/Flink throughput |
| ---: | --- | --- | --- | ---: |
| 1,000,000 | In-memory | 5.862 [5.821–5.890]; 0.028 | 7.345 [7.315–7.971]; 0.030 | 0.798× |
| 1,000,000 | RocksDB | 7.139 [5.937–7.369]; 0.230 | 8.167 [8.048–8.505]; 0.119 | 0.874× |
| 10,000,000 | In-memory | 13.842 [12.511–14.490]; 0.648 | 20.037 [19.456–20.771]; 0.581 | 0.691× |
| 10,000,000 | RocksDB | 14.912 [13.205–15.988]; 1.076 | 26.523 [26.519–28.676]; 0.004 | 0.562× |

All one-million-event jobs emit 10,562 records; all ten-million-event jobs emit 106,331.
Every StreamFusion EXPLAIN reports acceleration and positive native activity; Flink reports
zero native activity. Native-plan / Calc invocation counts are:

| Events | Backend | Native-plan / Calc counts, each measured fork |
| ---: | --- | --- |
| 1,000,000 | In-memory | 844/264, 824/260, 856/270 |
| 1,000,000 | RocksDB | 1332/510, 1318/504, 1384/512 |
| 10,000,000 | In-memory | 6797/2467, 6735/2463, 6871/2475 |
| 10,000,000 | RocksDB | 11753/4924, 11753/4930, 11705/4910 |

These diagnostics do not redefine Flink's logical-record metrics. The ten-million-event native
in-memory median falls from 21.49s to 20.04s, but the ranges overlap. The one-million-event native
in-memory median increases slightly. Across the older and current measurement sets, Flink's
baselines also vary: its ten-million-event RocksDB median changes from 16.03s to 14.91s. The
cross-release differences are descriptive, not a controlled isolation of each code change.
No fork was dropped or replaced because it was slow.

## What changed and what was verified

Commit `0cf89ae71cd1adc200648655000d968325477a08` retains the physical payload-entry ordinals
already validated during window decoding. After DataFusion output drains, it deletes those
entries directly in batches of at most 256, without rereading their values to rediscover keys.
The retained ordinal vector owns its coarse reservation; transferring that credit does not
reserve the same memory twice. Old unpaged rows and variable-sized payload pages use the same
validated-entry contract.

Commit `f66d0afb` also retains whether the initial range scan actually reached EOF. If it did,
completion reuses that proof. If the visitor stopped at a page boundary, completion still reads
the tail and rejects unexpected entries. The proof is invocation-local: input and checkpoints
cannot mutate the window prefix while its watermark output is draining. Persisted state
encodings, corruption checks, output ordering and checkpoint boundaries remain unchanged.

DataFusion's nested-loop join still computes window matches. Arrow batches remain within the
fused native plan; there is no new RowData path, whole-batch transport copy, JNI crossing, tuning
option or upstream algorithm modification. Flink retains planning, distribution, managed-memory
shares, timers and recovery. See the [WindowJoin contract](/StreamFusion/operators/joins/#attached-inner-window-joins)
for the supported subset and precise fallbacks.

Validation at the measured release passes 50 focused native WindowJoin tests on both backends,
including wide multi-page payloads, legacy-state restore, memory-credit return and unexpected
payloads after both a short page and an exact 256-row page. Corruption is rejected before state
mutation. Twenty-six Java SQL/metric/channel-recovery/rescaling tests and sixteen Q5/Q8 production
integration cases pass. Each query's eight integration cases cover both backends, parallelism
one/four and both optimizer settings, with collecting parity and blackhole counts.

Four separate collecting jobs at 12.5 million events also complete, one per engine/backend.
All emit 133,078 records; complete output and materialized hashes are
`a275055412e0a03336506a7f9edf44091fa0bec4149024b9ebc284474dd51e59`.
Independent network scheduling does not impose a total order on tied windows: these query
checks preserve each record's bytes and multiplicity, while fixed-arrival operator tests
compare ordered changelogs. Blackhole counts alone are not parity evidence.

## Configuration and artifacts

Both engines use original Q8 SQL, the deterministic official Nexmark RowData source and the
unmodified Flink blackhole sink, with parallelism four, UTC, mini-batching disabled, one-second
exactly-once checkpoints, 1 GiB managed memory and consumer weights
`OPERATOR:70,STATE_BACKEND:70,PYTHON:30`. Both JVMs use `-Xms1g -Xmx1g`,
`-XX:MaxDirectMemorySize=2g`, `-XX:ActiveProcessorCount=4` and the required `java.nio` opening.
No Kafka services or connector benchmarks are involved.

The host is Intel Core i7-12650H, sixteen reported logical CPUs, WSL2 Linux 6.18.33.2,
approximately 7.6 GiB RAM and 2 GiB swap. OpenJDK is 24.0.2+12-54; Rust is 1.94,
DataFusion 55 and Arrow Rust 59.2. Native artifacts use release optimization, native CPU features,
frame pointers and separate DWARF symbols, with CPU baseline `44dd0ad765af32a3`.
The clean measured checkout is the full `f66d0afb` revision above. Artifact SHA-256 values are:

- Core: `ca1dfda85e258e4cd4c8c87ff620c62c497497b6f5f61a28fdd7a5730642d39b`.
- RocksDB: `fe1af76cd4e48dc789eca1eb720d1fdea5b67d40f465956401c39a6653f08862`.

Metadata records exact commands, classpath, machine/runtime details and the allowed Flink patch
fingerprint. Upstream revisions are Flink `c0f8d1a1e09f209885a88f9c19ceb9d9e9870283` and Nexmark
`6b3646c3baec701f1fa74baf938d235f742e5d3c`. Flink's source, sink and operator algorithms are unchanged.

## Longer mixed profiles

Each engine/backend has a separate successful 12.5-million-event async-profiler 4.5 recording,
with CPU/wall sampling at 10 ms, Java non-safepoint sampling, native DWARF/frame-pointer unwinding,
JFR and Java allocation sampling at 2 MiB. All emit 133,078 records. StreamFusion native-plan / Calc
counts are 8,568/3,096 in memory and 14,672/6,150 on RocksDB. Profile timings are excluded above.
Per-engine flame graphs, CPU/wall/allocation collapsed stacks and CPU differential flame graphs
are retained under the benchmark module's `target/` directory.

The diagnostic launcher calls upstream `asprof_init` through Java FFM after JVM startup and before
execution, enabling hooks for newly created native threads alongside the JVM agent. The native
RocksDB library is preloaded for symbol resolution in its profile only. Neither operation occurs
in measured forks. The Flink RocksDB profile has 158 background-flush/thread-pool CPU samples;
no sample matches those same background functions in the native-job recording. That zero does
not establish absence or negligible cost of native background work in other forks.

Percentages are inclusive shares of captured CPU samples, Flink / StreamFusion. Totals are
6,377 / 9,599 in memory and 7,148 / 13,890 on RocksDB. Categories overlap: JNI includes downstream
execution and DataFusion frames include custom adaptations. Missing samples or callers do not
prove zero cost; these are not elapsed-time ratios.

| CPU category | In-memory F / SF | RocksDB F / SF |
| --- | ---: | ---: |
| Source polling | 53.646% / 33.243% | 48.559% / 24.528% |
| Row copying | 23.475% / 8.376% | 19.936% / 6.033% |
| RowData-to-Arrow writing | 0.000% / 3.011% | 0.000% / 1.951% |
| Arrow C Data / JNI | 0.000% / 43.494% | 0.000% / 58.128% |
| Native plan lowering | 0.000% / 0.000% | 0.000% / 0.007% |
| DataFusion execution | 0.000% / 34.056% | 0.000% / 43.931% |
| Arrow-backed output access | 0.000% / 0.250% | 0.000% / 0.274% |
| Native WindowJoin | 0.000% / 26.117% | 0.000% / 37.027% |
| Window completion/decode | 0.000% / 9.282% | 0.000% / 24.334% |
| RocksDB | 0.000% / 0.000% | 15.753% / 17.898% |
| Memory-budget callbacks | 0.000% / 5.792% | 0.000% / 4.428% |
| JIT compilation | 17.406% / 13.564% | 17.893% / 9.914% |
| Garbage collection | 7.841% / 1.615% | 2.434% / 1.159% |

The RocksDB comparison confirms the targeted reduction. The table uses identical stack-match
rules across the baseline, direct-deletion intermediate release and final release. These are
single profile forks with overlapping categories, not additional performance measurements.

| Captured StreamFusion CPU samples | Baseline `d4a3db14` | Direct deletion `0cf89ae7` | Validated EOF `f66d0afb` |
| --- | ---: | ---: | ---: |
| Total | 16,742 | 15,050 | 13,890 |
| Window completion/decode | 5,929 | 4,289 | 3,380 |
| State `visit_range` | 4,048 | 2,455 | 1,621 |
| Payload deletion | 2,183 | 455 | 475 |
| Payload deletion with range visit | 1,752 | 0 | 0 |
| Completion with payload decode | 1,140 | 1,175 | 0 |
| DataFusion closed-window setup | 598 | 647 | 591 |

Window completion/decode falls from 35.4% to 24.3% of captured RocksDB-job CPU samples. Required
initial payload decoding remains. The sampled deletion range visits disappear after direct
key deletion; sampled completion tail decoding disappears after reusing validated EOF. Tests
still cover paths that must perform the tail check, so the zero is specific to this recording.

The corresponding native-job wall samples show synchronous snapshot work nearly unchanged
(770 before, 763 after), including native flush waiting (605 before, 595 after), while window
completion samples fall from 3,690 to 769. Wall samples aggregate threads and overlap; these
counts cannot be converted into process elapsed time. They support the state-read finding
without attributing every timing difference to it.

Remaining sampled work includes required state decoding, per-window DataFusion setup, source
processing and memory-budget callbacks. Closed-window DataFusion setup remains about 5.9% of
in-memory and 4.3% of RocksDB CPU samples. The measured evidence does not justify replacing
DataFusion, weakening memory accounting or batching away Flink-visible results. Larger changes
to join scheduling would need separate semantic and ownership validation. This checkpoint
completes the two demonstrated redundant-read fixes and retains the performance gap explicitly.

## Scope and retained evidence

The earlier twenty-million-event in-memory capacity failure belongs to `d4a3db14`; this release
does not rerun that size or establish that its capacity limit is resolved. Its demonstrated
collecting/profile size is 12.5 million events on both backends. Earlier enabled-multi-join
measurements use different settings and must not be pooled with this default-path comparison.

Raw results are under `streamfusion-nexmark-benchmarks/target/measurements/q8-default/f66d0afb/`:
`*-1m`, `*-10m`, `*-12500k-profile`, `collecting-validation-12500000.json` and
`window-completion-cpu.json`. The intermediate `0cf89ae7/` directory retains all 24 measured
jobs, four longer profiles and four successful collecting jobs; it is not substituted for the
final release above. The [baseline report](/StreamFusion/benchmarks/q8-default-rowdata/) retains
its original measurements and failed larger/profiler attempts.
Q9 is the next numbered checkpoint; the full Nexmark goal remains incomplete.

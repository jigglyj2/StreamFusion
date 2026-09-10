---
title: Q17 RowData release comparison
description: Original auction statistics, exact-result validation and release performance on both backends.
---

Original Q17 passes ordinary whole-plan admission on both state backends. Release
`88d4e066ea1c855d3505d5543fbe9c557d8ef941` runs its unchanged auction/day statistics SELECT
using the existing DataFusion COUNT, filtered COUNT, MIN, MAX and SUM accumulators, including
the documented Flink integer SUM width and AVG division adaptations. No new operator subset,
planner exception, runtime setting or query-specific compute path was introduced.

## Unprofiled release measurements

Each case uses three fresh-JVM pairs, alternating F/SF, SF/F, F/SF. Times are end-to-end seconds;
MAD is median absolute deviation. Ratios divide Flink median time by StreamFusion median time,
so values above 1 favor StreamFusion. All forks are retained. Profile timings are excluded.

| Backend | Events | Flink median [range]; MAD (s) | StreamFusion median [range]; MAD (s) | Throughput ratio |
| --- | ---: | ---: | ---: | ---: |
| hashmap | 1,000,000 | 5.308 [5.278, 5.437]; 0.030 | 5.948 [5.913, 5.981]; 0.033 | 0.892× |
| rocksdb | 1,000,000 | 5.634 [5.560, 6.190]; 0.074 | 6.000 [5.963, 6.776]; 0.037 | 0.939× |
| hashmap | 10,000,000 | 13.856 [13.730, 16.580]; 0.126 | 12.553 [12.514, 21.470]; 0.039 | 1.104× |
| rocksdb | 10,000,000 | 15.153 [14.976, 16.276]; 0.177 | 13.414 [13.342, 13.554]; 0.072 | 1.130× |

At 1M, all three pairs favor Flink on each backend. In-memory ranges are disjoint; RocksDB
ranges overlap. At 10M, StreamFusion wins two in-memory pairs and all three RocksDB pairs.
The 10M in-memory ranges overlap, including the retained 21.470-second native fork; the other
two native forks take 12.514 and 12.553 seconds. The 10M RocksDB ranges are disjoint.
These are local results, not a universal speedup or evidence of a performance ceiling.

## Correctness and acceleration

`NexmarkQ17PlanningIT` runs the original SQL resource through ordinary planner selection and
the original blackhole sink on both backends. `NexmarkQ17ProductionIT` checks its SELECT and
result schema, then separately runs collecting and blackhole jobs at parallelism one and four.
Single-source collecting jobs compare the complete changelog digest. Parallel collecting jobs
compare final materializations and counts: independent input-channel interleavings need not
produce the same sequence of transient updates. The generated aggregate harness supplies an
identical input order and compares the complete changelog byte-for-byte.

The checkpoint passed 25 focused checks: four original-SQL planning cases, five production cases,
two catalog tests and 14 shared aggregate SQL/runtime tests. The latter cover generated parity,
nullable/filter/retract behavior, full metric surface, Arrow topology, aligned/unaligned
checkpoint restore, actual channel replay and rescaling. Existing integer overflow/truncating
AVG, large-buffer admission, canonical cross-backend state and ownership tests apply unchanged.
See [group aggregation](/StreamFusion/operators/group-aggregation/) and
[native state](/StreamFusion/development/native-state/) for the supported contracts and limits.

Every measured and profiled native fork reports whole-plan acceleration and positive native
activity. Flink reports no native activity. The output count matches between engines in every
fork; these blackhole counts supplement the separate collecting validation.

| Backend | Events | Kind | Blackhole records per engine/fork | Native plan / Calc batches |
| --- | ---: | --- | ---: | --- |
| hashmap | 1,000,000 | measured, three forks | 920,000 | 404 / 140, 410 / 142, 398 / 138 |
| hashmap | 10,000,000 | measured, three forks | 9,200,000 | 3710 / 1242, 3752 / 1256, 3752 / 1256 |
| hashmap | 20,000,000 | profile only | 18,400,000 | 7394 / 2470 |
| rocksdb | 1,000,000 | measured, three forks | 920,000 | 416 / 144, 410 / 142, 424 / 144 |
| rocksdb | 10,000,000 | measured, three forks | 9,200,000 | 3728 / 1248, 3746 / 1254, 3724 / 1248 |
| rocksdb | 20,000,000 | profile only | 18,400,000 | 7445 / 2488 |

## Separate mixed JVM/native profiles

Both backends have separate 20M-event profile pairs. JFR, CPU and allocation collapsed stacks,
per-engine flame graphs and differential flame graphs are retained locally. Shares use all
process CPU samples; categories are inclusive and overlap. JNI includes downstream execution.
The broad DataFusion category includes stream wrappers around custom state work. Zero means
no matching sample, not zero actual cost.

| Inclusive CPU sample category | hashmap F / SF (%) | rocksdb F / SF (%) |
| --- | ---: | ---: |
| memory budget callbacks | 0.000 / 0.259 | 0.000 / 0.280 |
| garbage collection | 16.308 / 1.555 | 1.300 / 1.333 |
| row copy | 29.854 / 12.477 | 22.444 / 11.827 |
| rowdata to arrow write | 0.000 / 3.321 | 0.000 / 3.396 |
| arrow c data jni inclusive | 0.000 / 33.381 | 0.000 / 38.776 |
| native plan lowering | 0.000 / 0.000 | 0.000 / 0.008 |
| DataFusion frames, inclusive | 0.000 / 31.107 | 0.000 / 29.114 |
| datafusion functions and expressions | 0.000 / 8.792 | 0.000 / 7.573 |
| scalar to array conversion | 0.000 / 0.038 | 0.000 / 0.042 |
| native group aggregate | 0.000 / 27.536 | 0.000 / 25.488 |
| native state serialization | 0.000 / 0.307 | 0.000 / 0.331 |
| arrow gather | 0.000 / 0.307 | 0.000 / 0.348 |
| arrow row view access | 0.000 / 4.636 | 0.000 / 4.288 |
| source poll inclusive | 55.943 / 46.194 | 44.370 / 41.934 |
| rocksdb inclusive | 0.000 / 0.000 | 31.055 / 9.560 |
| rocksdb index read | 0.000 / 0.000 | 0.169 / 0.008 |
| rocksdb decompression | 0.000 / 0.000 | 0.806 / 0.739 |
| native artifact loading | 0.000 / 0.384 | 0.000 / 0.458 |
| jit compile | 8.000 / 10.730 | 6.598 / 10.944 |

CPU sample counts (Flink / StreamFusion): hashmap 12,350 / 10,419; RocksDB 16,004 / 11,778.

Source polling remains substantial, with deterministic source string generation and row copying
among the leading sampled leaves. In StreamFusion runs, row copying uses 12.477% / 11.827% of CPU samples;
RowData-to-Arrow writing uses 3.321% / 3.396%, and output views use 4.636% / 4.288%
(hashmap / RocksDB). The sink writer itself matches only 14 / 16 native-run samples.
These source and sink paths retain their required Flink behavior.

The per-record DataFusion accumulator adapter accounts for 17.545% / 15.257% of native-run
samples, including its callees. Its leading costs include `RowKernels::apply`, Arrow slice
construction and reference-counted buffer release. Q17 has no DISTINCT aggregate: the shared
analysis script's broad membership category also matches this common row adapter and must not
be interpreted as DISTINCT work. Columns and accumulators are already prepared/reused across
the incoming batch; intermediate results preserve Flink's required changelog. Further batching
would need to preserve every intermediate result, including repeated keys, and has not been
implemented or measured here. Replacing the accumulators with handwritten arithmetic would
violate the DataFusion compute rule.

RocksDB accounts for 9.560% of native-run samples, versus 31.055% in Flink. Index reads and
decompression are small in this profile. Native plan lowering, scalar-to-array conversion and
memory callbacks each remain below 0.3%. These profiles support retaining the existing compute
path at this checkpoint. They do not establish the cause of the slow 10M in-memory fork or
prove that further improvements are exhausted. Short end-to-end runs include startup and JIT;
the measured 1M deficit must not be attributed entirely to those costs without further evidence.

For RocksDB profiles only, a launcher loads the same verified plugin through JVM `System.load`
before benchmark main, allowing async-profiler to resolve symbols in the library normally loaded
from Rust via `dlopen`. Measured forks never use that launcher. Profiled throughput is excluded.

## Method, artifacts and limits

Both engines use parallelism 4, mini-batching disabled, one-second exactly-once checkpoints,
no restarts, UTC, 1 GiB managed memory and `OPERATOR:70,STATE_BACKEND:70,PYTHON:30`.
JVM flags are `-Xms1g -Xmx1g -XX:MaxDirectMemorySize=2g -XX:ActiveProcessorCount=4`;
no CPU affinity is applied. RocksDB uses Flink's default full/private-file checkpoint strategy.
No builds run alongside measured or profiled forks.

Timing includes Java-only counter reset, setup, EXPLAIN preflight, native initialization when
selected, cluster startup, execution and cleanup. It excludes JVM launch, argument parsing and
build time. The host is WSL2 Linux, Intel Core i7-12650H, 16 logical CPUs, approximately 7.6 GiB
RAM and 2 GiB swap, Java 24.0.2. Native artifacts use release optimization, native CPU features,
frame pointers and profiling symbols without reducing optimization. CPU baseline fingerprint:
`44dd0ad765af32a3`. Async-profiler 4.5 uses CPU sampling at 10 ms, Java non-safepoint sampling,
native DWARF unwinding, JFR output and allocation sampling at 2 MiB.

Verified benchmark JAR artifact SHA-256 values:

- Native runtime: `55d7010b67473690440b0310ce51483192b272709a804c5f1d096e5e94bba793`.
- RocksDB plugin: `fe1af76cd4e48dc789eca1eb720d1fdea5b67d40f465956401c39a6653f08862`.

Upstream Flink is `c0f8d1a1e09f209885a88f9c19ceb9d9e9870283`, with only the approved
planner/class-loading installation and complete-StreamGraph memory callback. Nexmark is clean
at `6b3646c3baec701f1fa74baf938d235f742e5d3c`. The source is the deterministic RowData adapter
and the sink is unmodified Flink blackhole. No Kafka service or connector benchmark is involved.

Exact commands, metadata, results, counters and profiles are under
`streamfusion-nexmark-benchmarks/target/measurements/q17/88d4e066/`. All six campaign cases
completed successfully. The 20M profiles are not an unprofiled performance comparison or proof
of restore capacity at that size. Mini-batch mode and other deployment sizes are outside this
measurement. Q17 has reached its admission, correctness and performance checkpoint; Q18 is next.

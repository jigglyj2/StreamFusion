---
title: Q18 RowData release comparison
description: Original last-bid deduplication, DataFusion cumulative windows, parity and release measurements.
---

Original Q18 passes ordinary whole-plan admission on both state backends. Release
`80df432a4db095e8ab72db4aa139069354bdd2ac` runs its unchanged last-bid SELECT using
DataFusion cumulative MIN/MAX window expressions. StreamFusion groups input indices by key
and adapts the running extrema to Flink's per-arrival changelog. Keep-last accepts timestamp
ties; keep-first requires strict improvement. It does not coalesce intermediate winners.

The native region passes Arrow batches between its Calc and deduplication stages. It reserves
the fixed-width DataFusion workspace once per batch, reads state in one batch and flushes dirty
keys in one batch. No handwritten extremum computation, query-specific execution shortcut,
new deployment setting or upstream operator modification was introduced.

## Unprofiled release measurements

Each completed case uses three fresh-JVM pairs, alternating F/SF, SF/F, F/SF. Times are
end-to-end seconds; MAD is median absolute deviation. Ratios divide Flink median time by
StreamFusion median time, so values above 1 favor StreamFusion. All forks are retained;
profile timings are excluded.

| Backend | Events | Flink median [range]; MAD (s) | StreamFusion median [range]; MAD (s) | Throughput ratio |
| --- | ---: | ---: | ---: | ---: |
| hashmap | 1,000,000 | 5.458 [5.217, 12.520]; 0.240 | 6.008 [5.988, 6.196]; 0.019 | 0.908× |
| hashmap | 2,000,000 | 6.212 [6.197, 6.268]; 0.015 | 6.960 [6.911, 7.114]; 0.049 | 0.893× |
| rocksdb | 1,000,000 | 6.010 [6.003, 6.072]; 0.007 | 6.284 [6.265, 6.297]; 0.012 | 0.956× |
| rocksdb | 10,000,000 | 79.537 [72.289, 105.947]; 7.248 | 17.949 [17.331, 24.174]; 0.618 | 4.431× |

At 1M events, Flink has the lower median on both backends. StreamFusion wins the first
in-memory pair, whose Flink fork takes 12.520 seconds; Flink wins the other two pairs and
the ranges overlap. All three 1M RocksDB pairs favor Flink, with disjoint ranges.
At 10M RocksDB events, all three pairs favor StreamFusion and the ranges are disjoint.
The wide dispersion in both engines is retained. These local results do not establish a
universal speedup or a performance ceiling.

The first 10M-event in-memory Flink fork failed with a TaskManager heartbeat timeout and
produced no benchmark result. The case stopped before any native fork was attempted.
This is neither an established out-of-memory diagnosis nor a StreamFusion capacity result;
there is no speedup ratio for that case. The separate 2M-event in-memory comparison uses the
same heap, settings and artifacts.
All three pairs favor Flink, with disjoint ranges; the native median is 6.960 seconds versus
6.212 seconds for Flink (0.893× throughput).

## Correctness and acceleration

The compute prerequisite passed 21 Rust and 136 Java checks. These cover generated
key/envelope and changelog parity, timestamp extrema and ties, coarse workspace/history
memory refusal, complete shared metrics, Arrow topology, canonical cross-backend restore,
aligned/unaligned checkpoints, 1-to-2-to-1 rescaling, incremental SST reuse and actual
two-channel Arrow IPC replay. The admission checkpoint added five focused planner unit checks
and nine original-SQL planning/production integration cases.

Original Q18 collecting validation uses 50,000 events at parallelism one and four on both
backends, separately from the unmodified blackhole sink. One-source jobs match the complete
ordered changelog and final result digest byte-for-byte. With four readers, equal timestamps
can produce different legal last payloads under different channel interleavings. The validator
replays the original seeded readers and checks every full result payload against their legal
last candidates at the greatest timestamp, including key coverage and one row per key.
There are 167 ambiguous keys among 13,670 final keys in this validation configuration.
This is not a claim of identical final bytes across different interleavings. Controlled
shared-runtime tests compare complete changelogs for identical arrival order.

See [deduplication](/StreamFusion/operators/deduplication/) for supported types, changelog,
memory, metrics and recovery contracts. Processing-time SQL, timer-backed insert-only row-time
output, unsupported field types, TTL, async state and mini-batching retain precise whole-plan
fallback. This checkpoint does not remove those semantic restrictions.

Every completed measured and profiled native fork reports whole-plan acceleration and
positive native activity. Flink reports zero native activity. Both engines emit the same
blackhole record count in every completed fork. Counts supplement the separate collecting
validation; they are not a byte-parity proof at benchmark scale.

| Backend | Events | Kind | Blackhole records per engine/fork | Native plan / Calc batches |
| --- | ---: | --- | ---: | --- |
| hashmap | 1,000,000 | measured, three forks | 920,000 | 398 / 138, 392 / 136, 392 / 136 |
| hashmap | 2,000,000 | measured, three forks | 1,840,000 | 776 / 264, 800 / 272, 782 / 266 |
| rocksdb | 1,000,000 | measured, three forks | 920,000 | 392 / 136, 416 / 144, 392 / 136 |
| rocksdb | 10,000,000 | measured, three forks | 9,200,000 | 3824 / 1280, 3728 / 1248, 3710 / 1242 |
| hashmap | 2,000,000 | profile only | 1,840,000 | 776 / 264 |
| hashmap | 4,000,000 | profile only | 3,680,000 | 1502 / 506 |
| rocksdb | 20,000,000 | profile only | 18,400,000 | 7466 / 2494 |

## Separate mixed JVM/native profiles

Separate profile pairs completed at 4M in-memory events and 20M RocksDB events, both longer
than their largest completed unprofiled comparison. An additional 2M in-memory profile pair
is retained in the artifacts; the table below uses the longer profiles.

JFR, CPU and allocation collapsed stacks, per-engine flame graphs and differential flame
graphs are retained locally. Shares use all process CPU samples; categories are inclusive
and overlap. JNI includes downstream execution. The broad DataFusion category also includes
stream wrappers around native state work. A zero means no matching sample, not zero actual cost.

| Inclusive CPU sample category | hashmap 4M F / SF (%) | rocksdb 20M F / SF (%) |
| --- | ---: | ---: |
| native deduplication | 0.000 / 12.278 | 0.000 / 11.418 |
| DataFusion window evaluation | 0.000 / 3.107 | 0.000 / 3.924 |
| memory budget callbacks | 0.000 / 0.241 | 0.000 / 0.269 |
| garbage collection | 15.771 / 3.318 | 1.183 / 1.536 |
| row copy | 20.871 / 10.649 | 12.362 / 13.123 |
| RowData-to-Arrow writing | 0.000 / 2.172 | 0.000 / 2.695 |
| Arrow C Data / JNI, inclusive | 0.000 / 13.906 | 0.000 / 37.649 |
| native plan lowering | 0.000 / 0.000 | 0.000 / 0.000 |
| DataFusion frames, inclusive | 0.000 / 11.885 | 0.000 / 12.670 |
| DataFusion functions and expressions | 0.000 / 3.439 | 0.000 / 4.515 |
| scalar-to-array conversion | 0.000 / 0.030 | 0.000 / 0.061 |
| Arrow gather | 0.000 / 1.056 | 0.000 / 1.198 |
| Arrow output view access | 0.000 / 5.400 | 0.000 / 6.711 |
| source polling, inclusive | 34.353 / 29.623 | 23.056 / 35.261 |
| RocksDB, inclusive | 0.000 / 0.000 | 51.088 / 25.447 |
| RocksDB index reads | 0.000 / 0.000 | 3.188 / 0.038 |
| RocksDB decompression | 0.000 / 0.000 | 7.788 / 1.106 |
| native artifact loading | 0.000 / 1.207 | 0.000 / 0.461 |
| JIT compilation | 20.896 / 28.356 | 3.968 / 10.443 |

CPU sample counts (Flink / StreamFusion): hashmap 4,020 / 3,315; RocksDB 27,568 / 13,023.

DataFusion window evaluation accounts for 3.107% / 3.924% of native-run samples (hashmap /
RocksDB). The complete native deduplication category is 12.278% / 11.418%. Native leaves include
Arrow array construction/release, selection/state adaptation and DataFusion window evaluation;
the profile does not demonstrate a dominant compute kernel that warrants another rewrite.
The cumulative-window helper is largely inlined: its own near-zero frame count does not mean
DataFusion is bypassed.

Source polling remains substantial, including deterministic string generation and row copying.
Native-run RowData-to-Arrow writing takes 2.172% / 2.695%, output views 5.400% / 6.711%, and
row copying 10.649% / 13.123%. JNI's inclusive share includes native computation and state calls;
it must not be interpreted as pure transport overhead. Native plan lowering has no matched
samples, while memory callbacks and scalar-to-array conversion each remain below 0.3%.

The 20M RocksDB profiles show 51.088% of Flink samples and 25.447% of native-run samples in
RocksDB. Leading Flink leaves include skip-list search, cache lookup and decompression. Native
RocksDB costs include batched reads (14.820% inclusive) and writes (4.669% inclusive);
index reads and decompression account for 0.038% and 1.106%. This supports retaining the batched
state path. It is not an isolated A/B proof attributing the measured speedup to one implementation
difference, and percentages from different total sample counts are not elapsed-time ratios.

The short in-memory profiles include substantial JIT work: 28.356% of native-run samples at
4M, alongside 29.623% in source polling. Flink's GC share is 15.771%, versus 3.318% for native.
These observations do not establish the cause of the failed 10M baseline or explain the entire
1M/2M native deficit. This checkpoint retains the current general DataFusion compute path;
it does not claim further improvement is exhausted.

For RocksDB profiles only, a launcher loads the same verified plugin through JVM `System.load`
before benchmark main, allowing async-profiler to resolve symbols in the library normally
loaded from Rust via `dlopen`. Measured forks never use that launcher. Profiled throughput is excluded.

## Method, artifacts and limits

Both engines use parallelism 4, mini-batching disabled, one-second exactly-once checkpoints,
no restarts, UTC, 1 GiB managed memory and `OPERATOR:70,STATE_BACKEND:70,PYTHON:30`.
JVM flags are `-Xms1g -Xmx1g -XX:MaxDirectMemorySize=2g -XX:ActiveProcessorCount=4`;
no CPU affinity is applied. RocksDB uses Flink's default full/private-file checkpoint strategy.
No builds run alongside measured or profiled forks.

Timing includes Java-only counter reset, setup, EXPLAIN preflight, native initialization when
selected, cluster startup, execution and cleanup. It excludes JVM launch, argument parsing
and build time. The host is WSL2 Linux, Intel Core i7-12650H, 16 logical CPUs, approximately
7.6 GiB RAM and 2 GiB swap, Java 24.0.2. Native artifacts use release optimization, native
CPU features, frame pointers and profiling symbols without reducing optimization. CPU baseline
fingerprint: `44dd0ad765af32a3`. Async-profiler 4.5 uses CPU sampling at 10 ms, Java non-safepoint
sampling, native DWARF unwinding, JFR output and allocation sampling at 2 MiB.

Verified benchmark JAR artifact SHA-256 values:

- Native runtime: `e1952c34ba7ffe9f3ad13a0d26dead9e649be52f78c2a7afa71fb91ed5c57c66`.
- RocksDB plugin: `fe1af76cd4e48dc789eca1eb720d1fdea5b67d40f465956401c39a6653f08862`.

Upstream Flink is `c0f8d1a1e09f209885a88f9c19ceb9d9e9870283`, with only the approved
planner/class-loading installation and complete-StreamGraph memory callback. Nexmark is clean
at `6b3646c3baec701f1fa74baf938d235f742e5d3c`. The source is the deterministic RowData adapter
and the sink is unmodified Flink blackhole. No Kafka service or connector benchmark is involved.

Exact commands, metadata, results, counters and profiles are under
`streamfusion-nexmark-benchmarks/target/measurements/q18/80df432a/`. Failed cases remain recorded
alongside completed cases. Longer profiles are neither unprofiled performance comparisons nor
proof of restore capacity at their event counts. Other deployment sizes and mini-batch mode
are outside this measurement.

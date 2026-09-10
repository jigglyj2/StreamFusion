---
title: Q16 RowData release comparison
description: Native RocksDB ownership repair, exact-result validation and release performance.
---

Original Q16 passes ordinary whole-plan admission on both state backends. This report measures
release `94d996f0b7466bd5a078b11c509302857e202df8`, after correcting native RocksDB ownership.
The unchanged SELECT uses DataFusion string MAX, BIGINT COUNT and filtered DISTINCT counts
with composite channel/day grouping. Source and sink are the deterministic Nexmark RowData
adapter and the unmodified Flink blackhole sink. There is no Kafka interaction.

## Unprofiled release measurements

Each case uses three fresh-JVM pairs, alternating F/SF, SF/F, F/SF. Times are end-to-end seconds.
MAD is median absolute deviation. Ratios divide Flink median time by StreamFusion median time;
values above 1 favor StreamFusion. All forks are retained. Profile timings are excluded.

| Backend | Events | Flink median [range]; MAD (s) | StreamFusion median [range]; MAD (s) | Throughput ratio |
| --- | ---: | ---: | ---: | ---: |
| hashmap | 1,000,000 | 6.541 [5.791, 6.693]; 0.152 | 13.418 [7.773, 13.459]; 0.042 | 0.487× |
| rocksdb | 1,000,000 | 8.814 [8.712, 13.128]; 0.102 | 8.218 [8.100, 8.588]; 0.117 | 1.073× |
| rocksdb | 10,000,000 | 86.909 [63.564, 101.928]; 15.019 | 64.174 [55.467, 73.671]; 8.707 | 1.354× |

At 1M, all three in-memory pairs favor Flink, with disjoint timing ranges. The first native
fork takes 7.773 seconds; the next two take 13.418 and 13.459 seconds. All are included, and
the current CPU profile does not establish the cause of that wall-time spread. All three 1M
RocksDB pairs favor StreamFusion, also with disjoint ranges. At 10M RocksDB, StreamFusion wins
two pairs and Flink wins one; ranges overlap substantially. The median gain is local evidence,
not a universal speedup or proof of a performance ceiling.

## Correctness and ownership

Opt-in Q16 integration validates 50,000 events at parallelism one and four on both backends.
It checks the original SELECT/result schema, exact single-source collecting changelogs, final
parallel materializations, independent blackhole counts and positive native activity. Independent
parallel jobs can interleave channels differently; their intermediate changelogs are not compared
as if their input order were identical. The generated harness supplies identical order and compares
complete changelog bytes, record envelopes and the full registered metric surface.

[Group aggregation](/StreamFusion/operators/group-aggregation/) records Unicode/composite-key/FILTER
coverage, the original HASH boundary required for Flink string comparison, large-buffer denial
and ownership tests, canonical cross-backend restore, full/incremental aligned/unaligned
checkpoints, real channel replay and rescaling. The ownership repair passed 19 focused runtime
checks, 53 SQL/parity/metric/recovery tests and nine Q16 planning/production checks.

The previous backend identified native owners using a transformation-name prefix, while Flink
provides a runtime-class/operator-ID/subtask identifier. It therefore opened an unneeded Java
RocksDB database and assigned native storage a fallback allowance from the operator pool.
Native owners now register the exact Flink identity before backend creation. Their Java delegate
is the intended heap lifecycle shell; native storage receives the standard `STATE_BACKEND` lease.
This changes no deployment budget, RocksDB option, storage algorithm, native state encoding or
checkpoint algorithm. The former empty Java shell has no managed snapshot handle to migrate
under either full or incremental checkpointing. See [native state](/StreamFusion/development/native-state/).

A live 10M native fork confirmed four native databases, each with a 111,848,106-byte cache,
and no Java RocksDB database alongside them. The previous path had four native caches of
14,913,080 bytes plus four Java caches of 111,848,106 bytes. Ordinary Flink has four
223,696,213-byte caches: it does not declare the native `OPERATOR` consumer. Both engines
receive the same 1 GiB managed budget and 70/70/30 consumer weights. These are configured
cache capacities, not additive live memory usage; write buffers charge their shared cache.

## Acceleration and output evidence

| Backend | Events | Kind | Blackhole records per engine/fork | Native plan / Calc batches |
| --- | ---: | --- | ---: | --- |
| hashmap | 1,000,000 | measured, three forks | 920,000 | 392 / 136, 416 / 144, 392 / 136 |
| rocksdb | 1,000,000 | measured, three forks | 920,000 | 427 / 152, 442 / 156, 427 / 154 |
| rocksdb | 10,000,000 | measured, three forks | 9,200,000 | 3861 / 1308, 3772 / 1276, 3818 / 1290 |
| hashmap | 1,250,000 | profile only | 1,150,000 | 515 / 178 |
| rocksdb | 20,000,000 | profile only | 18,400,000 | 7505 / 2522 |

## Separate mixed JVM/native profiles

Longer forks retain JFR, CPU and allocation collapsed stacks, per-engine flame graphs and a
differential flame graph. Shares use all process CPU samples; categories are inclusive and
overlap. JNI includes downstream execution. The broad DataFusion category includes stream
wrappers around custom state work. Zero means no matching sample, not zero actual cost.

| Inclusive CPU sample category | hashmap 1,250,000 F / SF (%) | rocksdb 20,000,000 F / SF (%) |
| --- | ---: | ---: |
| memory budget callbacks | 0.000 / 0.098 | 0.000 / 0.147 |
| garbage collection | 5.828 / 2.614 | 0.706 / 0.354 |
| row copy | 15.442 / 4.379 | 8.345 / 2.638 |
| rowdata to arrow write | 0.000 / 0.980 | 0.000 / 0.553 |
| arrow c data jni inclusive | 0.000 / 29.935 | 0.000 / 85.389 |
| native plan lowering | 0.000 / 0.000 | 0.000 / 0.002 |
| DataFusion frames, inclusive | 0.000 / 28.824 | 0.000 / 20.318 |
| datafusion functions and expressions | 0.000 / 2.876 | 0.000 / 1.909 |
| scalar to array conversion | 0.000 / 0.033 | 0.000 / 0.007 |
| native group aggregate | 0.000 / 29.804 | 0.000 / 19.431 |
| native distinct membership | 0.000 / 10.621 | 0.000 / 6.036 |
| native membership state | 0.000 / 7.647 | 0.000 / 5.547 |
| native state serialization | 0.000 / 1.405 | 0.000 / 1.024 |
| arrow gather | 0.000 / 0.196 | 0.000 / 0.140 |
| arrow row view access | 0.000 / 1.797 | 0.000 / 1.176 |
| source poll inclusive | 26.132 / 13.693 | 16.406 / 8.374 |
| rocksdb inclusive | 0.000 / 0.000 | 64.854 / 69.562 |
| rocksdb index read | 0.000 / 0.000 | 3.593 / 45.126 |
| rocksdb decompression | 0.000 / 0.000 | 5.036 / 43.784 |
| native artifact loading | 0.000 / 1.307 | 0.000 / 0.086 |
| jit compile | 33.593 / 30.196 | 2.972 / 2.276 |

CPU sample counts (Flink / StreamFusion): hashmap 2694 / 3060; rocksdb 41222 / 61331.

PROFILE_At 1M, all three in-memory pairs favor Flink, with disjoint timing ranges. The first native
fork takes 7.773 seconds; the next two take 13.418 and 13.459 seconds. All are included, and
the current CPU profile does not establish the cause of that wall-time spread. All three 1M
RocksDB pairs favor StreamFusion, also with disjoint ranges. At 10M RocksDB, StreamFusion wins
two pairs and Flink wins one; ranges overlap substantially. The median gain is local evidence,
not a universal speedup or proof of a performance ceiling.

For RocksDB profiles only, a launcher loads the same verified native plugin through JVM
`System.load` before benchmark main. This lets async-profiler observe a library normally loaded
from Rust via `dlopen` and resolve its symbols. It changes no state ownership or algorithm.
Measured forks never use the launcher. Profiled throughput is not a benchmark result.

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
at `6b3646c3baec701f1fa74baf938d235f742e5d3c`. Each case records machine/runtime metadata,
upstream diffs, exact commands, counters and results. Logs, summaries, profiles and the live
database audit are under `streamfusion-nexmark-benchmarks/target/measurements/q16/94d996f0/`.

The [pre-ownership measurements](/StreamFusion/benchmarks/q16-before-ownership/) retain the
initial 90/10-weight results, the later default-weight campaign, their complete profiles and
the earlier failed larger in-memory attempts. The 2M native-memory reservation denial and
10M Flink heartbeat timeout were observed at that earlier revision; they were not rerun here
and do not establish a current maximum capacity or an out-of-memory cause for the Flink timeout.
There is no completed 10M in-memory comparison or unprofiled 20M comparison in this report.
A completed 20M RocksDB job is not proof of restore capacity at that size. The supported recovery
contracts and savepoint limits remain documented in [native state](/StreamFusion/development/native-state/).

Q16 has a verified admission/correctness checkpoint and the measured performance evidence above.
Its in-memory performance gap and remaining RocksDB index-read cost are explicit limitations.
There is no claim that all Nexmark queries outperform Flink; the next query checkpoint is Q17.

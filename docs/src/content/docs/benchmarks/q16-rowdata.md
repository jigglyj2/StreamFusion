---
title: Q16 RowData release comparison
description: Original channel statistics, release measurements and remaining capacity limits.
---

Original Q16 now passes ordinary whole-plan admission on both state backends. It uses
DataFusion string MAX and BIGINT COUNT, including filtered DISTINCT counts, with composite
channel/day grouping. This report measures release commit
`1f3cafbefcf7144b7a6e25bcce0fbc3336736e24` using the unchanged Nexmark SELECT.
At one million events, StreamFusion reaches **0.709× Flink's median throughput in memory**
and **1.389× on RocksDB**. At ten million, the RocksDB ratio is **0.912×**. This is an initial
baseline, with remaining state-capacity and performance work; it does not establish a general
speedup or a performance ceiling. These results precede the native RocksDB ownership repair
described below; release measurements after that repair remain pending.

## Validation

[Group aggregation](/StreamFusion/operators/group-aggregation/) documents the physical boundary
required for exact Flink string ordering, generated changelog/metric comparisons, large-buffer
admission and canonical/backend-switch, aligned/unaligned, channel-replay and rescaling coverage.
The imported Flink binary-string MIN/MAX SQL case and generated Unicode SQL comparisons pass
on both backends. Opt-in Q16 integration validates 50,000 events at parallelism one and four,
complete single-source changelogs, final parallel results, independent blackhole record counts
and positive native activity. These checks supplement controlled same-order changelog tests.

## Unprofiled measurements

Each completed case uses three fresh-JVM pairs, alternating engine order F/SF, SF/F, F/SF.
Times are end-to-end seconds. MAD is median absolute deviation; throughput ratio is Flink
median time divided by StreamFusion median time. Profile timings are excluded.

| Backend | Events | Engine | Median (s) | Range (s) | MAD (s) | Throughput ratio |
| --- | ---: | --- | ---: | --- | ---: | ---: |
| hashmap | 1,000,000 | flink | 6.190 | 5.993–6.551 | 0.197 | — |
| hashmap | 1,000,000 | streamfusion | 8.732 | 8.667–8.790 | 0.057 | 0.709× |
| rocksdb | 1,000,000 | flink | 17.986 | 9.267–24.624 | 6.638 | — |
| rocksdb | 1,000,000 | streamfusion | 12.947 | 9.148–15.592 | 2.645 | 1.389× |
| rocksdb | 10,000,000 | flink | 95.113 | 91.267–115.664 | 3.846 | — |
| rocksdb | 10,000,000 | streamfusion | 104.344 | 103.348–146.059 | 0.996 | 0.912× |

All three in-memory pairs favor Flink, with tight, disjoint timing ranges. All three
one-million-event RocksDB pairs favor StreamFusion, but both engines vary substantially;
their ranges overlap. All three ten-million-event RocksDB pairs favor Flink, also with overlapping
ranges. Every fork remains in the report; no startup-heavy or slower fork was discarded.

## Failed larger in-memory cases

The two-million-event StreamFusion profile failed when the native state consumer requested
another **2,241,243 bytes**, with **88,174,856 bytes** already reserved and **1,848,430 bytes**
available. The error is a managed-memory reservation denial. The successful 1.25M profile gives
longer-run evidence below this observed limit; it does not establish the maximum admissible size.

The ten-million-event measured attempt failed in its first Flink fork with a TaskManager
heartbeat timeout. That log does not establish an out-of-memory cause. The runner therefore did
not start the corresponding StreamFusion fork. There is no ten-million-event in-memory median,
completed engine comparison or twenty-million-event memory profile. Failed logs and the partial
profile remain available. No budget, checkpoint setting or source/sink behavior was changed to
make either case pass.

## Equal-weight diagnostic

A separate 20M profile at the same baseline commit used
`OPERATOR:50,STATE_BACKEND:50,PYTHON:30` for both engines. With no Python consumers, that is
the same operator/state ratio as Flink's default 70/70 weights. It retained the same total
managed memory, heap, parallelism, checkpoint and source/sink settings. It is separate from
the 90/10 measured results above and is not an optimization speedup comparison.

Flink completed with 18.4M output records. StreamFusion failed on an additional 9,271,328-byte
batch scratch/output reservation, with 27,221,600 bytes already reserved by that consumer and
8,954,619 bytes available. Those figures describe that consumer and broker availability, not
the entire task's allocation. No successful throughput or complete profile comparison is claimed.
The evidence is retained in `rocksdb/profiles-20000000-weights-50-50/` under the campaign.

This exposed a later-than-needed workspace lifetime: emptied membership lookup tables and
finished-computation credit survived while Arrow output was allocated. The phase-retirement
change described in [group aggregation](/StreamFusion/operators/group-aggregation/) is a tested
prerequisite. Repeating the same 20M equal-weight profile at release commit
`3d7317704cbe74d5d0a3bab10925892d3b43025c` completed on both engines, each emitting 18.4M
blackhole records. StreamFusion reported 7,482 native plan batches and 2,528 native Calc batches.
The retained artifacts are under
`streamfusion-nexmark-benchmarks/target/measurements/q16/3d731770/rocksdb/profiles-20000000-weights-50-50/`.

This establishes completion at the default operator/state ratio, not a throughput improvement.
Across 48,275 Flink and 73,117 StreamFusion CPU samples, inclusive RocksDB shares were 65.823%
and 75.623%. StreamFusion index-reader and decompression frames remained at 49.949% and 52.324%
(overlapping categories). A later live-database audit showed that the changed weights did not
reach the intended native cache: the unneeded Java backend held the state-backend share. Scalar
array conversion fell to 0.003% after the separate scalar-retention change; these profiles do not
isolate a causal throughput gain from that change. No profiled elapsed time is used as a result.

The harness now preserves Flink's configured/default weights instead of automatically imposing
90/10. Explicit measurement overrides remain available through the standard Flink setting.
The unprofiled results above remain the original 90/10 baseline. A subsequent default-weight
campaign at `747b033a0bcf7a787713abad6a78ae73f4a1b4ae` measured 0.492× in memory and 1.110×
on RocksDB at 1M, and 0.557× on RocksDB at 10M. These measurements preceded the ownership
repair below and cannot be presented as performance of the intended native-cache ownership.
Their full forks, dispersion and profiles remain under the corresponding `747b033a/` directory.

## Default-weight measurements before ownership repair

Release `747b033a0bcf7a787713abad6a78ae73f4a1b4ae` includes direct retention of evaluated DataFusion scalars
and retirement of completed DISTINCT computation workspace before Arrow output allocation.
Both engines receive Flink's default `OPERATOR:70,STATE_BACKEND:70,PYTHON:30` weights and the
same 1 GiB managed-memory budget. These runs precede the ownership repair described below. They are not an isolated code-change
speedup over the historical 90/10 baseline below. All other measurement settings remain the same.

Each case contains three unprofiled fresh-JVM pairs, alternating F/SF, SF/F, F/SF. Times are
end-to-end seconds; MAD is median absolute deviation. Ratios divide Flink median time by
StreamFusion median time. Every fork is retained.

| Backend | Events | Engine | Median (s) | Range (s) | MAD (s) | Throughput ratio |
| --- | ---: | --- | ---: | --- | ---: | ---: |
| hashmap | 1,000,000 | flink | 6.617 | 5.760–7.110 | 0.493 | — |
| hashmap | 1,000,000 | streamfusion | 13.461 | 7.888–14.068 | 0.607 | 0.492× |
| rocksdb | 1,000,000 | flink | 9.363 | 8.713–12.586 | 0.650 | — |
| rocksdb | 1,000,000 | streamfusion | 8.431 | 8.345–8.498 | 0.066 | 1.110× |
| rocksdb | 10,000,000 | flink | 56.121 | 55.499–87.178 | 0.623 | — |
| rocksdb | 10,000,000 | streamfusion | 100.673 | 77.866–103.391 | 2.717 | 0.557× |

| Backend | Events | Blackhole records per engine/fork | StreamFusion native plan / Calc batches |
| --- | ---: | ---: | --- |
| hashmap | 1,000,000 | 920,000 | 416 / 144, 421 / 148, 398 / 138 |
| rocksdb | 1,000,000 | 920,000 | 419 / 146, 423 / 148, 441 / 158 |
| rocksdb | 10,000,000 | 9,200,000 | 3718 / 1248, 3751 / 1254, 3715 / 1246 |

The longer memory profile uses 1.25M events at this release. The completed 20M RocksDB
profile uses `3d7317704cbe74d5d0a3bab10925892d3b43025c`, the identical native binary and
explicit 50/50/30 weights. With no Python consumers, its effective operator/state ratio equals
the 70/70/30 measured configuration. That profile predates only the harness-default change,
which its explicit override bypasses. It is reused here without rerunning the identical native path.
Neither profile contributes a throughput measurement.

| Inclusive CPU sample category | hashmap 1,250,000 F / SF | rocksdb 20,000,000 F / SF |
| --- | ---: | ---: |
| memory budget callbacks | 0.000 / 0.263 | 0.000 / 0.141 |
| garbage collection | 5.841 / 2.797 | 0.648 / 0.330 |
| row copy | 14.136 / 4.936 | 8.278 / 2.305 |
| rowdata to arrow write | 0.000 / 0.856 | 0.000 / 0.472 |
| arrow c data jni inclusive | 0.000 / 30.207 | 0.000 / 87.123 |
| native plan lowering | 0.000 / 0.033 | 0.000 / 0.001 |
| DataFusion frames, inclusive | 0.000 / 28.924 | 0.000 / 14.276 |
| datafusion functions and expressions | 0.000 / 3.192 | 0.000 / 1.510 |
| scalar to array conversion | 0.000 / 0.000 | 0.000 / 0.003 |
| native group aggregate | 0.000 / 29.385 | 0.000 / 13.493 |
| native distinct membership | 0.000 / 10.925 | 0.000 / 4.839 |
| native membership state | 0.000 / 7.634 | 0.000 / 3.675 |
| native state serialization | 0.000 / 2.205 | 0.000 / 0.732 |
| arrow gather | 0.000 / 0.066 | 0.000 / 0.096 |
| arrow row view access | 0.000 / 2.303 | 0.000 / 0.996 |
| source poll inclusive | 24.260 / 13.689 | 14.890 / 7.198 |
| rocksdb inclusive | 0.000 / 0.000 | 65.823 / 75.623 |
| native artifact loading | 0.000 / 1.283 | 0.000 / 0.082 |
| jit compile | 33.567 / 29.253 | 2.970 / 1.990 |

All shares use total process CPU samples. Categories overlap; JNI includes downstream work,
and the broad DataFusion category includes stream wrappers. A zero is no matching sample,
not proof that an operation has no cost.

hashmap profile: 2568 / 3039 Flink / StreamFusion CPU samples; both engines emit
1,150,000 blackhole records. Native plan / Calc batches: 500 / 176.

rocksdb profile: 48275 / 73117 Flink / StreamFusion CPU samples; both engines emit
18,400,000 blackhole records. Native plan / Calc batches: 7482 / 2528.

New run logs, metadata, medians, CPU/alloc collapsed stacks, JFRs, per-engine flame graphs
and the memory differential flame graph are retained under
`streamfusion-nexmark-benchmarks/target/measurements/q16/747b033a/`.
The reused RocksDB profile and differential graph remain at the path in the equal-weight
diagnostic section. The verified native SHA-256 is
`55d7010b67473690440b0310ce51483192b272709a804c5f1d096e5e94bba793`;
RocksDB remains `fe1af76cd4e48dc789eca1eb720d1fdea5b67d40f465956401c39a6653f08862`.
Machine/runtime, CPU baseline, upstream revisions and profiling method match the baseline
method below. No builds ran alongside measured or profiled forks.

## Native RocksDB ownership defect

Reading the live 10M native fork's database OPTIONS/LOG files showed four native caches of
14,913,080 bytes and four Java caches of 111,848,106 bytes. The configured Flink weights were
70/70/30. Flink's own run had four 223,696,213-byte caches; it did not declare the additional
native operator consumer. Cache totals are not additive live usage: write buffers charge their
shared cache, and capacities describe allowances.

The backend's former `streamfusion-` transformation-name prefix did not match Flink's actual
runtime-class-based operator identifier. Consequently it created the Java RocksDB delegate and
the native database took a smaller fallback reservation from `OPERATOR`. This is an ownership
bug, not evidence that RocksDB needs different algorithms, larger deployment budgets or a private
patch. The [native state contract](/StreamFusion/development/native-state/) now uses explicit
task-local registration before backend creation. The state-backend lease is assigned to the
native owner, while Flink's keyed lifecycle uses its intended heap shell.

Live audit files are retained in `747b033a/live-options/` alongside the campaign. Earlier RocksDB
results remain historical measurements of the executed path; they do not establish performance
with the intended cache ownership. Corrected release measurements remain pending.

## Acceleration and output evidence

All successful StreamFusion forks require whole-plan acceleration and nonzero native batches;
Flink forks require zero native activity. Every completed pair has identical blackhole counts.

| Backend | Events | Kind | Output records per completed engine | StreamFusion native plan / Calc batches |
| --- | ---: | --- | ---: | --- |
| hashmap | 1,000,000 | three measured forks | 920,000 | 416 / 144, 416 / 144, 416 / 144 |
| rocksdb | 1,000,000 | three measured forks | 920,000 | 447 / 156, 403 / 138, 426 / 152 |
| rocksdb | 2,000,000 | profile only | 1,840,000 | 927 / 328 |
| rocksdb | 10,000,000 | three measured forks | 9,200,000 | 3724 / 1252, 3739 / 1254, 3775 / 1272 |
| rocksdb | 20,000,000 | profile only | 18,400,000 | 7460 / 2514 |
| hashmap | 1,250,000 | profile only | 1,150,000 | 488 / 168 |

## Separate mixed JVM/native profiles

Each completed profile pair retains JFR, per-engine CPU flame graphs and collapsed stacks,
allocation stacks and a differential flame graph. Shares below use all process CPU samples.
Categories are inclusive and overlap: JNI includes downstream computation. Do not sum them
or interpret inclusive JNI as exclusive transport overhead. Zero means no matching sample.

| Sample category | rocksdb 2,000,000 F / SF | rocksdb 20,000,000 F / SF | hashmap 1,250,000 F / SF |
| --- | ---: | ---: | ---: |
| memory budget callbacks | 0.000 / 0.279 | 0.000 / 0.125 | 0.000 / 0.145 |
| garbage collection | 2.760 / 2.454 | 0.733 / 0.299 | 6.406 / 2.491 |
| row copy | 7.882 / 4.748 | 8.200 / 2.229 | 14.467 / 4.809 |
| rowdata to arrow write | 0.000 / 1.237 | 0.000 / 0.530 | 0.000 / 0.666 |
| arrow c data jni inclusive | 0.000 / 44.883 | 0.000 / 87.351 | 0.000 / 31.576 |
| native plan lowering | 0.000 / 0.000 | 0.000 / 0.000 | 0.000 / 0.029 |
| DataFusion frames, inclusive | 0.000 / 43.946 | 0.000 / 16.568 | 0.000 / 30.185 |
| DataFusion functions and expressions | 0.000 / 2.513 | 0.000 / 1.611 | 0.000 / 3.476 |
| scalar to array conversion | 0.000 / 0.938 | 0.000 / 0.547 | 0.000 / 1.043 |
| native group aggregate | 0.000 / 42.689 | 0.000 / 15.757 | 0.000 / 31.460 |
| native distinct membership | 0.000 / 9.635 | 0.000 / 6.162 | 0.000 / 11.674 |
| native membership state | 0.000 / 18.352 | 0.000 / 4.120 | 0.000 / 6.663 |
| native state serialization | 0.000 / 0.918 | 0.000 / 0.795 | 0.000 / 1.419 |
| arrow gather | 0.000 / 0.259 | 0.000 / 0.114 | 0.000 / 0.058 |
| arrow row view access | 0.000 / 1.835 | 0.000 / 0.937 | 0.000 / 2.057 |
| source poll inclusive | 15.545 / 13.505 | 15.785 / 7.258 | 26.364 / 12.659 |
| rocksdb inclusive | 37.582 / 24.875 | 66.200 / 73.734 | 0.000 / 0.000 |
| native artifact loading | 0.000 / 0.898 | 0.000 / 0.079 | 0.000 / 1.159 |
| jit compile | 21.124 / 22.861 | 2.777 / 2.044 | 33.861 / 29.606 |

Sample counts (Flink / StreamFusion), in table order: 5037 / 5013; 44731 / 72979; 2841 / 3452.

The initial 2M RocksDB profile has unresolved frames inside the separately loaded state plugin.
Async-profiler intercepts JVM library loading, while Rust loads that component through `dlopen`.
For the 20M profile only, a launcher calls JVM `System.load` on the same verified, cached plugin
path before entering the benchmark main method. This exposes its symbols to the profiler without
changing native state ownership or algorithms. Measured forks do not use this launcher. The
profiling-only launcher and its metadata are retained with the campaign.

The corrected 20M profile records RocksDB in 73.734% of StreamFusion CPU samples, versus
66.200% for Flink. StreamFusion's index-reader path appears in 36,229 of 72,979 samples
(49.643%), with block decompression in 37,098 samples (50.834%). These paths overlap. Snappy's
branchless decompressor alone is the leaf in 21,871 samples. This identifies index-block reads
and decompression as a stronger optimization target than scalar conversion; cache behavior and
state-key layout need investigation before choosing a change. It does not justify changing
Flink's cache settings, adding a budget or modifying RocksDB/Snappy.

The broad DataFusion category includes stream wrappers around custom state work. The narrower
function/expression category is 1.611% in the long profile. Scalar-to-array conversion accounts
for 0.547% there and 1.043% in the successful memory profile. Directly retaining evaluated scalars
can remove unnecessary arrays and string copies, but those sampled shares do not promise a
large overall gain. Short profiles contain 21–34% JIT samples. Symbol visibility and retained
stack frames also limit comparisons between the initial and corrected profiles. Inclusive
categories are not a partition of total execution time.

Large in-memory retained state remains a separate limitation. A completed RocksDB run or
checkpoint at 20M is not proof of restore capacity at that size; the supported recovery contracts
and portable savepoint limits remain documented in [native state](/StreamFusion/development/native-state/).

## Method and artifacts

The workload is original Nexmark Q16 with the deterministic RowData source adapter and unmodified
Flink blackhole sink. No Kafka interaction occurs. Both engines use parallelism 4, mini-batching
disabled, one-second exactly-once checkpoints, no restarts, UTC, 1 GiB managed memory and consumer
weights `OPERATOR:90,STATE_BACKEND:10,PYTHON:30`. JVM flags are
`-Xms1g -Xmx1g -XX:MaxDirectMemorySize=2g -XX:ActiveProcessorCount=4`; no CPU affinity is applied.
RocksDB uses Flink's default full/private-file checkpoint strategy. No builds ran alongside forks.

The timer includes Java-only counter reset, setup, EXPLAIN preflight, native initialization when
selected, cluster startup, execution and cleanup. JVM launch, argument parsing and build time are
excluded. The host is WSL2 Linux on an Intel Core i7-12650H, 16 logical CPUs, approximately 7.6 GiB
RAM and 2 GiB swap, with Java 24.0.2. Native binaries use release optimization, native CPU features,
frame pointers and profiling symbols without reducing optimization. The CPU baseline fingerprint
is `44dd0ad765af32a3`.

Profiles use async-profiler 4.5, 10 ms CPU sampling, Java non-safepoint sampling, native DWARF
unwinding, JFR output and 2 MiB allocation sampling. Profile timings are not benchmark results.

Artifact SHA-256 values verified against the benchmark JARs:

- `streamfusion-native`: `e9a6c078c9682ff2661277ffd2845880b95768cd1858e277706cea74fa4e62f4`.
- `streamfusion-state-rocksdb`: `fe1af76cd4e48dc789eca1eb720d1fdea5b67d40f465956401c39a6653f08862`.

Upstream reference revisions:

- `flink-2.3.0`: `c0f8d1a1e09f209885a88f9c19ceb9d9e9870283`.
- `nexmark`: `6b3646c3baec701f1fa74baf938d235f742e5d3c`.

Flink carries only the approved planner/class-loading installation and final StreamGraph
managed-memory callback; its benchmark-side operators and algorithms are unchanged. Nexmark is
clean. Captured upstream diffs and machine/runtime metadata accompany each case.

Run logs, failures, metadata, medians/dispersion, counters and profiles are retained locally under
`streamfusion-nexmark-benchmarks/target/measurements/q16/1f3cafbe/`. The parent `q16/` directory
contains the runner and CPU-category scripts. `campaign.json` records successful and failed cases.
Historical results remain in [query checkpoints](/StreamFusion/benchmarks/query-checkpoints/).

---
title: Q15 RowData release comparison
description: Append-only DISTINCT presence state, measured throughput and remaining capacity limits.
---

Original Q15 uses ordinary whole-plan acceleration on both backends. At release commit
`10371175af14d422cb4af38334495291ebda5d9e`, StreamFusion reaches **1.552× Flink's median throughput
on RocksDB at ten million events**. At one million, the ratio is 1.192× on RocksDB and 0.729×
in memory. The ten-million-event in-memory attempt still fails during hash-table growth.

This campaign does not establish an incremental throughput gain over the previous StreamFusion
implementation. At `54a5e5c0`, the RocksDB ten-million-event median was 23.870s, range
23.556–27.643s. The current 25.798s median, range 22.788–26.114s, overlaps that result.
The comparison against Flink and the verified storage savings are separate claims. Q15 has not
reached a demonstrated performance or capacity ceiling.

## Implementation and validation

New append-only COUNT DISTINCT groups store presence bits for shared FILTERs, following Flink's
append-only DISTINCT data view. Existing counted groups retain signed multiplicities. DataFusion
COUNT kernels still compute each observable transition, and state access remains batched around
Arrow computation. Duplicate-only input updates the group header without rewriting unchanged
members. This is selected by the planner's changelog contract, with no query-text matching,
new runtime setting or upstream fork. [Group aggregation](/StreamFusion/operators/group-aggregation/)
documents the versioned formats and rejection of presence-state restore into a retractable plan.

A native test verifies 15 fewer persisted bytes per member with two shared FILTERs and no member
writes for a 128-row duplicate batch. Validation passed 105 aggregate Rust tests, three paged-import
and poisoning tests, and four state-ABI snapshot tests. The 82 focused Java/integration checks
cover Flink-generated complete changelog and metric parity, canonical/backend-switch restore,
full/incremental aligned/unaligned checkpoints, actual channel replay, 1-to-2-to-1 rescaling,
retractable-state regressions, topology guards and Q15 ordinary admission/production checks.
Production collecting-sink integration uses 50,000 events, parallelism one/four and both backends.
Benchmark sink counters supplement those byte-parity tests; they do not replace them.

## Unprofiled measurements

Each successful case uses three separate-JVM pairs, alternating engine order F/SF, SF/F, F/SF.
Times are end-to-end seconds; MAD is median absolute deviation. Throughput ratio is Flink's
median time divided by StreamFusion's median time.

| Backend | Events | Engine | Median (s) | Range (s) | MAD (s) | Throughput ratio |
| --- | ---: | --- | ---: | --- | ---: | ---: |
| hashmap | 1,000,000 | flink | 5.645 | 5.501–7.043 | 0.145 | — |
| hashmap | 1,000,000 | streamfusion | 7.745 | 7.347–8.109 | 0.364 | 0.729× |
| rocksdb | 1,000,000 | flink | 8.248 | 8.137–10.045 | 0.112 | — |
| rocksdb | 1,000,000 | streamfusion | 6.918 | 6.894–7.018 | 0.024 | 1.192× |
| rocksdb | 10,000,000 | flink | 40.035 | 39.573–41.813 | 0.462 | — |
| rocksdb | 10,000,000 | streamfusion | 25.798 | 22.788–26.114 | 0.316 | 1.552× |

All three in-memory one-million-event pairs favor Flink. All three RocksDB pairs at each size
favor StreamFusion. Engine timing ranges do not overlap within any of these three cases.

The ten-million-event in-memory attempt completed its first Flink fork in 13.107334s, then
StreamFusion failed during table-growth admission: another **34,734,199 bytes** were denied,
with **47,547,602 bytes** already reserved and **22,112,691 bytes** available to that consumer.
The earlier `9fc0c19c` attempt failed with 61,712,205 bytes retained and 7,967,496 available.
Smaller values reduce retention at this observed boundary but do not accommodate the next table
allocation. The failed log and preceding Flink result are retained. No median, throughput ratio
or twenty-million-event in-memory profile is claimed for that failed case. Bounding table-growth
workspace remains the next demonstrated capacity problem under Flink's unchanged budget.

## Acceleration and output evidence

Every successful StreamFusion fork reports whole-plan acceleration and positive native plan/Calc
batch counters; every Flink fork reports zero native activity. Completed engines emit 920,000,
1,840,000, 9,200,000 or 18,400,000 records at the corresponding 1M, 2M, 10M or 20M input size.

| Backend | Events | Run kind | StreamFusion native plan / Calc batch counters |
| --- | ---: | --- | --- |
| hashmap | 1,000,000 | three measured forks | 212 / 144, 200 / 136, 212 / 144 |
| rocksdb | 1,000,000 | three measured forks | 220 / 148, 220 / 148, 208 / 140 |
| rocksdb | 10,000,000 | three measured forks | 1921 / 1282, 1898 / 1268, 1890 / 1263 |
| hashmap | 2,000,000 | profile only | 404 / 272 |
| rocksdb | 2,000,000 | profile only | 388 / 260 |
| rocksdb | 20,000,000 | profile only | 3898 / 2602 |

## Separate mixed JVM/native profiles

Two-million-event profiles cover both engines and backends. A separate twenty-million-event
RocksDB pair completes with checkpoints enabled. Profile durations are excluded from throughput
results. Each pair retains JFR, per-engine CPU flame graphs and collapsed stacks, allocation
stacks and a differential flame graph.

Percentages below are inclusive shares of **all process CPU samples**. Categories nest and
overlap: JNI includes downstream native execution. Do not sum them or interpret inclusive JNI
as exclusive transport overhead. Zero means no matching sample, not proof of zero cost.

| Sample category | Memory 2M F / SF | RocksDB 2M F / SF | RocksDB 20M F / SF |
| --- | ---: | ---: | ---: |
| Row copying | 16.822 / 6.386 | 10.745 / 5.851 | 19.524 / 13.181 |
| RowData → Arrow writes | 0.000 / 2.043 | 0.000 / 2.365 | 0.000 / 3.615 |
| Arrow C Data / JNI, inclusive | 0.000 / 13.367 | 0.000 / 14.274 | 0.000 / 33.788 |
| Native plan lowering | 0.000 / 0.000 | 0.000 / 0.041 | 0.000 / 0.010 |
| DataFusion execution | 0.000 / 12.218 | 0.000 / 11.286 | 0.000 / 28.367 |
| Native aggregate path | 0.000 / 10.728 | 0.000 / 9.668 | 0.000 / 24.811 |
| DISTINCT row/semantic adapters | 0.000 / 5.960 | 0.000 / 5.228 | 0.000 / 12.356 |
| Membership state path | 0.000 / 1.703 | 0.000 / 1.618 | 0.000 / 4.224 |
| Aggregate/member encoding | 0.000 / 0.043 | 0.000 / 0.041 | 0.000 / 0.059 |
| Arrow gather | 0.000 / 0.170 | 0.000 / 0.207 | 0.000 / 0.206 |
| Arrow-backed output access | 0.000 / 2.427 | 0.000 / 2.075 | 0.000 / 5.058 |
| Source polling, inclusive | 32.755 / 23.585 | 21.116 / 22.365 | 37.572 / 43.453 |
| RocksDB, inclusive | 0.000 / 0.000 | 17.372 / 2.905 | 32.014 / 5.756 |
| Memory-budget callbacks | 0.000 / 0.128 | 0.000 / 0.041 | 0.000 / 0.147 |
| JIT compilation | 34.490 / 35.292 | 31.561 / 35.062 | 7.383 / 11.531 |
| Garbage collection | 4.717 / 3.661 | 3.407 / 3.693 | 1.491 / 1.611 |

Sample counts (Flink / StreamFusion): 2247 / 2349; 2671 / 2410; 13747 / 10181, in table order.

The short profiles contain roughly one-third JIT samples, limiting conclusions about sustained
execution. In the longer StreamFusion profile, source polling is 43.453%, DataFusion execution
28.367% and the native aggregate path 24.811%, with overlapping categories. Among its leaf
samples are `Math.floorMod` (536), string-builder growth (516), deterministic source text
creation (283), source conversion (264), Rust vector construction (253), aggregate-value
comparison (234) and `RowKernels::apply` (158). Source and aggregate work remain material.

Member encoding (0.059%) and memory callbacks (0.147%) are small sampled shares. These results
do not justify per-allocation tracking or new memory budgets. Successful checkpointing does not
establish restore capacity at this size; the separately tested paged-restore bounds are documented
in [native state](/StreamFusion/development/native-state/).

## Method and artifacts

The workload is original Nexmark Q15 with the deterministic RowData source adapter and unmodified
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

- `streamfusion-native`: `be36298a33d125b414c8709a7e6a7c424f1d64e174166821a76697781297a3e4`.
- `streamfusion-state-rocksdb`: `fe1af76cd4e48dc789eca1eb720d1fdea5b67d40f465956401c39a6653f08862`.

Upstream reference revisions:

- `flink-2.3.0`: `c0f8d1a1e09f209885a88f9c19ceb9d9e9870283`.
- `nexmark`: `6b3646c3baec701f1fa74baf938d235f742e5d3c`.

Flink carries only the approved planner/class-loading installation and final StreamGraph
managed-memory callback; its benchmark-side operators and algorithms are unchanged. Nexmark is
clean. Captured upstream diffs and machine/runtime metadata accompany each case.

Run logs, failures, metadata, medians/dispersion, counters and profiles are retained locally under
`streamfusion-nexmark-benchmarks/target/measurements/q15/10371175/`. The parent `q15/` directory
contains the runner and CPU-category scripts. `campaign.json` records successful and failed cases.
Historical results remain in [query checkpoints](/StreamFusion/benchmarks/query-checkpoints/).

---
title: Q15 RowData release comparison
description: Bounded memory-state table growth, measured throughput and retained-state limits.
---

Original Q15 uses ordinary whole-plan acceleration on both backends. At release commit
`3f77da616840ad9532ca003b4df726f57813d870`, StreamFusion reaches **1.561× Flink's median throughput
on RocksDB at ten million events**. At one million, the ratio is 1.189× on RocksDB and 0.729×
in memory. The ten-million-event in-memory attempt still fails, now at retained-state capacity
after passing the former large hash-table replacement boundary.

The latest change divides the point-state memory backend's directory into independently growing
tables within each original Flink key group. It leaves RocksDB's storage path unchanged. These
RocksDB comparisons against Flink are not evidence of an incremental gain from the memory-directory
change. Q15 has not reached a demonstrated performance or capacity ceiling.

## Implementation and validation

Append-only COUNT DISTINCT groups use the presence-state implementation introduced at
`10371175`: one presence bit per shared FILTER for new groups, exact signed counts for legacy
groups, unchanged DataFusion COUNT computation and batched state access. Duplicate-only batches
update the group header without rewriting unchanged membership. A native test verifies 15 fewer
persisted bytes per member with two shared FILTERs and no member writes for a 128-row duplicate
batch. [Group aggregation](/StreamFusion/operators/group-aggregation/) documents the formats and
rejection of presence-state restore into retractable plans.

At `3f77da61`, each populated memory key group has 64 independently growing hash tables. Admission
covers final growth and the largest replacement overlap before applying a batch's mutations.
A new regression covers hashbrown's larger replacement after tombstone deletions; the former
live-entry estimate under-admitted that case. The change preserves canonical state bytes, original
Flink ownership and existing managed-memory settings. The internal table count is not a deployment
option. [Native state](/StreamFusion/development/native-state/) describes the storage contract.

A capacity test stores and reads every one of 240,000 entries in a hot key group under a 20 MiB
budget that the former single-table replacement would exceed. Mixed update/delete/insert denial
leaves logical state unchanged. Validation passed 156 distinct native checks: 44 storage tests,
seven shared-state conformance tests and 105 aggregate tests. The 82 focused Java/integration
checks cover Flink-generated changelog and metrics, canonical/backend-switch restore,
full/incremental aligned/unaligned checkpoints, channel replay, 1-to-2-to-1 rescaling,
retractable-state regressions, topology guards and Q15 ordinary admission/production checks.
Collecting-sink integration uses 50,000 events, parallelism one/four and both backends.
Benchmark sink counts supplement those byte-parity tests; they do not replace them.

## Unprofiled measurements

Each successful case uses three separate-JVM pairs, alternating engine order F/SF, SF/F, F/SF.
Times are end-to-end seconds; MAD is median absolute deviation. Throughput ratio is Flink's
median time divided by StreamFusion's median time.

| Backend | Events | Engine | Median (s) | Range (s) | MAD (s) | Throughput ratio |
| --- | ---: | --- | ---: | --- | ---: | ---: |
| hashmap | 1,000,000 | flink | 5.685 | 5.589–5.773 | 0.087 | — |
| hashmap | 1,000,000 | streamfusion | 7.799 | 7.002–7.825 | 0.025 | 0.729× |
| rocksdb | 1,000,000 | flink | 8.468 | 8.343–9.489 | 0.125 | — |
| rocksdb | 1,000,000 | streamfusion | 7.122 | 7.109–7.912 | 0.013 | 1.189× |
| rocksdb | 10,000,000 | flink | 39.335 | 39.126–46.779 | 0.209 | — |
| rocksdb | 10,000,000 | streamfusion | 25.200 | 23.115–28.341 | 2.085 | 1.561× |

All three in-memory one-million-event pairs favor Flink. All three RocksDB pairs at each size
favor StreamFusion. Engine ranges do not overlap within any of these three cases.

The earlier presence-state campaign at `10371175` measured 5.645s versus 7.745s in memory at one
million events (0.729×), 8.248s versus 6.918s on RocksDB at one million (1.192×), and 40.035s versus
25.798s on RocksDB at ten million (1.552×). Current and preceding StreamFusion timing ranges
overlap. No incremental throughput improvement is established by this directory change.
The earlier full-checkpoint campaign at `54a5e5c0` likewise had an overlapping RocksDB ten-million
range of 23.556–27.643s. All earlier artifacts remain available.

## Remaining in-memory capacity limit

The ten-million-event in-memory campaign completed one Flink fork, then StreamFusion failed with
another **129,591 bytes** requested, **68,893,748 bytes** already reserved by the state consumer,
and **112,721 bytes** available. This is a different boundary from `10371175`, which failed on
a **34,734,199-byte** table replacement with **47,547,602 bytes** reserved and **22,112,691 bytes**
available. Independent tables remove that earlier large replacement peak, but stored state still
fills the unchanged allowance. Reducing the retained key/value footprint is the next capacity
problem; raising the budget is not the implemented remedy.

The failed log and preceding Flink result are retained. No successful ten-million-event in-memory
median, throughput ratio or twenty-million-event profile is claimed. The bounded directory test
proves its stated storage case, not successful execution or recovery at arbitrary Nexmark sizes.

## Acceleration and output evidence

Every successful StreamFusion fork reports whole-plan acceleration and positive native plan/Calc
batch counters; every Flink fork reports zero native activity. Completed engines emit 920,000,
1,840,000, 9,200,000 or 18,400,000 records at the respective 1M, 2M, 10M or 20M input size.

| Backend | Events | Run kind | StreamFusion native plan / Calc batch counters |
| --- | ---: | --- | --- |
| hashmap | 1,000,000 | three measured forks | 200 / 136, 214 / 146, 215 / 146 |
| rocksdb | 1,000,000 | three measured forks | 220 / 148, 220 / 148, 211 / 142 |
| rocksdb | 10,000,000 | three measured forks | 1937 / 1294, 1942 / 1298, 1897 / 1266 |
| hashmap | 2,000,000 | profile only | 404 / 272 |
| rocksdb | 2,000,000 | profile only | 388 / 260 |
| rocksdb | 20,000,000 | profile only | 3805 / 2538 |

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
| Row copying | 15.873 / 6.066 | 10.952 / 6.518 | 21.192 / 13.278 |
| RowData → Arrow writes | 0.000 / 1.393 | 0.000 / 2.186 | 0.000 / 3.334 |
| Arrow C Data / JNI, inclusive | 0.000 / 13.811 | 0.000 / 14.494 | 0.000 / 34.349 |
| Native plan lowering | 0.000 / 0.082 | 0.000 / 0.040 | 0.000 / 0.000 |
| DataFusion execution | 0.000 / 12.828 | 0.000 / 11.619 | 0.000 / 28.401 |
| Native aggregate path | 0.000 / 10.656 | 0.000 / 10.040 | 0.000 / 24.573 |
| DISTINCT row/semantic adapters | 0.000 / 5.410 | 0.000 / 5.304 | 0.000 / 12.341 |
| Membership state path | 0.000 / 1.680 | 0.000 / 1.336 | 0.000 / 3.867 |
| Aggregate/member encoding | 0.000 / 0.041 | 0.000 / 0.040 | 0.000 / 0.079 |
| Arrow gather | 0.000 / 0.082 | 0.000 / 0.121 | 0.000 / 0.247 |
| Arrow-backed output access | 0.000 / 2.090 | 0.000 / 2.227 | 0.000 / 4.725 |
| Source polling, inclusive | 32.390 / 21.762 | 20.911 / 22.024 | 38.436 / 44.540 |
| RocksDB, inclusive | 0.000 / 0.000 | 16.759 / 2.753 | 30.904 / 6.274 |
| Memory-budget callbacks | 0.000 / 0.164 | 0.000 / 0.000 | 0.000 / 0.148 |
| JIT compilation | 35.521 / 37.008 | 31.716 / 35.385 | 7.122 / 11.404 |
| Garbage collection | 4.590 / 3.607 | 3.675 / 3.644 | 1.532 / 1.480 |

Sample counts (Flink / StreamFusion): 2331 / 2440; 2721 / 2470; 14364 / 10137, in table order.

The short profiles contain roughly one-third JIT samples, limiting conclusions about sustained
execution. In the longer StreamFusion profile, source polling is 44.540%, DataFusion execution
28.401% and the native aggregate path 24.573%, with overlapping categories. Leaf samples include
`Math.floorMod` (531), string-builder growth (499), deterministic source text creation (326),
Rust vector construction (280), source conversion (254) and aggregate-value comparison (245).
Source and aggregate work remain material; the directory change does not establish a compute ceiling.

Member encoding (0.079%) and memory callbacks (0.148%) are small sampled shares. These results
do not justify per-allocation tracking or new budgets. Successful checkpointing does not establish
restore capacity at this size; separately tested paged-restore bounds remain documented in
[native state](/StreamFusion/development/native-state/).

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

- `streamfusion-native`: `5c2204e863cd610eab00dd5fe45f9650f38d72df1213db01659472889922e3e8`.
- `streamfusion-state-rocksdb`: `fe1af76cd4e48dc789eca1eb720d1fdea5b67d40f465956401c39a6653f08862`.

Upstream reference revisions:

- `flink-2.3.0`: `c0f8d1a1e09f209885a88f9c19ceb9d9e9870283`.
- `nexmark`: `6b3646c3baec701f1fa74baf938d235f742e5d3c`.

Flink carries only the approved planner/class-loading installation and final StreamGraph
managed-memory callback; its benchmark-side operators and algorithms are unchanged. Nexmark is
clean. Captured upstream diffs and machine/runtime metadata accompany each case.

Run logs, failures, metadata, medians/dispersion, counters and profiles are retained locally under
`streamfusion-nexmark-benchmarks/target/measurements/q15/3f77da61/`. The parent `q15/` directory
contains the runner and CPU-category scripts. `campaign.json` records successful and failed cases.
Historical results remain in [query checkpoints](/StreamFusion/benchmarks/query-checkpoints/).

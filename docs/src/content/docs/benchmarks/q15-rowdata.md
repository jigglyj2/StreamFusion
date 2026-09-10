---
title: Q15 RowData release comparison
description: Packed memory state, measured throughput and remaining retained-state limits.
---

Original Q15 uses ordinary whole-plan acceleration on both backends. At release commit
`158ff8b357c9033bfe9ddcb3d4dd702fbff1eb67`, StreamFusion reaches **1.748× Flink's median throughput
on RocksDB at ten million events**. At one million, the ratio is 1.221× on RocksDB and 0.676×
in memory. The ten-million-event in-memory attempt still exceeds its retained-state allowance.

The latest change packs each point-state key and value into one retained allocation, reducing
hash-table descriptors from 32 to 24 bytes per bucket on 64-bit hosts. It leaves RocksDB's storage
path unchanged. These RocksDB comparisons against Flink are not evidence of an incremental gain
from the memory-backend change. The storage-capacity tests demonstrate savings; this campaign
does not demonstrate an incremental throughput gain or a completed large in-memory run.

## Implementation and validation

Append-only COUNT DISTINCT groups retain the presence-state implementation introduced at
`10371175`: one presence bit per shared FILTER for new groups, exact signed counts for legacy
groups, unchanged DataFusion COUNT computation and batched state access. Duplicate-only batches
update the group header without rewriting unchanged membership. A native test verifies 15 fewer
persisted bytes per member with two shared FILTERs and no member writes for a 128-row duplicate
batch. [Group aggregation](/StreamFusion/operators/group-aggregation/) documents the formats and
rejection of presence-state restore into retractable plans.

Each populated memory key group retains the 64 independently growing tables introduced at
`3f77da61`. Packed entries now share one key/value buffer. Hashing and equality inspect only the
key; equal-length updates reuse the allocation, and variable-length updates replace it. Batch
admission covers retained growth and the largest table or payload replacement overlap before
changing logical state. Snapshots sort one reference per entry and emit unchanged canonical
key/value bytes. Flink's original key groups, partition hash, managed-memory settings and Arrow
member keys remain unchanged. There is no new deployment option or per-allocation JNI accounting.
[Native state](/StreamFusion/development/native-state/) describes the storage contract.

A capacity test stores and verifies 120,000 entries with 56-byte keys and ten-byte values within
15 MiB; the equivalent unpacked directory would exceed that share. The earlier 240,000-entry,
20 MiB table-growth test still passes. New fixtures cover identical concatenated bytes with
different key/value boundaries, prefix scans, canonical restore and variable-length replacement
denial. Equal-length updates remain admissible without allocating a second packed payload.

Validation passed 159 distinct native checks: 47 storage tests, seven shared-state conformance
tests and 105 aggregate tests. The 82 focused Java/integration checks cover Flink-generated
changelog and metrics, canonical/backend-switch restore, full/incremental aligned/unaligned
checkpoints, channel replay, 1-to-2-to-1 rescaling, retractable-state regressions, topology guards
and Q15 ordinary admission/production checks. Collecting-sink integration uses 50,000 events,
parallelism one/four and both backends. Benchmark sink counts supplement those byte-parity tests;
they do not replace them.

## Unprofiled measurements

Each successful case uses three separate-JVM pairs, alternating engine order F/SF, SF/F, F/SF.
Times are end-to-end seconds; MAD is median absolute deviation. Throughput ratio is Flink's
median time divided by StreamFusion's median time.

| Backend | Events | Engine | Median (s) | Range (s) | MAD (s) | Throughput ratio |
| --- | ---: | --- | ---: | --- | ---: | ---: |
| hashmap | 1,000,000 | flink | 6.606 | 6.039–8.652 | 0.567 | — |
| hashmap | 1,000,000 | streamfusion | 9.771 | 9.223–11.504 | 0.548 | 0.676× |
| rocksdb | 1,000,000 | flink | 10.129 | 10.014–18.117 | 0.115 | — |
| rocksdb | 1,000,000 | streamfusion | 8.293 | 7.788–13.628 | 0.506 | 1.221× |
| rocksdb | 10,000,000 | flink | 42.620 | 42.235–47.301 | 0.385 | — |
| rocksdb | 10,000,000 | streamfusion | 24.377 | 23.871–25.069 | 0.506 | 1.748× |

All three in-memory pairs favor Flink, with disjoint engine ranges. All three RocksDB pairs at
each size favor StreamFusion. RocksDB ranges overlap at one million events and are disjoint at
ten million. The first one-million-event RocksDB pair is slower for both engines than the later
pairs; all forks remain in the results.

The preceding `3f77da61` campaign measured 5.685s versus 7.799s in memory at one million events
(0.729×), 8.468s versus 7.122s on RocksDB at one million (1.189×), and 39.335s versus 25.200s on
RocksDB at ten million (1.561×). Both engines take longer in the current one-million-event
in-memory campaign. This is not an alternating comparison between StreamFusion commits, so the
difference cannot be assigned solely to packing. Current and previous StreamFusion RocksDB
ranges overlap. No incremental throughput improvement is established. Earlier artifacts remain
available, including the presence-state and full-checkpoint campaigns.

## Remaining in-memory capacity limit

The ten-million-event in-memory campaign completed one Flink fork in 13.613057s, then StreamFusion
failed with another **120,575 bytes** requested, **68,907,600 bytes** already reserved by the state
consumer, and **100,253 bytes** available. Smaller descriptors do not bring this workload within
its unchanged allowance. The preceding `3f77da61` attempt failed with 129,591 bytes requested,
68,893,748 reserved and 112,721 available. These failures do not provide a completed-input count
from which to infer a larger successful capacity.

The failed log and preceding Flink result are retained. No successful ten-million-event in-memory
median, throughput ratio or twenty-million-event profile is claimed. The bounded storage tests
prove their stated cases, not execution or recovery at arbitrary Nexmark sizes. Repeated grouping
keys and other retained payloads remain possible storage improvements; Q15 has not reached a
proven capacity or compute ceiling.

## Acceleration and output evidence

Every successful StreamFusion fork reports whole-plan acceleration and positive native plan/Calc
batch counters; every Flink fork reports zero native activity. Completed engines emit 920,000,
1,840,000, 9,200,000 or 18,400,000 records at the respective 1M, 2M, 10M or 20M input size.

| Backend | Events | Run kind | StreamFusion native plan / Calc batch counters |
| --- | ---: | --- | --- |
| hashmap | 1,000,000 | three measured forks | 212 / 144, 212 / 144, 235 / 160 |
| rocksdb | 1,000,000 | three measured forks | 219 / 148, 220 / 148, 208 / 140 |
| rocksdb | 10,000,000 | three measured forks | 1900 / 1268, 1924 / 1284, 1962 / 1310 |
| hashmap | 2,000,000 | profile only | 404 / 272 |
| rocksdb | 2,000,000 | profile only | 420 / 282 |
| rocksdb | 20,000,000 | profile only | 3826 / 2552 |

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
| Row copying | 16.572 / 6.854 | 11.663 / 5.768 | 18.443 / 12.860 |
| RowData → Arrow writes | 0.000 / 1.632 | 0.000 / 2.055 | 0.000 / 3.630 |
| Arrow C Data / JNI, inclusive | 0.000 / 13.749 | 0.000 / 14.066 | 0.000 / 34.610 |
| Native plan lowering | 0.000 / 0.041 | 0.000 / 0.040 | 0.000 / 0.000 |
| DataFusion execution | 0.000 / 12.648 | 0.000 / 10.589 | 0.000 / 28.525 |
| Native aggregate path | 0.000 / 11.138 | 0.000 / 9.166 | 0.000 / 24.725 |
| DISTINCT row/semantic adapters | 0.000 / 5.345 | 0.000 / 4.544 | 0.000 / 11.865 |
| Membership state path | 0.000 / 2.734 | 0.000 / 2.015 | 0.000 / 5.165 |
| Aggregate/member encoding | 0.000 / 0.082 | 0.000 / 0.040 | 0.000 / 0.066 |
| Arrow gather | 0.000 / 0.122 | 0.000 / 0.119 | 0.000 / 0.351 |
| Arrow-backed output access | 0.000 / 2.448 | 0.000 / 2.371 | 0.000 / 4.587 |
| Source polling, inclusive | 32.536 / 22.317 | 22.735 / 21.849 | 36.598 / 44.115 |
| RocksDB, inclusive | 0.000 / 0.000 | 17.043 / 3.319 | 31.925 / 6.065 |
| Memory-budget callbacks | 0.000 / 0.082 | 0.000 / 0.119 | 0.000 / 0.133 |
| JIT compilation | 34.765 / 34.761 | 30.267 / 34.769 | 8.307 / 11.856 |
| Garbage collection | 4.335 / 3.550 | 3.783 / 3.793 | 1.689 / 1.516 |

Sample counts (Flink / StreamFusion): 2468 / 2451; 2881 / 2531; 14916 / 10552, in table order.

The short profiles contain roughly one-third JIT samples, limiting conclusions about sustained
execution. In the longer StreamFusion profile, source polling is 44.115%, DataFusion execution
28.525% and the native aggregate path 24.725%, with overlapping categories.
The two-million-event memory profile contains 22 samples with point-state memory frames out of
2,451 total samples. It does not establish the packed table as the leading CPU bottleneck.
Long-profile leaf samples include `Math.floorMod` (566), string-builder growth (490), deterministic
source text creation (321), Rust vector construction (273), source conversion (239), and
aggregate-value comparison (233). Source and aggregate work remain material.

Member encoding (0.066%) and memory callbacks (0.133%) are small sampled shares in the
longer profile. These results do not justify per-allocation tracking or new budgets. Successful
checkpointing does not establish restore capacity at this size; separately tested paged-restore
bounds remain documented in [native state](/StreamFusion/development/native-state/).

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

- `streamfusion-native`: `e9a6c078c9682ff2661277ffd2845880b95768cd1858e277706cea74fa4e62f4`.
- `streamfusion-state-rocksdb`: `fe1af76cd4e48dc789eca1eb720d1fdea5b67d40f465956401c39a6653f08862`.

Upstream reference revisions:

- `flink-2.3.0`: `c0f8d1a1e09f209885a88f9c19ceb9d9e9870283`.
- `nexmark`: `6b3646c3baec701f1fa74baf938d235f742e5d3c`.

Flink carries only the approved planner/class-loading installation and final StreamGraph
managed-memory callback; its benchmark-side operators and algorithms are unchanged. Nexmark is
clean. Captured upstream diffs and machine/runtime metadata accompany each case.

Run logs, failures, metadata, medians/dispersion, counters and profiles are retained locally under
`streamfusion-nexmark-benchmarks/target/measurements/q15/158ff8b3/`. The parent `q15/` directory
contains the runner and CPU-category scripts. `campaign.json` records successful and failed cases.
Historical results remain in [query checkpoints](/StreamFusion/benchmarks/query-checkpoints/).

---
title: Q12 processing-time RowData release comparison
description: DataFusion TUMBLE COUNT, borrowed timer keys, release measurements and mixed profiles.
---

Q12 accelerates through ordinary whole-plan selection using DataFusion COUNT inside the shared
processing-time TUMBLE runtime. At 50 million input events, median throughput is 1.303× Flink
in memory and 1.720× on RocksDB. Timing ranges are disjoint on both backends, but the third
RocksDB pair is nearly tied: 50.510 seconds for Flink and 49.651 for StreamFusion. The RocksDB
median ratio is specific to this variable measured set and is not a stable general 72% gain.

These are end-to-end RowData-to-unmodified-blackhole measurements, including setup and cleanup.
Each backend has three alternating fresh-JVM pairs, ordered Flink/StreamFusion,
StreamFusion/Flink, Flink/StreamFusion. Every fork is retained. Greater than one means higher
StreamFusion input throughput. MAD is median absolute deviation.

| Backend | Events | Flink seconds [range]; MAD | StreamFusion seconds [range]; MAD | Throughput ratio |
| --- | --- | --- | --- | --- |
| In-memory | 50,000,000 | 47.263 [46.517, 49.912]; 0.746 | 36.273 [34.881, 36.338]; 0.065 | 1.303× |
| RocksDB | 50,000,000 | 66.224 [50.510, 66.383]; 0.159 | 38.500 [37.829, 49.651]; 0.672 | 1.720× |

Median input rates are 1,057,912 / 1,378,440 events per second in memory and
755,015 / 1,298,689 on RocksDB, respectively Flink / StreamFusion.

## Processing-time output and comparison limits

The official SQL retains its ten-second processing-time window and max-speed bounded source.
The JVM/table time zone is UTC. Finishing input does not emit the final open processing-time window;
terminal event-time watermarks do not close it either. Independent jobs have different arrival
clocks, absolute window labels and final partial windows. Consequently, these rates compare the
same input workload but not an identical number of emitted windows or identical clock histories.
They do not isolate aggregate-kernel speed or establish independent-job changelog equality.

The original blackhole writer's standard `numRecordsIn` counters prove non-empty output in each
fork. The benchmark reporter retains those counters through job completion and adds no data-plane
operator or per-row callback. Output counts are logical records, not Arrow batches or IPC frames.

| Backend / pair | Flink seconds | StreamFusion seconds | Flink output records | StreamFusion output records |
| --- | --- | --- | --- | --- |
| Memory / 1 | 49.912437 | 34.881332 | 854,077 | 808,840 |
| Memory / 2 | 46.516929 | 36.272890 | 1,000,297 | 903,135 |
| Memory / 3 | 47.262919 | 36.337882 | 791,929 | 781,316 |
| RocksDB / 1 | 66.223882 | 37.828687 | 996,080 | 911,829 |
| RocksDB / 2 | 66.382527 | 38.500351 | 827,962 | 944,193 |
| RocksDB / 3 | 50.509929 | 49.651413 | 1,014,728 | 1,014,818 |

Ten-million-event one-pair diagnostics at `d8a671dd` produced sharply different output counts:
110,454 / 4,418 in memory and 18,064 / 184,153 on RocksDB, respectively Flink / StreamFusion.
They were not promoted to performance results. A subsequent 50-million-event memory diagnostic
emitted 760,121 / 754,714 records and informed the longer measured workload. These diagnostics
remain retained; they are not additional measured forks or before/after optimization evidence.

## Method, architecture and correctness

The clean measured checkout is `2b10136024c49e101f36b02cf17d0cdf6984d35b`. Timing includes Java-only
counter setup, table/EXPLAIN preparation, native initialization for StreamFusion, local cluster
startup, execution and cleanup. It excludes JVM launch and argument parsing. No Kafka is used.

Both engines use the upstream Nexmark generator through the same StreamFusion-owned bounded
RowData adapter. Its existing deterministic string/URL normalization removes upstream process-random
cache differences; that normalization also consumes CPU in both engines. The adapter is unchanged
by this optimization, and the upstream Nexmark dependency is not privately patched.

The WSL2 Linux host has an Intel Core i7-12650H, 16 reported logical CPUs, about 7.6 GiB RAM and
2 GiB swap. Both engines use OpenJDK 24.0.2+12-54, `-Xms1g -Xmx1g`, 2 GiB maximum direct memory,
`ActiveProcessorCount=4`, UTC and the Arrow-required `java.nio` opening. Flink settings are
parallelism four, 1 GiB managed memory, weights `OPERATOR:90,STATE_BACKEND:10,PYTHON:30`, disabled
mini-batching, one-second exactly-once filesystem checkpoints and no restart. Compilation does
not overlap measurement or profiling. This workload exercises state on both backends.

Rust 1.94 builds use release optimization, `target-cpu=native`, frame pointers and `debuginfo=1`,
with separate DWARF symbols and enforced packaged CPU metadata. Artifact SHA-256 values:

- Core: `9edfb160c30acd5a24887876267a14677179afa1cda2929ec207f6635eb4940a`.
- RocksDB: `118e0d6c10fb0ed24ef99d44e0bc8eaf7d3f81402554bf5b9848ba806cf2e85a`.

The approved Flink patches install the planner and finalize original managed-memory shares after
complete graph construction. Flink retains its allocation rules, planning/control responsibilities,
mailbox timers and checkpoint coordination. Recorded upstream revisions and working-tree patch
snapshots accompany the runs. Both engines use the same Nexmark RowData source and unmodified blackhole connector.

Ordinary admission supports direct UTC processing-time TUMBLE, one nullable or non-null BIGINT key,
unfiltered COUNT(*), start/end properties, synchronous state and disabled mini-batching. The
original PROCTIME Calc and exchange are preserved. Other zones, windows, calls/key types and
unverified clock placements retain precise whole-plan fallback. Native stages exchange shared
Arrow arrays; clock capture happens once per record at the external receiving edge using Flink's
clock. There is no private native clock or substitution of one timestamp per batch.

DataFusion grouped computation feeds the shared ordered slice store and DataFusion merger.
The native buffer preserves Flink's original paged capacity and pressure/checkpoint flush points.
Raw arrivals register absolute timers; publishing partials cannot recreate fired timers. State
reads/writes are batched and output is bounded to 1,024 timers per pull. Large buffers, retained
state and growing workspaces use Flink reservations. Sort keys use versioned Arrow row encodings;
Flink BinaryRow hashing independently determines key groups.

`NexmarkQ12ProductionIT` passes at 50 million events and parallelism four on both engines/backends.
It requires non-empty INSERT output, positive counts, unique bidder/window pairs, ten-second UTC
alignment and positive StreamFusion plan/Calc activity. These independent-clock integration checks
supplement exact parity tests; they do not substitute for them.

Generated controlled-clock tests compare complete changelog/control bytes and registered Flink
metrics, including nullable keys, varying batches, different window sizes, rollback/repeated timers,
COUNT zero, pressure flushing and terminal paths. Recovery covers backend switches, one-to-two-to-one
rescaling, incremental RocksDB SST reuse and actual aligned/unaligned Arrow channel replay into a
later processing-time window. Only Flink's unspecified order among independent keys at the same
timer deadline is canonicalized. Original buffer shares match Flink graphs with weighted boundaries
and operators added after SQL translation.

## General timer optimization

The first 100-million-event profiles at `ee51ddc9` attributed 8.94% / 8.11% of StreamFusion process
CPU to processing-time timer registration in memory / RocksDB. Repeated rows constructed owned
keys and probed the retained timer tree even when another row in the same batch had registered
the identical timer. Tree lookup, allocation and partition hashing were visible in those stacks.

The final implementation first deduplicates borrowed Arrow grouping keys plus absolute window
ends within the batch. It constructs owned timer keys, computes Flink partition hashes and probes
the retained tree only for distinct registrations. Every raw record still reaches DataFusion COUNT.
The temporary index uses the existing batch reservation and is discarded after registration, so
future arrivals can re-register a fired window. No query name, window duration, hot key or source
cardinality is special-cased; persisted encodings and memory allowances are unchanged.

The new generated native regression retains all 8,192 counts per input phase across nullable keys,
three batch sizes, multiple window deadlines and repeated labels after firing. Timer counters and
complete credit release are checked on both backends. After the change, 94 focused native tests and
20 Flink parity, pressure, topology and recovery tests pass against the release artifact.
The final official collecting integration also passes again on both backends with that artifact,
alongside six focused benchmark harness unit tests.

## Mixed JVM/native profiles

Separate 100-million-event forks use async-profiler 4.5, CPU sampling at 10 ms, Java non-safepoint
sampling, native DWARF unwinding and JFR output, plus Java allocation sampling at 2 MiB. Profile
timings are excluded from the measurement table. Per-engine flame graphs, CPU/allocation collapsed
stacks, JFR files and differential flame graphs are retained.

The following percentages use all process CPU samples. Categories are inclusive and overlap:
source polling includes chained downstream work, JNI includes native execution, and DataFusion
physical streams include state/control adapters as well as kernels. They must not be added or
read as isolated arithmetic costs. Zero samples do not establish zero cost.

| CPU category | Flink memory | StreamFusion memory | Flink RocksDB | StreamFusion RocksDB |
| --- | --- | --- | --- | --- |
| Source polling, inclusive | 76.62% | 79.83% | 69.20% | 74.01% |
| Row copying | 38.66% | 15.71% | 35.16% | 15.26% |
| RowData → Arrow writes | 0.00% | 11.24% | 0.00% | 10.03% |
| Arrow C Data / JNI, inclusive | 0.00% | 11.52% | 0.00% | 14.54% |
| Native plan lowering | 0.00% | 0.00% | 0.00% | 0.00% |
| DataFusion execution, inclusive | 0.00% | 8.07% | 0.00% | 8.17% |
| Native processing-time window, inclusive | 0.00% | 6.45% | 0.00% | 9.68% |
| Processing-time timer registration | 0.00% | 2.08% | 0.00% | 1.92% |
| DataFusion buffer adaptation | 0.00% | 1.71% | 0.00% | 1.77% |
| Shared slice input | 0.00% | 0.85% | 0.00% | 1.11% |
| Shared slice firing | 0.00% | 1.22% | 0.00% | 1.18% |
| Flink clock capture at Arrow input | 0.00% | 1.04% | 0.00% | 0.92% |
| DataFusion grouped-compute adaptation | 0.00% | 0.34% | 0.00% | 0.35% |
| Native timers | 0.00% | 1.51% | 0.00% | 1.40% |
| State writes | 0.00% | 0.34% | 6.98% | 1.37% |
| Flink processing-time window, inclusive | 8.46% | 0.00% | 21.53% | 0.00% |
| RocksDB, inclusive | 0.00% | 0.00% | 14.99% | 7.92% |
| Arrow-backed output access | 0.00% | 0.11% | 0.00% | 0.06% |
| Managed-budget callbacks | 0.00% | 0.41% | 0.00% | 0.38% |
| Garbage collection | 6.42% | 1.34% | 1.43% | 1.13% |
| Native artifact loading | 0.00% | 0.14% | 0.00% | 0.15% |
| JIT compilation | 3.08% | 5.28% | 2.84% | 4.68% |

CPU sample totals in table order are 41,593 / 29,787 / 49,062 / 31,804.
Corresponding Java allocation sample counts are 329,620 / 142,269 / 320,253 / 140,185. These are sampled
allocation events, not byte totals or a native heap profile.

Processing-time registration now accounts for 2.08% / 1.92% of StreamFusion CPU
in memory / RocksDB, versus 8.94% / 8.11% before deduplication. This supports the targeted reduction
in repeated registration work. Cross-run sample shares and the earlier one-pair diagnostics do
not quantify an isolated throughput gain from that change.

Source polling, event/string generation, RowData copying and Arrow writing remain substantial.
The remaining costs do not justify replacing Flink's per-record clock, changing the official
window duration, bypassing blackhole output, or specializing the source. Shared source/boundary
improvements remain possible; this is not a claim that the query has reached a performance ceiling.
Shared-library leaves do not always resolve to individual functions; only named parent stacks
are used to attribute those samples.

No unknown-stack markers appear in these final profiles. Unresolved shared-library leaves remain,
including 1,983 samples in Flink's RocksDB JNI library and 1,494 / 1,769 libc leaf samples in
StreamFusion's memory / RocksDB profiles. Their internal functions are not inferred. Named source
stacks include the harness's deterministic string generation, upstream event generation and the
RowData deserializer, consistent with the source/boundary costs above.

## Native activity, retained evidence and scope

Every measured and profiled StreamFusion fork reports ordinary acceleration and positive native
plan/Calc activity. Flink records zero native batches. These diagnostics remain separate from the
logical Flink metric surface. The following numbers are plan batches / Calc batches:

| Workload | In-memory StreamFusion | RocksDB StreamFusion |
| --- | --- | --- |
| 50M / measured range | 18859–18940 / 6212–6236 | 18946–19121 / 6236–6274 |
| 100M / separate profile | 37786 / 12442 | 37921 / 12472 |

Profile output counts are 1,917,412 / 1,875,929 in memory and 1,950,985 / 1,851,934 on RocksDB
(Flink / StreamFusion). All 100-million-event profiles complete without capacity failure.
This does not establish unlimited state capacity, non-UTC support or mini-batch compatibility.

Use the [RowData blackhole method](/StreamFusion/benchmarks/rowdata-blackhole/) with `q12`,
50,000,000 events, parallelism four and the settings above. Record UTC and emitted output counts;
repeat at least three alternating unprofiled pairs and profile longer forks separately.

All commands, classpaths, machine/runtime/native metadata, upstream revisions and patch snapshots,
EXPLAIN/native-activity logs, individual fork results, medians/ranges/MAD and profile artifacts are
under `streamfusion-nexmark-benchmarks/target/measurements/q12/`. Final directories are
`2b101360/{hashmap,rocksdb}/{50000000,profiles-100000000}`. Earlier profiles remain under
`ee51ddc9/{hashmap,rocksdb}/profiles-100000000`, and diagnostics under `d8a671dd`. The runner and
CPU-category analysis script are retained there. Generated profiles are not checked into Git.

---
title: Q7 default-optimizer release comparison
description: Bounded join staging, verified one-million-event results, and remaining RocksDB checkpoint costs.
---

At release code `f4886002654e94b24fb84b9c966f413966bd93e2`, original Q7 accelerates on both
backends with Flink's default `table.optimizer.multi-join.enabled=false`. All one-million-event
measured forks and separate collecting runs complete, as do longer 1.25-million-event profiles.
Regular-join state staging now processes zero-copy Arrow slices of at most 1,024 rows. This bounds
touched-key staging per state batch while preserving the parent invocation and result order.

RocksDB remains substantially slower than Flink. The in-memory comparison has wide variation and
does not establish a reliable gain. Two-million-event capacity remains unresolved. This is a
measured progress checkpoint, not a claim that Q7 has reached reasonable performance limits.
The [historical Q7 report](/StreamFusion/benchmarks/q7-rowdata/) uses smaller workloads and different
optimizer/memory settings; its results must not be combined with this comparison.

## Measurements, September 10, 2026

Each backend uses three fresh unprofiled JVMs per engine at one million input events, alternating
Flink/StreamFusion, StreamFusion/Flink, Flink/StreamFusion. MAD is median absolute deviation.
Timing is end-to-end: setup, EXPLAIN, native initialization, cluster startup, execution and cleanup.
It excludes JVM launch, argument parsing and artifact builds; runtime JIT compilation is included.
No profiled timing enters the following table.

| Backend | Flink seconds: median [range]; MAD | StreamFusion seconds: median [range]; MAD | StreamFusion/Flink throughput |
| --- | --- | --- | ---: |
| In-memory | 18.397 [6.726–24.078]; 5.681 | 8.741 [7.917–18.518]; 0.824 | 2.105× |
| RocksDB | 8.210 [8.132–8.603]; 0.078 | 23.835 [22.017–28.418]; 1.818 | 0.344× |

StreamFusion wins only one of three paired in-memory comparisons despite the 2.105× ratio of
medians. The overlapping ranges and slow Flink forks prevent a reliable speedup claim. On
RocksDB, StreamFusion loses all three pairs with disjoint ranges: median throughput is 65.6%
lower. These short local end-to-end measurements are not steady-state throughput guarantees.

All twelve measured jobs emit one blackhole record. Every StreamFusion EXPLAIN reports
acceleration and native activity is positive; Flink reports zero native activity. Native-plan /
Calc invocation counts are 555/152, 550/150 and 564/152 in memory, and 951/260, 971/268 and
937/256 on RocksDB. These batch diagnostics are separate from Flink's logical-record counters.

## Configuration and correctness

Both engines use the same deterministic official Nexmark RowData source, original Q7 SQL and
unmodified Flink blackhole sink, parallelism four, UTC, disabled mini-batching, one-second
exactly-once checkpoints, 1 GiB managed memory and consumer weights
`OPERATOR:70,STATE_BACKEND:70,PYTHON:30`. JVM flags include `-Xms1g -Xmx1g`,
`-XX:MaxDirectMemorySize=2g`, `-XX:ActiveProcessorCount=4` and the required `java.nio` opening.
No Kafka services or connector benchmarks are involved.

The host is an Intel Core i7-12650H under WSL2 Linux 6.18.33.2, with sixteen reported logical
CPUs, approximately 7.6 GiB RAM and 2 GiB swap. OpenJDK is 24.0.2+12-54. Rust 1.94 builds use
release optimization, native CPU features, frame pointers and separate DWARF profiling symbols
without reducing optimization. CPU baseline is `44dd0ad765af32a3`. Artifact SHA-256 values are:

- Core: `a31de8e2c33e9cf32c4f431c15fb544969f2bc8e694123fd64b9ce6313339612`.
- RocksDB: `fe1af76cd4e48dc789eca1eb720d1fdea5b67d40f465956401c39a6653f08862`.

The measured checkout is clean. Metadata records complete JVM flags, artifacts, classpath and
upstream revisions: Flink `c0f8d1a1e09f209885a88f9c19ceb9d9e9870283` and Nexmark
`6b3646c3baec701f1fa74baf938d235f742e5d3c`. Flink's local patches are the permitted planner
installation/class-loading and post-StreamGraph resource-finalization hooks. Source, sink and
Flink operator algorithms are unchanged. Builds do not overlap measurements.

The join-staging change passes 60 focused native regular-join tests and 24 focused Java tests
covering generated region parity, complete metric surfaces, selected topology, restore/rescaling
and aligned/unaligned channel recovery. Sixteen Q7/Q8 integration cases pass, eight per query,
covering both state backends, parallelism one/four and both optimizer settings. They compare
collecting-sink bytes and unmodified-blackhole counts, require ordinary admission and native
activity, and reject standalone local-window JNI execution. The full-join slicing fixture also
checks input-row origins, canonical state and cancellation/snapshot boundaries; it does not
admit previously unsupported outer-join SQL.

Separate fresh-JVM collecting runs at one million events produce one record on both engines and
both backends. Complete changelog, ordered output and materialized-result SHA-256 all equal
`e440ad7d261dd37d6c50696421a22f26b0073cfddf1ec5fbfe46ef553e3e6c92`.
Blackhole timing itself is not output-parity evidence. See the supported
[join](/StreamFusion/operators/joins/) and [window](/StreamFusion/operators/window-aggregation/) contracts.

## Complete longer profiles

Each engine/backend has a separate successful 1.25-million-event profile using async-profiler
4.5, CPU and wall sampling at 10 ms, Java non-safepoint sampling, native DWARF/frame-pointer
unwinding and JFR output. Allocation sampling uses a 2 MiB interval. The RocksDB profile-only
symbol preloader is recorded in metadata. Per-engine flame graphs, CPU/wall collapsed stacks
and differential CPU flame graphs are retained. All four jobs emit one record; native-plan /
Calc counts are 620/168 in memory and 1,205/332 on RocksDB.

The table gives inclusive percentages of all-process CPU samples as Flink / StreamFusion.
Denominators are 3,138 / 3,268 in memory and 3,574 / 4,229 on RocksDB. Categories overlap:
JNI and DataFusion frames include downstream native work, and the DataFusion category includes
StreamFusion execution-plan adaptations, not only library kernels. Unwinding can omit callers;
zero means no matching sample, not proof of zero cost.

| CPU category | In-memory F / SF | RocksDB F / SF |
| --- | ---: | ---: |
| Source polling | 21.447% / 13.953% | 17.571% / 11.279% |
| Row copying | 10.166% / 3.305% | 6.995% / 2.294% |
| RowData-to-Arrow writing | 0.000% / 1.010% | 0.000% / 0.780% |
| Arrow C Data / JNI | 0.000% / 29.284% | 0.000% / 42.256% |
| Native plan lowering | 0.000% / 0.031% | 0.000% / 0.000% |
| DataFusion execution | 0.000% / 27.876% | 0.000% / 19.839% |
| Arrow-backed output access | 0.000% / 0.000% | 0.000% / 0.000% |
| Native regular join | 0.000% / 27.570% | 0.000% / 19.343% |
| RocksDB | 0.000% / 0.000% | 26.861% / 25.065% |
| Memory-budget callbacks | 0.000% / 0.306% | 0.000% / 0.307% |
| JIT compilation | 35.437% / 31.059% | 29.323% / 25.845% |
| Garbage collection | 12.173% / 2.570% | 3.190% / 2.175% |

RocksDB's wall profile supplies a stronger performance lead than CPU shares alone. Flink has
508 wall samples under synchronous snapshotting, including 478 waiting for memtable flushes;
StreamFusion has 5,868 and 5,777 respectively. StreamFusion's waits pass through the native
checkpoint callback, upstream `Checkpoint::create_checkpoint`, `FlushMemTable`, and
`WaitForFlushMemTables` or `WaitUntilFlushWouldNotStallWrites`. These are aggregate thread
samples and must not be converted directly into job elapsed time. They establish substantial
checkpoint waiting, but do not yet identify why the native database flushes take longer.

Both implementations already disable WAL writes and synchronously create an upstream RocksDB
checkpoint at the snapshot boundary. StreamFusion does not perform a redundant explicit flush.
Skipping this flush or moving it beyond Flink's snapshot boundary would compromise correctness.
The next investigation must compare state volume and flush/compaction behavior while preserving
Flink's memory allocation and checkpoint semantics. Small reservation callbacks account for
about 0.3% of native-job CPU samples, so they are not the leading optimization target.

## Remaining capacity and performance work

The 1,024-row zero-copy slices bound transient touched-key staging without adding JNI crossings,
copying full batches or changing computation. Keys are loaded at each state-batch start and
dirty mutations are written together at its end. Repeated keys across slices can incur additional
batched state I/O. Retained state and the history of one hot key still must fit their allowances.
The earlier `d298de39` 1.25M in-memory profile failed admission; this release completes both
1.25M profile pairs. That is progress at a verified size, not unlimited capacity.

A two-million-event in-memory comparison did not complete. The first Flink fork suffered severe
heap/GC pressure and uncaught asynchronous-thread exceptions; diagnostics found hundreds of
full collections and millions of retained BinaryRowData objects. After diagnosis the failed job
was manually terminated before any StreamFusion fork started. It supplies no throughput ratio.
A separate native-only two-million-event capacity run also failed: join mutation admission
requested 1,363,296 additional bytes with 16,363,582 held and 1,130,384 available. Slicing has
not resolved that limit. Failed attempts remain recorded and are excluded from measured results.

Q7's RocksDB checkpoint cost is the next demonstrated blocker to reasonable performance.
Q8 default-path release measurements remain pending. Neither this report nor the earlier
admission audit establishes completion of the full Nexmark goal.

Raw results, complete metadata, collecting hashes and profiles remain under
`streamfusion-nexmark-benchmarks/target/measurements/q7-default/f4886002/`. The `hashmap-1m` and
`rocksdb-1m` directories hold unprofiled results; `*-1250k-profile` hold complete profiles;
`hashmap-2m` and `hashmap-2m-native-capacity` retain failures. Earlier `d298de39` results remain
separate. Focused validation logs are under `target/measurements/q6-q8-default/`.

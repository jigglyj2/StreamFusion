---
title: Q23 RowData release comparison
description: Bare-join envelope repair, composed-join parity, release measurements and memory limits.
---

Original Q23 passes ordinary whole-plan admission with Flink's default two-join topology at
`78414b06d67e43e0f25d157aad0874ef809f326e`. The SELECT joins bids, people and auctions;
the only upstream SQL adaptation quotes `A.dateTime`, a reserved identifier in Flink 2.3.
The 21-column blackhole schema is unchanged. The benchmark's historical preset enables
multi-join optimization, so both engines explicitly use the existing Flink option
`table.optimizer.multi-join.enabled=false`, which is Flink's production default.
Enabling it produces a genuine three-input `StreamExecMultiJoin` that still retains precise
whole-plan fallback. This checkpoint does not claim that optional implementation is admitted.

A non-empty Q23 run exposed a production bug: a regular join ending a native region emitted
input arrival ordinals without an owned record envelope. Its first output failed with
`Native output has an invalid global input ordinal: 0`. Earlier join→Calc tests hid the bug
because Calc supplied the missing timestamp policy. Streaming joins now declare their own
version-3 timestamp policy, matching Flink's `StreamingJoinOperator`, and plan composition
cannot downgrade it when older fragments are appended. The existing DataFusion projection
shares payload/RowKind buffers and reserves its two metadata vectors. The fix adds no Java
row reconstruction, upstream runtime patch, native algorithm or extra JNI crossing.

## Unprofiled release measurements

Each completed case uses three fresh-JVM pairs in F/SF, SF/F, F/SF order. Times are end-to-end
seconds; MAD is median absolute deviation. Ratios divide Flink median time by StreamFusion
median time. All forks, including outliers and the failed larger case, are retained.
Profiler timings are excluded.

| Backend | Events | Flink median [range]; MAD (s) | StreamFusion median [range]; MAD (s) | Throughput ratio |
| --- | ---: | ---: | ---: | ---: |
| hashmap | 100,000 | 8.309 [5.402, 9.256]; 0.946 | 9.369 [5.425, 9.948]; 0.579 | 0.887× |
| rocksdb | 100,000 | 5.136 [5.100, 5.207]; 0.036 | 5.693 [5.644, 5.749]; 0.049 | 0.902× |
| rocksdb | 500,000 | 17.990 [8.787, 35.262]; 9.203 | 13.727 [8.154, 32.042]; 5.573 | 1.311× |

All three 100k pairs favor Flink on each backend. The in-memory ranges overlap substantially;
the 100k RocksDB ranges are disjoint. At 500k RocksDB, two of three pairs favor StreamFusion,
but the broad overlapping ranges and large dispersion make 1.311× a variable local median,
not a robust speedup or a performance ceiling.

The 500k in-memory comparison is incomplete. Flink completed in 5.668 seconds with 515,539
output rows. StreamFusion failed when join in-flight state requested another 10,211,924 bytes,
with 4,836,474 bytes already reserved by that consumer and 4,018,271 bytes available.
No ratio is calculated for that pair. This is a demonstrated limit under the existing Flink
allowance, not a process-OOM diagnosis or an exact event-count ceiling. A separate 200k
in-memory profile completed without enlarging or bypassing that allowance.

## Correctness and architecture

The prerequisite passed 110 focused Java unit cases and 11 Q23 integration cases. Generated
bare-join and two-join tests compare complete ordered changelog bytes, timestamp envelopes,
all four RowKinds, Unicode/null payloads, 5,000-row fan-out, complete stage metric surfaces,
latency, watermarks/status and pre-barrier callbacks. Both direct Arrow and IPC inputs are
covered on both state backends. Recovery checks cover canonical backend switching,
1→2→1 rescaling, aligned/unaligned snapshots, incremental SST reuse and actual replay of
an in-flight third-input Arrow frame through two restored joins. Existing binary-join and
join/Calc checks also pass. Two older legacy metric assertions were corrected only after
reproducing their stale page-read expectations on the unchanged baseline; compact manifests
already avoid those extra reads.

Original-query collecting validation at 100,000 events passes on both backends at parallelism
1 and 4, comparing complete changelog/result digests and positive blackhole counts. Independent
join inputs can interleave results differently; fixed-arrival harnesses compare every ordered
byte. The pinned upstream generator adds `FIRST_PERSON_ID` twice to bid bidders, so short
runs may produce zero matches. It remains unmodified, and every Q23 validation/measurement
requires non-empty output. Counts alone do not prove byte parity at benchmark scale.

DataFusion evaluates Calc stages and the output record policy; Arrow kernels construct batches.
The retained custom join state transitions preserve Flink's multiset, retraction, arrival-order
and key-group recovery contract, which DataFusion's append-input symmetric hash join does not
provide. The [join contract](/StreamFusion/operators/joins/) documents this semantic adaptation.
State access and changed writes remain batched. Adjacent native stages share Arrow buffers
inside one execution-plan tree; planned exchanges carry Arrow IPC between native regions.
Large buffers and retained state remain under Flink's existing reservations, with no global
allocator enforcement or individual accounting for ephemeral objects.

All completed native forks report acceleration and positive native activity; Flink reports
zero native activity. Completed pairs have identical blackhole output counts.

| Backend | Events | Kind | Blackhole records per engine/fork | Native plan / Calc batches |
| --- | ---: | --- | ---: | --- |
| hashmap | 100,000 | measured, three forks | 59,021 | 397 / 100, 388 / 100, 399 / 100 |
| rocksdb | 100,000 | measured, three forks | 59,021 | 642 / 160, 637 / 160, 643 / 160 |
| rocksdb | 500,000 | measured, three forks | 515,539 | 3125 / 686, 2951 / 650, 2987 / 642 |
| hashmap | 200,000 | profile only | 194,803 | 718 / 160 |
| rocksdb | 1,000,000 | profile only | 1,163,824 | 6004 / 1282 |

## Separate mixed JVM/native profiles

Both engines completed 200k-event in-memory and 1M-event RocksDB profile forks, longer than
their completed measured cases. The in-memory profiles contain 1,986 / 2,024 process CPU
samples for Flink / StreamFusion; RocksDB contains 4,996 / 4,060. JFR, CPU/allocation collapsed
stacks, per-engine flame graphs and differential flame graphs are retained locally.

These shares are inclusive, overlapping percentages of all process CPU samples. JNI includes
downstream native execution, DataFusion projection streams include their custom join children,
and the Arrow-backed output path includes downstream Flink copying. They are not isolated
kernel costs or elapsed-time ratios. Zero means no matching sample.

| CPU category | In-memory Flink | In-memory StreamFusion | RocksDB Flink | RocksDB StreamFusion |
| --- | ---: | ---: | ---: | ---: |
| Row copying | 8.510% | 4.496% | 8.267% | 7.340% |
| RowData-to-Arrow writes | 0.000% | 1.729% | 0.000% | 1.773% |
| Arrow C Data/JNI, inclusive | 0.000% | 8.646% | 0.000% | 25.074% |
| Native plan lowering | 0.000% | 0.000% | 0.000% | 0.000% |
| DataFusion execution, inclusive | 0.000% | 4.447% | 0.000% | 14.236% |
| Native regular join | 0.000% | 4.298% | 0.000% | 12.906% |
| Native join state access | 0.000% | 1.383% | 0.000% | 4.975% |
| Native join decoding | 0.000% | 1.630% | 0.000% | 2.980% |
| Arrow row encoding/decoding | 0.000% | 0.741% | 0.000% | 1.897% |
| Arrow IPC | 0.000% | 0.148% | 0.000% | 0.690% |
| Arrow-backed output path, inclusive | 0.000% | 2.569% | 0.000% | 4.138% |
| Native reservation callbacks | 0.000% | 0.148% | 0.000% | 0.567% |
| RocksDB, inclusive | 0.000% | 0.000% | 37.390% | 9.680% |
| Flink streaming join | 6.798% | 0.000% | 41.473% | 0.000% |
| JIT compilation | 44.109% | 45.702% | 22.558% | 31.429% |

The inspected path is source→RowData-to-Arrow writer→native Calc/exchange, then Flink network
input→Arrow C Stream→DataFusion projection/native join→Arrow IPC or the RowData-view sink edge.
The Flink copying collector before the unchanged blackhole sink still copies RowData fields;
using Arrow views does not eliminate that engine-owned edge cost. Native join leaves include
page decoding, Arrow variable-width decoding, allocation/free and row-vector destruction.
Stored payloads already use shared `Arc` ownership, so cloning the original/updated state
vectors does not duplicate their wide payload bytes. Flink's RocksDB join profile includes
skip-list/index lookup, cache lookup, allocation and locking.

The smaller profiles spend about 44–46% of samples in JIT compilation, limiting conclusions
about sustained join throughput. The larger RocksDB profiles show meaningful state work,
but their shares cannot establish the cause of the variable measured median. Further work
could reduce state decode/workspace pressure and source/sink boundary copying; these remain
open engineering opportunities, not proof that this path has reached its reasonable limits.
This checkpoint retains the required Arrow/DataFusion path, original generator, unmodified
sink and Flink memory semantics. No query-specific shortcut or new tuning knob is introduced.

## Method, artifacts and limits

Both engines use parallelism 4, mini-batching disabled, one-second exactly-once checkpoints,
no restarts, UTC, 1 GiB managed memory and `OPERATOR:70,STATE_BACKEND:70,PYTHON:30`.
JVM flags are `-Xms1g -Xmx1g -XX:MaxDirectMemorySize=2g -XX:ActiveProcessorCount=4`;
no CPU affinity is applied. No builds run alongside measured or profiled forks.

Timing includes Java-only counter reset, setup, EXPLAIN preflight, selected native initialization,
cluster startup, execution and cleanup. It excludes JVM launch, argument parsing and builds.
The host is WSL2 Linux, Intel Core i7-12650H, 16 logical CPUs, approximately 7.6 GiB RAM and
2 GiB swap, Java 24.0.2. Native artifacts use release optimization, native CPU features,
frame pointers and profiling symbols without reducing optimization. CPU baseline fingerprint:
`44dd0ad765af32a3`. Async-profiler 4.5 uses CPU sampling at 10 ms, Java non-safepoint sampling,
native DWARF unwinding, JFR output and allocation sampling at 2 MiB. The profiling-only RocksDB
launcher loads the verified plugin before benchmark main for symbol resolution; measured
forks do not use that launcher.

Verified benchmark JAR artifact SHA-256 values:

- Native runtime: `4bb5f0c035898f29490958d88f1821216cd3ffb984e36f3fb10976e95708e63a`.
- RocksDB plugin: `fe1af76cd4e48dc789eca1eb720d1fdea5b67d40f465956401c39a6653f08862`.

Upstream Flink is `c0f8d1a1e09f209885a88f9c19ceb9d9e9870283`, with only the approved
planner/class-loading installation and complete-StreamGraph memory callback. Nexmark is clean
at `6b3646c3baec701f1fa74baf938d235f742e5d3c`. The source is the deterministic RowData adapter
and the sink is unmodified Flink blackhole. No Kafka service or connector benchmark is involved.

Commands, metadata, partial and completed results, counters and profiles are under
`streamfusion-nexmark-benchmarks/target/measurements/q23/78414b06/`. Other deployment sizes,
mini-batch mode, genuine three-input MultiJoin and recovery capacity at benchmark scale remain
outside this comparison. The larger in-memory failure remains part of the result.

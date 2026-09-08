---
title: Q7 RowData release comparison
description: TUMBLE and timestamp-join admission, smaller-workload measurements, and capacity limits.
---

Q7 now accelerates through the ordinary planner on in-memory and default RocksDB state. Its
local/global TUMBLE MAX uses DataFusion compute, and its timestamp join residual uses DataFusion
integer arithmetic with Flink's full-range millisecond wrapping semantics. Flink still owns
planning, exchanges, resource assignment, checkpoints and recovery.

At 250,000 input events, StreamFusion has 9.4% lower median input throughput in memory and 13.6%
higher median throughput on RocksDB. RocksDB timings vary widely and their ranges overlap.
A one-million-event in-memory attempt exhausts managed memory. These small-workload results
establish neither a general speedup nor large-input capacity.

## Measurements, September 8, 2026

Each engine/backend ran three fresh, unprofiled JVMs, alternating Flink/StreamFusion,
StreamFusion/Flink, Flink/StreamFusion. All runs use 250,000 events; MAD is median absolute deviation.

| Backend | Flink seconds: median [range]; MAD | StreamFusion seconds: median [range]; MAD | StreamFusion/Flink throughput |
| --- | --- | --- | ---: |
| In-memory | 5.013 [4.910–5.192]; 0.103 | 5.534 [5.422–5.557]; 0.023 | 0.906× |
| RocksDB | 9.265 [5.247–10.147]; 0.882 | 8.159 [7.906–9.010]; 0.253 | 1.136× |

Native plan/Calc counts are 182/48 in every memory fork and 198–226/52–60 on RocksDB. Every
StreamFusion run reports ordinary acceleration; Flink records zero native invocations.
Logical operator I/O metrics count Flink records separately from these batch diagnostics.

## Method and correctness

The clean measured checkout is `966ea864a428e98a64cd1df59978e01f9ebe7a91`. Both engines use the
same bounded official Nexmark RowData source, Q7 SQL and unmodified Flink blackhole sink.
End-to-end timing includes setup, EXPLAIN, native initialization, cluster startup, execution and
cleanup; it excludes JVM launch and argument parsing. These are not steady-state rates.

The host is WSL2 Linux, Intel Core i7-12650H, 16 reported logical CPUs, approximately 7.6 GiB RAM
and 2 GiB swap. Both engines use OpenJDK 24.0.2+12-54, 1 GiB fixed heap, 2 GiB maximum direct memory,
`ActiveProcessorCount=4`, and the Arrow-required `java.nio` opening. Flink settings are parallelism
four, 1 GiB managed memory, consumer weights `OPERATOR:90,STATE_BACKEND:10,PYTHON:30`, disabled
mini-batching, one-second exactly-once filesystem checkpoints and no restart. The existing
multi-join optimizer setting is identical. Compilation does not overlap measurements.

Rust 1.94 uses release optimization, `target-cpu=native`, frame pointers and `debuginfo=1`.
Separate DWARF files retain release optimization, and packaged CPU metadata is verified at load.
Artifact SHA-256 values:

- Core: `4d6dfc32984bfe57b5cbb3a01da25470b36a55b71d2739c146c8a63d34e300cb`.
- RocksDB: `39a6c768f5074c0a2c51fee3d30b662933bc8e8bd3a7bdf2d612e3756aba30f9`.

`NexmarkQ7ProductionIT` separately compares complete collected changelog and materialized results
at 10,000 events, parallelism one and four, on both backends. It requires ordinary acceleration,
positive native plan activity and zero standalone local-window JNI invocations. Generated
shared-runtime fixtures compare Flink changelog bytes and complete default metric surfaces,
canonical backend switching, rescaling, and aligned/unaligned input-channel replay. Timestamp
arithmetic covers nulls, negative epochs and signed 64-bit endpoints against Flink-generated code.
Blackhole timing itself provides no output-parity evidence. See the supported
[join](/StreamFusion/operators/joins/) and [window](/StreamFusion/operators/window-aggregation/) contracts.

## General improvements and capacity limit

Small join keys now store one compact record; large keys retain stable external pages. Compact
read and mutation admission follow that physical layout. Immutable hash-state buffers discard
spare vector capacity, and hash bucket descriptors shrink. Input encoding uses logical flat Arrow
spans rather than repeatedly counting shared IPC allocations. Batch-directory credit covers
its largest overlapping allocation phase. Drained state/input buffers are released before backend
write growth, while mutations and pending output retain their own coarse allowances.

Regression tests verify retained bytes, observed allocation peaks, complete payloads, layout
transitions, snapshot compatibility and recovery. These changes preserve DataFusion execution,
one batched state flush per input batch and the same Flink memory budget. They introduce no query
rewrite, deployment tuning option, allocator hook or per-row reservation ledger.

At one million events the final in-memory attempt fails during join mutation staging: it requests
2,947,824 bytes while the in-flight consumer retains 24,309,896 bytes and has 2,738,518 available.
Other state and buffer consumers share the budget; that in-flight figure is not the entire region
footprint. Historical rows are decoded for all touched keys, and the regular join retains state
under its Flink semantics. Bounded output does not bound this accumulated state or batch workspace.
The failed comparison and earlier attempts remain available. There is no million-event throughput
result and no million-event RocksDB measurement. Memory settings were not raised to obtain success.
The measured workload was explicitly reduced to 250,000 events, with separate longer 500,000-event
profiles on both backends. Further capacity work needs a broader state-lifetime improvement;
continuing to trim allowances around a near-threshold run is not justified by these measurements.

## Separate mixed CPU and allocation profiles

Each engine/backend has a separate 500,000-event fork using async-profiler 4.5, 10 ms CPU sampling,
Java non-safepoint sampling, native DWARF unwinding, Java allocation sampling at 2 MiB and JFR output.
Profiled timings are excluded from the throughput table.

| Inclusive CPU category | Flink memory | StreamFusion memory | Flink RocksDB | StreamFusion RocksDB |
| --- | ---: | ---: | ---: | ---: |
| Source polling and synchronous downstream path | 16.24% | 11.25% | 14.79% | 10.56% |
| RowData / binary-row copying | 6.83% | 2.53% | 5.96% | 2.04% |
| RowData-to-Arrow writer | 0.00% | 0.93% | 0.00% | 0.96% |
| Arrow C Data / JNI execution boundary | 0.00% | 8.72% | 0.00% | 18.96% |
| Native plan lowering | 0.00% | 0.00% | 0.00% | 0.00% |
| DataFusion execution | 0.00% | 7.64% | 0.00% | 6.30% |
| Native local window | 0.00% | 0.10% | 0.00% | 0.04% |
| Native global window | 0.00% | 0.00% | 0.00% | 0.00% |
| Native streaming join | 0.00% | 8.31% | 0.00% | 8.26% |
| Memory-budget callbacks | 0.00% | 0.00% | 0.00% | 0.43% |
| Arrow-backed row access / sink adapter | 0.00% | 0.10% | 0.00% | 0.00% |
| RocksDB | 0.00% | 0.00% | 16.40% | 15.65% |
| Garbage collection | 6.31% | 3.10% | 3.51% | 2.70% |
| Native artifact initialization | 0.00% | 2.06% | 0.00% | 1.91% |
| JIT compilation | 42.84% | 43.50% | 40.79% | 38.30% |

Denominators are all process CPU samples: 2,124/1,938 in memory and 2,366/2,300 on RocksDB
(Flink/StreamFusion). Categories overlap: JNI includes native descendants, DataFusion includes
its execution-plan framework and custom Flink adaptations, and source polling includes the
synchronous downstream chain. Zero samples do not imply zero cost. Profiled StreamFusion counts
are 305/80 native plan/Calc invocations in memory and 319/84 on RocksDB.

JIT and startup remain substantial. Native join profiles show state loading, compact encoding,
mutation staging and checkpoint state work. Java allocation samples are led by byte arrays and
Flink memory-segment descriptors; these sample counts are not allocated bytes and do not measure
all native allocations. No further query-specific optimization is inferred from these short runs.

Raw logs, commands/classpaths, machine/build metadata, summaries, JFR files, CPU and allocation
collapsed stacks, per-engine flame graphs and differential flame graphs remain under
`streamfusion-nexmark-benchmarks/target/measurements/q7/966ea864/`, in each backend's `250000` and
`profiles-500000` directories. The failed memory comparison is under `hashmap/1000000`.
Earlier commit directories retain their failed attempts. Generated profiles are not checked in.

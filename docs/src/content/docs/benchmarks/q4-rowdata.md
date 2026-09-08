---
title: Q4 RowData release comparison
description: Range-join and integer aggregation admission, measured results, and mixed runtime profiles.
---

Q4 is admitted by the ordinary planner on in-memory and default RocksDB state. It combines a
binary timestamp range join, keyed MAX, and retracting integer AVG. The one-million-event
RowData-to-blackhole comparison is approximately tied in memory and shows 15.5% higher median
input throughput for StreamFusion on RocksDB. Both comparisons have overlapping timing ranges.

## Measurements, September 8, 2026

Each engine ran in three fresh, unprofiled JVMs. Engine order alternated Flink/StreamFusion,
StreamFusion/Flink, Flink/StreamFusion. MAD is median absolute deviation.

| Backend | Input events | Flink seconds: median [range]; MAD | StreamFusion seconds: median [range]; MAD | StreamFusion/Flink throughput |
| --- | ---: | --- | --- | ---: |
| In-memory | 1,000,000 | 6.648 [6.488–6.922]; 0.161 | 6.602 [6.530–6.649]; 0.047 | 1.007× |
| RocksDB | 1,000,000 | 7.954 [7.939–7.958]; 0.004 | 6.888 [6.720–7.962]; 0.167 | 1.155× |

Median input rates are 150,416 versus 151,473 events/s in memory and 125,729 versus 145,185
events/s on RocksDB, respectively Flink and StreamFusion. The 0.7% memory difference is too
small to claim a meaningful advantage here. The RocksDB series retains its slower StreamFusion
fork; these three pairs on one host do not establish a universal speedup.

Every StreamFusion fork reports `Accelerated: yes`. Memory forks record 12,223–12,383 native
plan invocations and 528–536 Calc batches; RocksDB forks record 11,982–12,748 and 526–534.
Flink records zero native invocations. These diagnostics count native batches/streams, while
operator I/O metrics retain logical-record definitions.

## Method and correctness boundary

The clean benchmark checkout is `bcb0ba926232437df1008b349b14def609d71126`. Both engines use the
same deterministic bounded Nexmark RowData source, Q4 SQL, and unmodified Flink blackhole sink.
The end-to-end timer includes table setup, ordinary EXPLAIN preflight, StreamFusion native
initialization, local cluster startup, execution and cleanup. JVM launch and argument parsing
are outside it; separate process wall times are retained. These are not steady-state rates.

Generated shared-runtime tests compare complete changelog bytes and the full default Flink
metric surface for the range join and integer aggregation. Cases include nulls, inclusive
and negative timestamp boundaries, all RowKinds, integer overflow and retraction, downstream
Calc composition, canonical backend switching, aligned/unaligned checkpoints, rescaling and
actual Arrow channel replay. Insert-only and retracting aggregation paths are both covered.
Separate Q4 collecting-sink integration compares materialized results at parallelism one and
four on both backends. Independent jobs may interleave the two join inputs differently, so
that end-to-end comparison supplements the deterministic complete-changelog tests. Blackhole
timings alone establish no correctness claim. Mini-batching and unverified semantic subsets
remain explicit whole-plan fallbacks.

The machine is WSL2 Linux on an Intel Core i7-12650H, reporting 16 logical CPUs, approximately
7.6 GiB RAM and 2 GiB swap. Both engines use OpenJDK 24.0.2+12-54, `-Xms1g -Xmx1g`,
`-XX:MaxDirectMemorySize=2g`, `-XX:ActiveProcessorCount=4` and the Arrow-required `java.nio`
opening. Flink settings are parallelism four, 1 GiB managed memory, consumer weights
`OPERATOR:90,STATE_BACKEND:10,PYTHON:30`, disabled mini-batching, one-second exactly-once
filesystem checkpoints and no restart. Measurements run without concurrent compilation.

Native artifacts use Rust 1.94 release optimization, `target-cpu=native`, frame pointers and
`debuginfo=1`; symbols do not reduce optimization. Runtime ELF libraries have separate DWARF
files connected by GNU debug links, and CPU metadata is verified at load time. Packaged SHA-256:

- Core: `1cc2628a41932ad72943d47e58ed9823c7a3fd6b5f70106b42a3c7fc8f33668d`.
- RocksDB state: `39a6c768f5074c0a2c51fee3d30b662933bc8e8bd3a7bdf2d612e3756aba30f9`.

## General join optimization

Initial Q4 profiles at `e9bc6cf0` exposed per-input-row residual evaluation and memory-budget
callbacks. The join now evaluates bounded groups of candidate pairs with DataFusion expressions,
using Arrow slices for contiguous input selections and Arrow take otherwise. Shared mask owners
reserve memory once for the group and retain it through output pulls. The algorithm preserves
input transition order, bounded hot-key fallback, state writes and Flink changelog semantics.
It applies to ordinary join predicates rather than recognizing a query or changing its SQL.

A focused 1,024-row test observed 4,119 budget callbacks before the change; the new path passes
a limit of fewer than 64. Generated mixed-key and hot-key tests cover both input ports, all six
join kinds, retractions, output chunking and memory-denial recovery. No global allocator hook
or per-allocation accounting was introduced.

The earlier one-million-event StreamFusion medians were 8.699 seconds in memory and 7.967 on
RocksDB. However, the earlier Flink memory median was 12.069 seconds with a 6.536–14.460 range,
versus 6.648 in the final series. This variation prevents attributing the whole elapsed-time
change to the optimization. The final matched comparison above is the performance result;
the callback test and profiles independently establish the intended reduction in overhead.

## Separate CPU and allocation profiles

Each engine/backend pair has a separate longer two-million-event fork recorded with
async-profiler 4.5, 10 ms CPU sampling, Java non-safepoint sampling, native DWARF unwinding,
allocation sampling at 2 MiB, and JFR output. Profiled timings are excluded from measurements.

| Inclusive CPU category | Flink memory | StreamFusion memory | Flink RocksDB | StreamFusion RocksDB |
| --- | ---: | ---: | ---: | ---: |
| Source polling and synchronous downstream path | 27.17% | 21.74% | 16.70% | 16.68% |
| RowData serializer / binary row copying | 19.06% | 4.71% | 9.14% | 4.17% |
| RowData-to-Arrow writer | 0.00% | 2.86% | 0.00% | 1.86% |
| Arrow C Data / JNI execution boundary | 0.00% | 21.57% | 0.00% | 26.16% |
| Native plan lowering | 0.00% | 0.00% | 0.00% | 0.00% |
| DataFusion execution | 0.00% | 11.28% | 0.00% | 18.47% |
| Native streaming join | 0.00% | 4.71% | 0.00% | 12.72% |
| Native streaming aggregation | 0.00% | 5.46% | 0.00% | 8.31% |
| JVM memory-budget reserve/release callbacks | 0.00% | 1.42% | 0.00% | 1.46% |
| Arrow-backed row access / sink adapter | 0.00% | 0.43% | 0.00% | 0.20% |
| RocksDB | 0.00% | 0.00% | 38.28% | 0.46% |
| Garbage collection | 11.39% | 2.29% | 1.70% | 2.32% |
| StreamFusion artifact initialization | 0.00% | 0.97% | 0.00% | 0.92% |
| JIT compilation | 28.08% | 32.10% | 22.74% | 31.32% |

Denominators are all process CPU samples: 4,391 / 4,228 for memory and 6,244 / 4,993 for RocksDB
(Flink / StreamFusion). Categories overlap. JNI includes native execution beneath the call;
DataFusion includes physical-plan and expression descendants; source polling includes the
synchronous downstream chain. Zero sampled plan lowering does not mean zero lowering cost.
Profiled StreamFusion runs record 23,710 / 1,032 plan/Calc invocations in memory and
23,653 / 1,012 on RocksDB.

Before batching, budget callbacks accounted for 6.85% of memory samples and 5.78% on RocksDB;
the final profiles show about 1.4–1.5%. Native join shares changed from 17.26% to 4.71% in memory
and from 14.14% to 12.72% on RocksDB. These are sample shares, not isolated speedup estimates.
The remaining profiles include substantial JIT, streaming state computation and required
source/Arrow boundary work. This checkpoint stops after the measured general improvement;
it does not add specialized algorithms to fit Q4.

An earlier five-million-event Flink diagnostic profile failed with a TaskManager heartbeat
timeout. Its CPU sample share was approximately 85% garbage collection. There was no completed
comparison and StreamFusion was not started. The failed log, recording and failure metadata
are retained; it is neither an out-of-memory diagnosis nor a throughput result. Matched
profiles completed at two million events with the same JVM and Flink settings.

Raw logs, exact commands/classpaths, CPU/build metadata, summaries, JFR recordings, CPU and
allocation collapsed stacks, per-engine flame graphs, differential flame graphs and category
scripts are retained under `streamfusion-nexmark-benchmarks/target/measurements/q4/`.
The `e9bc6cf0` and `bcb0ba92` directories each contain backend-specific `one-million` and
`profiles-two-million` runs. The failed diagnostic is under `e9bc6cf0/hashmap/profiles`.
Generated artifacts are intentionally not checked into Git.

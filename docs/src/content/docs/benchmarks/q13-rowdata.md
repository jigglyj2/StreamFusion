---
title: Q13 lookup RowData release comparison
description: Cached CSV lookup acceleration, matched release measurements, and mixed JVM/native profiles.
---

Q13 now accelerates through ordinary whole-plan selection using a cached CSV lookup inside a
fused DataFusion-based native region. At ten million events its median throughput is 1.113×
Flink with HashMap configured and 1.149× with RocksDB configured; timing ranges do not overlap.
At one million events StreamFusion is slower. These are end-to-end measurements, not steady-state
rates or a claim that the implementation has reached a performance ceiling.

Each cell gives median seconds, [minimum, maximum], and median absolute deviation (MAD) from
three alternating fresh-JVM pairs. The throughput ratio is Flink median time divided by
StreamFusion median time. No measured forks or outliers were discarded.

| Events | Backend configuration | Flink seconds [range]; MAD | StreamFusion seconds [range]; MAD | Throughput ratio |
| --- | --- | --- | --- | --- |
| 1,000,000 | HashMap | 5.123 [5.101, 5.123]; 0.0001 | 5.829 [5.770, 5.969]; 0.060 | 0.879× |
| 1,000,000 | RocksDB configured | 5.278 [5.171, 5.288]; 0.010 | 5.826 [5.797, 5.974]; 0.029 | 0.906× |
| 10,000,000 | HashMap | 12.273 [12.056, 12.322]; 0.049 | 11.026 [10.977, 11.238]; 0.048 | 1.113× |
| 10,000,000 | RocksDB configured | 12.320 [11.962, 12.973]; 0.358 | 10.723 [10.710, 11.344]; 0.013 | 1.149× |

Every measured StreamFusion fork reports `Accelerated: yes` and positive native activity.
Both engines emit exactly 920,000 rows at one million events and 9,200,000 at ten million.
The cache is immutable task-local lookup state, not keyed RocksDB state. These backend
configurations therefore do not constitute RocksDB state-performance evidence.

## Method and correctness

The clean measured checkout is `7d39af39e795aba13d7d97f384526040df84830f`. Both engines use the same bounded,
deterministic Nexmark RowData adapter, the unchanged upstream Q13 SELECT, the legacy CSV lookup
source, and Flink's unmodified blackhole sink. Benchmark setup writes upstream SideInputGenerator's
10,000 integer/string pairs to a temporary file, and cleanup deletes it. The same source adapter
normalizes upstream process-random strings and URLs for reproducible bytes in both engines;
that existing work remains in the measured source path. No generator or deserializer optimization
was introduced for Q13.

Timing includes counter reset, setup, EXPLAIN, native initialization, cluster startup, execution,
and cleanup. JVM launch and argument parsing are excluded. The source and sink are Kafka-free.
The host is WSL2 Linux on an Intel Core i7-12650H, with 16 reported logical CPUs, approximately
7.6 GiB RAM and 2 GiB swap. Both engines use OpenJDK 24.0.2, a fixed 1 GiB heap, a 2 GiB direct-memory
limit, `ActiveProcessorCount=4`, UTC, and the Arrow-required `java.nio` opening. Flink settings are
parallelism four, 1 GiB managed memory, weights `OPERATOR:90,STATE_BACKEND:10,PYTHON:30`, disabled
mini-batching, one-second exactly-once filesystem checkpoints, and no restart. Compilation did
not overlap measured or profiled execution.

Rust 1.94 uses release optimization, native CPU features, frame pointers and `debuginfo=1`.
Separate DWARF files preserve release optimization. CPU requirements are recorded and checked
when native artifacts load. Artifact SHA-256 values:

- Core: `8bb167ab46be3d4030af8a38ffcb8a0d4e2f7f4a223759d1e3d7bde5f2cbbbcb`.
- RocksDB: `118e0d6c10fb0ed24ef99d44e0bc8eaf7d3f81402554bf5b9848ba806cf2e85a`.

`NexmarkQ13ProductionIT` separately compares full collected changelog hashes and materialized
results at 50,000 source events, parallelism one and four, with both backends configured. The
blackhole count matches each collecting run. A separate check compares the catalog SELECT
verbatim with the upstream SQL. `NexmarkQ13PlanningIT` also executes the original SQL directly
from the Nexmark JAR. Blackhole timing alone does not prove result parity.

Generated runtime tests compare ordered serialized rows, all four changelog kinds, duplicate
file order, SQL-null keys, record timestamps, watermark/idle events and complete operator metric
surfaces against Flink's actual lookup code generator, CSV function and ProcessOperator. They
cover Calc/lookup/Calc fusion and multiple native consumers of the same lookup stage. Source
serialization performs no file I/O; task open reads the snapshot once. Aligned and unaligned
operator snapshots retain no lookup cache, matching Flink; restoring a task reloads the source.
Cancellation, parsing errors and budget denial release managed credit. Shared Arrow payloads
are charged once, and retained outputs keep their producer ownership alive.

## Implementation and remaining opportunities

The lookup delegates hashing, bounded candidate enumeration and equality to DataFusion and
uses Arrow gather kernels. The immutable build table is constructed once; probe invocation does
not rebuild a DataFusion plan or accumulate metric descriptors. A Flink-specific lifetime adapter
is necessary because repeatedly executing DataFusion 55 HashJoinExec grows retained metrics,
while keeping its stream open can defer small outputs across Flink control boundaries. No
DataFusion code is privately patched. See [join support](/StreamFusion/operators/joins/) for
this justification, memory behavior and precise unsupported-semantic fallbacks.

The physical region exchanges Arrow batches directly between Calc and lookup stages. The CSV
reader is an explicit source boundary, and the Java sink receives a lightweight Arrow-backed
RowData view. Native output is bounded to 1,024 candidates per pull and shrinks under memory
pressure; neither candidate fan-out nor output buffering grows without admission. These are
general execution policies rather than special cases for Q13's modulo key or unique side file.

The longer profiles put lookup execution at about 3% of process CPU, Arrow gathers below 0.5%,
and budget callbacks below 0.5%. Selection-size checks are the largest individual lookup leaf,
but account for only about 0.9% of process samples. Reusing more contiguous probe buffers or
precomputing fixed-width selection sizes remains possible; this report does not establish a
material end-to-end benefit from either change. The larger opportunities are the common source,
RowData copying and Arrow boundary path. The source is deliberately unchanged in this comparison.
There is no claim that all general boundary improvements have been exhausted.

## Mixed JVM/native profiles

Separate two- and twenty-million-event forks use async-profiler 4.5, CPU sampling at 10 ms,
Java non-safepoint sampling, native DWARF unwinding and JFR output, plus Java allocation sampling
at 2 MiB. Profiled timings are excluded from the measured table. Per-engine JFR, flame graphs,
collapsed stacks, allocation stacks and differential flame graphs are retained under `target/`.

The following percentages use all process CPU samples in the twenty-million-event profiles.
Categories are inclusive and overlap: source polling includes chained operators, JNI includes
native execution, and lookup includes its DataFusion work. Zero samples do not imply zero cost.

| CPU category | Flink HashMap | StreamFusion HashMap | Flink RocksDB configured | StreamFusion RocksDB configured |
| --- | --- | --- | --- | --- |
| Source polling, inclusive | 82.86% | 72.67% | 82.64% | 72.91% |
| Row copying | 44.61% | 17.95% | 43.35% | 18.47% |
| RowData-to-Arrow writes | 0.00% | 5.79% | 0.00% | 5.15% |
| Arrow C Data/JNI, inclusive | 0.00% | 8.38% | 0.00% | 7.94% |
| Native plan lowering | 0.00% | 0.02% | 0.00% | 0.00% |
| DataFusion execution | 0.00% | 5.79% | 0.00% | 5.79% |
| Native lookup, inclusive | 0.00% | 3.12% | 0.00% | 2.98% |
| Arrow gathers | 0.00% | 0.43% | 0.00% | 0.36% |
| Snapshot setup | 0.00% | 0.58% | 0.00% | 0.54% |
| Flink lookup, inclusive | 12.95% | 0.00% | 12.48% | 0.00% |
| Arrow-backed output access | 0.00% | 5.64% | 0.00% | 5.79% |
| Memory budget callbacks | 0.00% | 0.27% | 0.00% | 0.41% |
| Garbage collection | 2.31% | 2.40% | 2.04% | 2.30% |
| JIT compilation | 9.17% | 15.56% | 9.37% | 15.42% |
| Native artifact loading | 0.00% | 0.65% | 0.00% | 0.64% |

The four profiles contain 7,604, 6,049, 7,830 and 6,134 CPU samples respectively. No `[unknown]`
stacks were observed; some libc leaves remain library-level names. The only RocksDB-category
sample in the native RocksDB-configured profile is backend configuration/class initialization,
not a state lookup. In the shorter profiles, JIT compilation accounts for roughly 35.7%/40.0%
of Flink/StreamFusion samples with HashMap and 27.5%/40.7% with RocksDB configured. This supports
startup sensitivity as an interpretation of short-run results, not a measured decomposition
of their elapsed-time difference.

## Native activity and retained evidence

The entries below list reported native plan/Calc batch counters for each StreamFusion fork.
These are instrumentation counters for native invocations and Calc activity, not output rows.
Flink reports zero for both counters in every measured and profiled fork.

| Events | Backend configuration | Native plan/Calc counters, in measured fork order |
| --- | --- | --- |
| 1,000,000 | hashmap | 178/75, 180/76, 172/72 |
| 1,000,000 | rocksdb | 176/74, 174/73, 176/74 |
| 10,000,000 | hashmap | 1324/632, 1326/631, 1329/633 |
| 10,000,000 | rocksdb | 1316/628, 1314/625, 1320/628 |

The two-million-event profiles emit 1,840,000 rows per engine; native counters are 292/132 with
HashMap and 296/134 with RocksDB configured. The twenty-million-event profiles emit 18,400,000
rows per engine; native counters are 2,588/1,242 and 2,610/1,253 respectively.

Raw evidence is under:

```text
streamfusion-nexmark-benchmarks/target/measurements/q13/7d39af39/
  {hashmap,rocksdb}/1000000/
  {hashmap,rocksdb}/10000000/
  {hashmap,rocksdb}/profiles-2000000/
  {hashmap,rocksdb}/profiles-20000000/
```

Each measured directory retains commands, per-fork logs, results, summary statistics, machine
and runtime metadata, CPU/artifact requirements, and upstream checkout commits and patch hashes.
Profile directories add the mixed stacks, per-engine and differential flame graphs, debug-symbol
metadata and CPU-category definitions. None of these generated profile artifacts is checked in.

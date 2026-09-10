---
title: Q8 RowData release comparison
description: DISTINCT TUMBLE admission, release measurements, and mixed JVM/native profiles.
---

Q8 now passes ordinary selection and official collecting/blackhole parity with both Flink's
default `table.optimizer.multi-join.enabled=false` and the benchmark's enabled preset, on both
backends and at parallelism one/four. The former uses the shared native WindowJoin added during
Q5 integration. The September 8 measurements below cover only the enabled multi-join preset;
the [default-path baseline](/StreamFusion/benchmarks/q8-default-rowdata/) now records release `d4a3db14`
with separate one/ten-million-event measurements, collecting parity, profiles and current limits.
Its two DISTINCT TUMBLE branches use DataFusion grouped presence state and join on the person/seller
and window bounds. Flink retains planning, routing, resource assignment, watermarks and recovery.

At one million events, StreamFusion's median input throughput is 10.8% below Flink in memory and
11.8% below on RocksDB. At ten million, it is 12.4% below in memory and 26.3% above on RocksDB.
The larger-workload timing ranges overlap widely. This establishes no general speedup or
in-memory median win. Separate twenty-million-event profiles complete on both backends without
capacity failure; their instrumented timings are excluded from the performance comparison.

## Measurements, September 8, 2026

Each size/backend uses three fresh unprofiled JVMs per engine, in alternating order:
Flink/StreamFusion, StreamFusion/Flink, Flink/StreamFusion. Every fork is retained.
MAD is median absolute deviation; throughput counts generated input events.

| Events | Backend | Flink seconds: median [range]; MAD | StreamFusion seconds: median [range]; MAD | StreamFusion/Flink throughput |
| ---: | --- | --- | --- | ---: |
| 1,000,000 | In-memory | 5.335 [5.325–5.394]; 0.011 | 5.980 [5.953–6.179]; 0.026 | 0.892× |
| 1,000,000 | RocksDB | 5.455 [5.377–5.578]; 0.078 | 6.182 [6.092–6.310]; 0.090 | 0.882× |
| 10,000,000 | In-memory | 12.361 [11.984–14.573]; 0.378 | 14.106 [11.590–17.534]; 2.516 | 0.876× |
| 10,000,000 | RocksDB | 15.724 [13.209–15.843]; 0.119 | 12.452 [12.316–14.269]; 0.136 | 1.263× |

Every StreamFusion fork reports ordinary acceleration and positive native plan activity; Flink
records zero native invocations. At one million events, plan/Calc counts are 852–864/260–264 in
memory and 856–876/264–270 on RocksDB. At ten million they are 7,096–7,146/2,473–2,480 in memory and
7,060–7,172/2,464–2,488 on RocksDB. These batch diagnostics are separate from logical Flink I/O metrics.

## Method and correctness

The clean measured checkout is `364230b8dbffb1f0515fca4eebc34a6569d6be11`. Both engines use the
same bounded official Nexmark RowData source, Q8 SQL and unmodified Flink blackhole sink.
End-to-end timing includes setup, EXPLAIN, native initialization, cluster startup, execution and
cleanup; it excludes JVM launch and argument parsing. These are not steady-state rates.

The host is WSL2 Linux, Intel Core i7-12650H, 16 reported logical CPUs, approximately 7.6 GiB RAM
and 2 GiB swap. Both engines use OpenJDK 24.0.2+12-54, a fixed 1 GiB heap, 2 GiB maximum direct memory,
`ActiveProcessorCount=4`, and the Arrow-required `java.nio` opening. Flink settings are parallelism
four, 1 GiB managed memory, consumer weights `OPERATOR:90,STATE_BACKEND:10,PYTHON:30`, disabled
mini-batching, one-second exactly-once filesystem checkpoints and no restart. The existing
multi-join optimizer setting is identical. Compilation does not overlap measurements.

Rust 1.94 uses release optimization, `target-cpu=native`, frame pointers and `debuginfo=1`.
Separate DWARF files retain release optimization; packaged CPU metadata is verified at load.
Artifact SHA-256 values:

- Core: `2643515e0cd89c5b0758db40b59ce3f38368b9c682af79613db356dd5cad9d34`.
- RocksDB: `39a6c768f5074c0a2c51fee3d30b662933bc8e8bd3a7bdf2d612e3756aba30f9`.

`NexmarkQ8ProductionIT` separately checks complete collected changelog bytes and materialized
results at 10,000 events, parallelism one and four, on both backends. It requires ordinary selection,
positive native plan activity and zero standalone local-window JNI invocations. It now also
checks unmodified-blackhole record counts and explicitly exercises both optimizer settings. Generated SQL
covers nullable composite Unicode keys, negative epochs and multiple window sizes. Native and
Flink-generated operator tests cover full registered metrics, canonical backend switching,
one-to-two-to-one rescaling and aligned/unaligned Arrow channel replay. Equal-window-end output
normalization preserves complete record bytes and multiplicity, window order and control boundaries:
Flink's timer comparator does not order distinct keys at a tied timestamp. Blackhole timing itself
provides no output-parity evidence. See the [window contract](/StreamFusion/operators/window-aggregation/).

## General changes and profile findings

Local VARCHAR grouping now models Flink's inline/padded variable storage from Arrow UTF-8 lengths,
preserving memory-pressure flushes without constructing rows. Global window input admission includes
logical wide-key spans before encoding, and its grouping hash table indexes one owned key vector
by ordinal instead of retaining a duplicate key. Tests cover wide sliced keys, restore, early
budget denial and credit return. These changes apply to the operator family; they introduce no
Q8-specific SQL rewrite, source filter, runtime tuning option or additional JNI handoff.

Separate two-million and twenty-million-event forks capture async-profiler 4.5 CPU samples every
10 ms with Java non-safepoint sampling and native DWARF unwinding, plus sampled Java allocations
at a 2 MiB interval. JFR, CPU collapsed stacks/flame graphs, allocation collapsed stacks and
differential flame graphs are retained. The table below uses the longer profiles; percentages are
inclusive shares of all process CPU samples and overlap. JNI includes downstream native execution,
and DataFusion includes custom children beneath its execution framework. Zero samples do not imply
zero cost. Totals are 9,024/7,572 samples for Flink/StreamFusion in memory and 11,007/8,611 on RocksDB.

| Inclusive CPU category | Flink memory | StreamFusion memory | Flink RocksDB | StreamFusion RocksDB |
| --- | ---: | ---: | ---: | ---: |
| Source polling and descendants | 56.106% | 62.586% | 47.397% | 55.371% |
| RowData copying | 24.379% | 15.848% | 20.233% | 14.331% |
| RowData-to-Arrow writing | 0.000% | 5.877% | 0.000% | 4.854% |
| Arrow C Data/JNI | 0.000% | 10.909% | 0.000% | 18.279% |
| Native plan lowering | 0.000% | 0.026% | 0.000% | 0.000% |
| DataFusion execution | 0.000% | 7.264% | 0.000% | 15.016% |
| Native local window | 0.000% | 0.792% | 0.000% | 0.581% |
| Native global window | 0.000% | 4.160% | 0.000% | 10.556% |
| Native join | 0.000% | 1.387% | 0.000% | 4.854% |
| Memory-budget callbacks | 0.000% | 0.409% | 0.000% | 0.592% |
| Arrow-backed row access | 0.000% | 0.357% | 0.000% | 0.105% |
| RocksDB and native plugin boundary | 0.000% | 0.000% | 28.273% | 12.786% |
| Garbage collection | 9.996% | 2.060% | 1.835% | 1.928% |
| Native artifact loading | 0.000% | 0.515% | 0.000% | 0.523% |
| JVM compilation | 12.644% | 17.235% | 11.765% | 16.189% |

The StreamFusion RocksDB profile has 717 samples (8.3% of its total) with unresolved leaf frames
below the named native RocksDB plugin boundary. The table includes them in that boundary's cost;
it does not attribute them to specific RocksDB internals. Verified debug files do not eliminate
this observed symbolization limit. The outer source/JNI/DataFusion/state call chain remains visible.

Both profiles show substantial source work, including Nexmark event/string generation and row
handling. StreamFusion has fewer absolute row-copy samples, while Arrow writing and native state
processing add costs. Native lowering and artifact loading are small sampled shares. Java allocation
sample counts are 58,168/36,530 in memory and 58,236/36,567 on RocksDB for Flink/StreamFusion, dominated
by byte arrays, memory segments and row/string objects. These are sample counts, not allocated bytes,
retained memory or a native allocator profile. The evidence does not justify changing query semantics
or specializing the source to improve this result. No further query-specific optimization was added.

## Reproduction and retained evidence

Build/install release artifacts first, then use `NexmarkBlackholeBenchmark` as described in the
[RowData blackhole method](/StreamFusion/benchmarks/rowdata-blackhole/), with `q8`, parallelism four,
and the settings above. Run each engine in its own JVM and alternate the order for at least three
unprofiled pairs. Run longer profiled forks separately.

Local evidence is under `streamfusion-nexmark-benchmarks/target/measurements/q8/364230b8/`, divided
by `hashmap`/`rocksdb` and `1000000`, `10000000`, `profiles-2000000`, `profiles-20000000`.
It includes exact commands, native build metadata, machine/runtime details, EXPLAIN/logs, every
fork result, medians/ranges/MAD and profile artifacts. Generated profiles are not checked in.

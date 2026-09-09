---
title: Q10 SELECT RowData release comparison
description: Numeric DATE_FORMAT acceleration, release measurements, and mixed JVM/native profiles.
---

Q10's SELECT path now accelerates through ordinary whole-plan selection. The RowData workload
uses the unchanged SELECT body from the official filesystem query, including its date and hour/minute
partition labels, with a blackhole sink. It does not exercise filesystem partition commits, file
rolling or CSV encoding. Those sink behaviors remain outside this report's evidence.

StreamFusion remains slower at one million events and approaches Flink at ten million. These
measurements do not establish a throughput win. Each cell reports the median end-to-end seconds,
[min, max], and median absolute deviation (MAD) from three alternating fresh-JVM pairs.
Throughput ratio is Flink median time divided by StreamFusion median time; greater than one is faster.

| Events | Backend configuration | Flink seconds [range]; MAD | StreamFusion seconds [range]; MAD | Throughput ratio |
| --- | --- | --- | --- | --- |
| 1,000,000 | In-memory | 4.819 [4.779, 4.878]; 0.041 | 5.494 [5.487, 5.705]; 0.007 | 0.877× |
| 1,000,000 | RocksDB configured | 4.784 [4.770, 4.872]; 0.015 | 5.509 [5.478, 5.751]; 0.031 | 0.868× |
| 10,000,000 | In-memory | 10.971 [10.816, 10.994]; 0.022 | 11.039 [10.984, 11.122]; 0.055 | 0.994× |
| 10,000,000 | RocksDB configured | 10.869 [10.856, 11.298]; 0.012 | 11.098 [11.021, 11.164]; 0.066 | 0.979× |

All measured StreamFusion forks report ordinary acceleration and positive native plan/Calc batches.
At ten million the timing ranges overlap. No runs or outliers were discarded.


## Method and correctness

The clean measured checkout is `8d1d994354e40d075a9dd7420a0f9a354ddb5cb9`. Both engines use the
same bounded official Nexmark RowData source, Q10 SELECT and unmodified Flink blackhole sink.
End-to-end timing includes setup, EXPLAIN, native initialization, cluster startup, execution and
cleanup; it excludes JVM launch and argument parsing. These are not steady-state rates.

The host is WSL2 Linux, Intel Core i7-12650H, 16 reported logical CPUs, approximately 7.6 GiB RAM
and 2 GiB swap. Both engines use OpenJDK 24.0.2+12-54, a fixed 1 GiB heap, 2 GiB maximum direct memory,
`ActiveProcessorCount=4`, and the Arrow-required `java.nio` opening. Flink settings are parallelism
four, 1 GiB managed memory, consumer weights `OPERATOR:90,STATE_BACKEND:10,PYTHON:30`, disabled
mini-batching, one-second exactly-once filesystem checkpoints and no restart. Compilation does not
overlap measurements. The query is stateless: the RocksDB-configured run is not state-performance
or state-recovery evidence, and this query's profiles do not exercise RocksDB state access.

Rust 1.94 uses release optimization, `target-cpu=native`, frame pointers and `debuginfo=1`.
Separate DWARF files retain release optimization; packaged CPU metadata is verified at load.
Artifact SHA-256 values:

- Core: `1b8ff8e8567ff3448691ed0f0554cf8cdf720f9f8e888ac2d9c39e17c386ba65`.
- RocksDB: `39a6c768f5074c0a2c51fee3d30b662933bc8e8bd3a7bdf2d612e3756aba30f9`.

`NexmarkQ10ProductionIT` separately checks the complete collected result bytes and materialized
results at 10,000 events, parallelism one and four, on both backend configurations. It requires
ordinary acceleration and positive shared plan/Calc activity. `NexmarkQ10CatalogTest` checks that
the SELECT matches the official filesystem SQL and retains both partition-label columns.
Blackhole timing itself provides no output-parity evidence.

Generated SQL tests compare ordered Flink RowData bytes without sorting for all four changelog
kinds and three seeds. Inputs combine ordinary years and the complete signed-millisecond range,
including both limits, BCE/year zero, AD year one, year 10,000, negative epochs, leap days, nulls
and a non-UTC session zone. Patterns include numeric fields, repeated years, quoted Unicode,
escaped apostrophes, percent signs and the empty pattern. Native tests add sliced/scalar/empty input,
large output, early budget denial and output slices retaining credit until their final release.
Unsupported formatting variants receive precise whole-plan fallback reasons.

## General formatting change

Numeric `DATE_FORMAT` over timezone-free `TIMESTAMP(3)` lowers to DataFusion arithmetic,
comparisons, `date_part`, `to_char`, padding and concatenation kernels. A Gregorian 400-year cycle
rebase and Java year-of-era/sign adaptation cover timestamps outside Arrow/chrono's calendar range.
For AD years 1–9999, where Java numeric year formatting and Arrow agree, DataFusion selects direct
`to_char`; other rows retain the full-range path, including within mixed batches. There is no
handwritten per-row calendar algorithm, Q10-specific format substitution or query rewrite.
One coarse managed reservation covers the format workspace and output, with buffer ownership
retained through the shared Calc tree and its Arrow output.

The initial implementation at `1227c358` always used the cycle adaptation. Its million-event
three-pair medians were 4.684/5.513 seconds for Flink/StreamFusion in memory (0.850× throughput)
and 4.827/5.516 with RocksDB configured (0.875×). Separate two-million-event profiles attributed
3.6–4.2% of process CPU samples to native formatting, motivating the general direct path.
The final million-event results remain slower than Flink; this change alone does not establish
a short-run throughput improvement. Earlier measurements and profiles are retained separately.

## Mixed JVM/native profiles

Separate two- and twenty-million-event forks use async-profiler 4.5 CPU sampling at 10 ms,
Java non-safepoint sampling, native DWARF unwinding and JFR output, plus Java allocation sampling
at 2 MiB. Their timings are excluded from the throughput table. Per-engine flame graphs,
collapsed stacks, JFR files, allocation stacks and differential flame graphs are retained.

The following percentages use all process CPU samples in the twenty-million-event profiles.
Categories are inclusive and overlap: source polling contains chained downstream work, JNI includes
native computation, and formatting is included in DataFusion. Zero samples do not prove zero cost.

| CPU category | Flink memory | StreamFusion memory | Flink RocksDB configured | StreamFusion RocksDB configured |
| --- | --- | --- | --- | --- |
| Source polling, inclusive | 81.96% | 75.53% | 82.25% | 74.78% |
| Row copying | 35.76% | 23.87% | 35.30% | 25.19% |
| RowData → Arrow writes | 0.00% | 4.75% | 0.00% | 5.03% |
| Arrow C Data / JNI, inclusive | 0.00% | 9.21% | 0.00% | 8.66% |
| Native plan lowering | 0.00% | 0.00% | 0.00% | 0.00% |
| DataFusion execution, inclusive | 0.00% | 8.17% | 0.00% | 7.50% |
| Native numeric formatting, inclusive | 0.00% | 6.20% | 0.00% | 5.93% |
| DataFusion `to_char` | 0.00% | 6.11% | 0.00% | 5.89% |
| Flink timestamp formatting | 2.94% | 0.00% | 2.95% | 0.00% |
| Arrow-backed output access | 0.00% | 12.38% | 0.00% | 12.17% |
| Managed-budget callbacks | 0.00% | 0.05% | 0.00% | 0.17% |
| Garbage collection | 2.41% | 2.25% | 2.03% | 2.34% |
| Native artifact loading | 0.00% | 0.61% | 0.00% | 0.52% |
| JIT compilation | 9.97% | 13.47% | 10.09% | 13.23% |

CPU sample totals in table order are 6,757, 6,502, 7,188, 7,082.
The corresponding Java allocation sample counts are 76,429 / 45,900 / 76,961 / 45,716;
these are sampled events, not allocated-byte totals or native heap measurements.

The complete stacks show source event construction, deterministic string generation and RowData
copies as substantial costs. Native formatting stacks resolve through DataFusion `to_char` to
chrono format parsing/rendering. The direct-calendar path removes most of the prior adaptation
work for these inputs, while format rendering remains real compute. Arrow output-view access also
remains visible. The supplied blackhole boundary still consumes the output normally; this work
is not bypassed to improve the benchmark. A few stacks have unresolved symbols (at most four CPU
samples per twenty-million-event process), and shared-library leaves without function names remain;
no attribution to their internal functions is claimed.

Two-million-event profiles attribute 2.43% / 2.34% of StreamFusion CPU samples to numeric formatting
(memory / RocksDB configured), versus 4.22% / 3.64% in the earlier implementation. These are
cross-run sample shares, not isolated-kernel speedup measurements. Startup/JIT remains a large part
of those shorter processes. Twenty-million-event profiles finish on both configurations without a
capacity error. Native plan/Calc invocation evidence is:

| Events / run type | Memory native plan / Calc batches | RocksDB configured native plan / Calc batches |
| --- | --- | --- |
| 1,000,000 / measured range | 138–144 / 138–144 | 144–152 / 144–152 |
| 10,000,000 / measured range | 1254–1264 / 1254–1264 | 1262–1280 / 1262–1280 |
| 2,000,000 / CPU profile | 264 / 264 | 268 / 268 |
| 20,000,000 / CPU profile | 2502 / 2502 | 2528 / 2528 |


## Reproduction and retained evidence

Build/install release artifacts first, then use `NexmarkBlackholeBenchmark` as described in the
[RowData blackhole method](/StreamFusion/benchmarks/rowdata-blackhole/), with `q10`, parallelism four,
and the settings above. Run each engine in its own JVM and alternate the order for at least three
unprofiled pairs. Run longer profiled forks separately.

Local evidence is under `streamfusion-nexmark-benchmarks/target/measurements/q10/8d1d9943/`, divided
by `hashmap`/`rocksdb` and `1000000`, `10000000`, `profiles-2000000`, `profiles-20000000`.
The earlier implementation's million-event measurements and two-million-event profiles are under
`q10/1227c358/`. Evidence includes exact commands, native build metadata, machine/runtime details,
EXPLAIN/logs, every fork, medians/ranges/MAD and profile artifacts. Generated profiles are not checked in.

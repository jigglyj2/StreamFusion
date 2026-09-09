---
title: Q11 SESSION RowData release comparison
description: Ordered session state, DataFusion COUNT, release measurements, and mixed JVM/native profiles.
---

Q11 now accelerates through ordinary whole-plan selection using DataFusion COUNT inside the shared
SESSION runtime and ordered per-session state. On this host, ten-million-event median throughput
is 1.181× Flink in memory and 2.001× on RocksDB, with disjoint timing ranges on both backends.
Flink's RocksDB times vary widely, so the 2.001× ratio is specific to this measured set, not a
stable general speedup. At one million events StreamFusion remains slower in memory; the RocksDB
ranges overlap.

Each cell reports median end-to-end seconds, [min, max], and median absolute deviation (MAD) from
three alternating fresh-JVM pairs. Throughput ratio is Flink median time divided by StreamFusion
median time; greater than one is faster. Every measured StreamFusion fork reports ordinary
acceleration and positive native plan/Calc activity. No forks or outliers were discarded.

| Events | State backend | Flink seconds [range]; MAD | StreamFusion seconds [range]; MAD | Throughput ratio |
| --- | --- | --- | --- | --- |
| 1,000,000 | In-memory | 5.169 [5.111, 5.228]; 0.058 | 5.669 [5.608, 5.730]; 0.061 | 0.912× |
| 1,000,000 | RocksDB | 6.745 [5.861, 6.850]; 0.105 | 5.897 [5.881, 6.374]; 0.015 | 1.144× |
| 10,000,000 | In-memory | 12.977 [11.840, 13.133]; 0.157 | 10.989 [10.656, 11.297]; 0.308 | 1.181× |
| 10,000,000 | RocksDB | 24.481 [17.923, 42.927]; 6.557 | 12.233 [12.065, 12.238]; 0.005 | 2.001× |

## Method and correctness

The clean measured checkout is `4cdb1b299b2b6e85e26838dc4b103c3904640c6e`. Both engines use the
same bounded official Nexmark RowData source, Q11 SQL and unmodified Flink blackhole sink.
End-to-end timing includes setup, EXPLAIN, native initialization, cluster startup, execution and
cleanup; it excludes JVM launch and argument parsing. These are not steady-state rates.

The host is WSL2 Linux, Intel Core i7-12650H, 16 reported logical CPUs, approximately 7.6 GiB RAM
and 2 GiB swap. Both engines use OpenJDK 24.0.2+12-54, a fixed 1 GiB heap, 2 GiB maximum direct memory,
`ActiveProcessorCount=4`, and the Arrow-required `java.nio` opening. Flink settings are parallelism
four, 1 GiB managed memory, consumer weights `OPERATOR:90,STATE_BACKEND:10,PYTHON:30`, disabled
mini-batching, one-second exactly-once filesystem checkpoints and no restart. Compilation does not
overlap measurements. Q11 exercises session state on both backends.

Rust 1.94 uses release optimization, `target-cpu=native`, frame pointers and `debuginfo=1`.
Separate DWARF files retain release optimization; packaged CPU metadata is verified at load.
Artifact SHA-256 values:

- Core: `6de0a3f63137161a5b6d1ad4a0403bf9446c2acde8e792e52aa7571dbf50745c`.
- RocksDB: `118e0d6c10fb0ed24ef99d44e0bc8eaf7d3f81402554bf5b9848ba806cf2e85a`.

`NexmarkQ11ProductionIT` separately checks the complete collected changelog and materialized
result bytes at 10,000 events, parallelism one and four, on both backends. It requires ordinary
acceleration and positive shared plan/Calc activity. Blackhole timing itself provides no
output-parity evidence.

Controlled-arrival tests compare exact ordered RowData and control bytes against a SQL-generated
Flink SESSION operator. They cover inclusive endpoint merging, older events joining a live session,
isolated late events, multiple bridges, nullable keys, negative timestamps, different batch sizes,
complete registered Flink metrics and terminal paths. Generated ordinary SQL tests cover different
gaps, window rowtime output and feeding that rowtime into a second window. Recovery tests cover
canonical backend switches, aligned/unaligned checkpoints, rescaling one-to-two-to-one, incremental
RocksDB SST reuse and exactly-once replay of captured Arrow IPC channel data. Native tests also
migrate the retained legacy interval-list format and check budget denial and output ownership.

## General implementation and optimization

SESSION COUNT uses DataFusion grouped update/merge kernels inside the shared native window plan.
Custom code assigns Flink merging namespaces in arrival order: sorting events first would change
which expired events Flink accepts. Each persisted interval has an Arrow-row-encoded end key,
a framed partition prefix and a versioned accumulator value. Flink BinaryRow identity determines
the key group independently. Memory and RocksDB load relevant ordered ranges before batch computation
and flush changed intervals together; they do not rewrite a growing whole-partition value or retain
append-only input events. Adjacent Calcs exchange shared Arrow buffers directly with the window.

The initial release at `06160250` had million-event median throughput ratios of 0.888× Flink in
memory and 0.981× on RocksDB. Separate two-million-event profiles showed repeated JNI budget
admission beneath small state-range pages. The final implementation grows decoded-state workspace
in 64 KiB chunks, retrying the exact required size if optional headroom does not fit. A regression
loads 1,024 existing partitions on each backend with fewer than 64 host growth calls across the
workspace, state and timers. It also verifies exact-fit admission and complete credit release.
This reduces budget-call frequency without changing Flink's allowance or query semantics. The
short-run results do not establish a throughput improvement from this change alone.

The subsequent `6f2be676` twenty-million-event RocksDB profile attributed 37.78% of all process
CPU samples to ordered state-range access. A completed nonempty range still required another
component call, iterator and Arrow C Data exchange to discover an empty final page. The final
RocksDB implementation returns optional versioned completion metadata on the existing scan reply.
It detects exhaustion using the iterator already positioned after the last emitted entry, including
full final pages. Older ABI-8 components retain conservative pagination, and invalid markers fail
explicitly. This general scan improvement changes neither persisted encodings nor the memory
allowance. Range/page boundary tests, a C Data metadata round-trip, the previous component and
SESSION recovery/parity tests cover the change. Distinct partition ranges still have separate scans.

Admission remains explicit: append-only input, one BIGINT partition key, unfiltered COUNT(*),
TIMESTAMP(3) event time, synchronous state and disabled mini-batching. Other SESSION calls, key
shapes and time semantics retain whole-plan fallback. No query name, particular gap, source
cardinality or sink behavior is special-cased.

## Mixed JVM/native profiles

Separate two- and twenty-million-event forks use async-profiler 4.5 CPU sampling at 10 ms,
Java non-safepoint sampling, native DWARF unwinding and JFR output, plus Java allocation sampling
at 2 MiB. Their timings are excluded from the throughput table. Per-engine flame graphs,
collapsed stacks, JFR files, allocation stacks and differential flame graphs are retained.

The following percentages use all process CPU samples in the twenty-million-event profiles.
Categories are inclusive and overlap: source polling contains chained downstream work, JNI
contains native computation, and DataFusion stream adapters contain state access as well as
aggregate kernels. The DataFusion row therefore does not isolate arithmetic. Zero samples do
not prove zero cost, particularly for inlined timer/firing and sink code.

| CPU category | Flink memory | StreamFusion memory | Flink RocksDB | StreamFusion RocksDB |
| --- | --- | --- | --- | --- |
| Source polling, inclusive | 51.86% | 61.02% | 33.13% | 39.30% |
| Row copying | 22.33% | 13.11% | 14.08% | 8.19% |
| RowData → Arrow writes | 0.00% | 5.53% | 0.00% | 3.12% |
| Arrow C Data / JNI, inclusive | 0.00% | 13.99% | 0.00% | 41.92% |
| Native plan lowering | 0.00% | 0.01% | 0.00% | 0.00% |
| DataFusion execution, inclusive | 0.00% | 11.55% | 0.00% | 37.04% |
| Native SESSION, inclusive | 0.00% | 10.15% | 0.00% | 37.79% |
| SESSION assignment | 0.00% | 1.64% | 0.00% | 1.17% |
| DataFusion grouped-compute adaptation | 0.00% | 0.70% | 0.00% | 0.45% |
| Native ordered state ranges | 0.00% | 1.56% | 0.00% | 29.98% |
| State write stacks | 0.00% | 0.70% | 12.23% | 1.91% |
| Native timers | 0.00% | 0.95% | 0.00% | 0.82% |
| Flink SESSION, inclusive | 11.79% | 0.00% | 50.86% | 0.00% |
| RocksDB, inclusive | 0.00% | 0.00% | 44.31% | 36.41% |
| Arrow-backed output access | 0.00% | 0.24% | 0.00% | 0.08% |
| Managed-budget callbacks | 0.00% | 0.40% | 0.00% | 0.32% |
| Garbage collection | 15.12% | 2.27% | 2.47% | 1.36% |
| Native artifact loading | 0.00% | 0.57% | 0.00% | 0.38% |
| JIT compilation | 9.55% | 15.36% | 6.68% | 10.14% |

CPU sample totals in table order are 10,658 / 6,967 / 17,468 / 11,655. Corresponding Java allocation
sample counts are 67,583 / 28,075 / 72,906 / 28,275; these are sampled events, not allocated-byte
totals or native heap measurements.

The complete stacks show substantial source event construction, deterministic string generation,
`Math.floorMod`, RowData copies and garbage collection. The memory run spends 15.12% of Flink CPU
samples in GC versus 2.27% for StreamFusion; that difference is observational, not an isolated causal
measurement. The RocksDB native path resolves from the Arrow C Stream edge through the shared
DataFusion/window stream, ordered range access and the component ABI to RocksDB seeks, block
loading/decompression and Arrow schema import/export. The final ordered-range share is 29.98%,
versus 37.78% in the earlier `6f2be676` profile. This supports removing terminal probes as a general
improvement, but cross-run sample shares do not quantify an isolated scan speedup. Ordered range
access remains the largest native cost. No query-specific key cache, budget increase, source or
blackhole bypass was introduced.

The earlier ten-million-event three-pair set at `6f2be676` had median throughput ratios of 1.113×
in memory and 1.053× on RocksDB. Its RocksDB timing ranges overlapped widely. Both the earlier
and final measurements are retained; Flink's changing dispersion prevents attributing the final
2.001× ratio entirely to the scan change.

One memory StreamFusion CPU sample has an unknown-stack marker. Shared-library leaves without
function names also remain, notably 1,218 samples in Flink's RocksDB JNI library and 1,433 in libc
for the StreamFusion RocksDB process. Their internal functions are not attributed. Arrow output
views and the normal sink boundary remain in use, even though this aggregate emits far fewer
records than it consumes.

The shorter two-million-event profiles attribute 3.17% / 14.03% of StreamFusion CPU samples to
SESSION and 0.58% / 11.25% to state ranges (memory / RocksDB). JIT compilation remains 39.53% /
35.01% of those processes. All twenty-million-event profiles finish without a capacity failure.
Native invocation evidence is:

| Events / run type | Memory native plan / Calc batches | RocksDB native plan / Calc batches |
| --- | --- | --- |
| 1,000,000 / measured range | 434–452 / 142–148 | 423–454 / 138–146 |
| 10,000,000 / measured range | 3796–3826 / 1252–1262 | 3842–3856 / 1264–1268 |
| 2,000,000 / CPU profile | 800 / 264 | 802 / 262 |
| 20,000,000 / CPU profile | 7575 / 2500 | 7640 / 2508 |

## Reproduction and retained evidence

Build/install release artifacts first, then use `NexmarkBlackholeBenchmark` as described in the
[RowData blackhole method](/StreamFusion/benchmarks/rowdata-blackhole/), with `q11`, parallelism four,
and the settings above. Run each engine in its own JVM and alternate the order for at least three
unprofiled pairs. Run longer profiled forks separately.

Local evidence is under `streamfusion-nexmark-benchmarks/target/measurements/q11/4cdb1b29/`, divided
by `hashmap`/`rocksdb` and `1000000`, `10000000`, `profiles-2000000`, `profiles-20000000`.
Earlier measurements and profiles are under `q11/06160250/` and `q11/6f2be676/`. Evidence includes
exact commands, native build metadata, machine/runtime details, EXPLAIN/logs, every fork,
medians/ranges/MAD and profile artifacts. Generated profiles are not checked in.

---
title: Q21 RowData release comparison
description: DataFusion channel extraction, exact Flink parity and separate-JVM release measurements.
---

Original Q21 now passes ordinary whole-plan admission on both backend configurations.
Release `4da78e0659f12d33e4c6fca6c846366ec77644c6` preserves its original CASE, LOWER,
REGEXP_EXTRACT predicate, projection and blackhole schema. There is no channel-name, URL-layout
or query-specific execution branch. Q21 is stateless: configuring RocksDB here does not
establish RocksDB state-performance or recovery-capacity evidence.

The planner retains the requested regex capture and converts other groups to noncapturing
groups. This prevents Arrow's omission of unmatched captures from shifting Java's group indices.
Rust checks the projected grammar, then composes DataFusion `regexp_match` and `array_element`.
Both computation steps use DataFusion; the custom wrapper provides Flink semantics and ownership.
The verified literal-pattern subset and precise fallback conditions are documented under
[projections](/StreamFusion/operators/select-where/projections/).

One coarse reservation covers bounded compilation/cache workspace, growing Arrow string/list
buffers, gathering and scalar broadcasting. It shrinks to the actual output and survives native
consumers and slices. An initial allowance based on the unrestricted compiler's rejection limit
was unnecessarily large: maximal-pattern probes within the supported grammar peaked below
0.5 MiB. The final policy keeps 4 MiB compilation headroom plus two 2 MiB DFA caches and reserves
growing payloads separately. No Flink budget was enlarged and no reservation check was bypassed.

## Unprofiled release measurements

Each case uses three fresh-JVM pairs in F/SF, SF/F, F/SF order. Times are end-to-end seconds;
MAD is median absolute deviation. Ratios divide Flink median time by StreamFusion median time.
Every fork is retained; profiled timings are excluded.

| Backend configuration | Events | Flink median [range]; MAD (s) | StreamFusion median [range]; MAD (s) | Throughput ratio |
| --- | ---: | ---: | ---: | ---: |
| hashmap | 1,000,000 | 5.308 [5.199, 5.418]; 0.108 | 5.735 [5.733, 5.877]; 0.002 | 0.925× |
| hashmap | 10,000,000 | 15.454 [14.106, 15.588]; 0.134 | 11.752 [11.450, 12.992]; 0.303 | 1.315× |
| rocksdb | 1,000,000 | 5.459 [5.434, 5.464]; 0.005 | 5.703 [5.635, 5.871]; 0.069 | 0.957× |
| rocksdb | 10,000,000 | 16.355 [16.243, 16.985]; 0.112 | 13.949 [13.895, 14.242]; 0.054 | 1.172× |

All three 1M pairs favor Flink in each configuration, with disjoint ranges. All three 10M
pairs favor StreamFusion in each configuration, also with disjoint ranges. At 10M events,
median throughput is 1.315× Flink in memory and 1.172× with RocksDB configured. The smaller
cases remain slower at 0.925× and 0.957×. These local results do not establish a universal
speedup or a performance ceiling. Every planned measured and profiled fork completed.

## Correctness and acceleration

The implementation checkpoint passed 13 focused native checks, 32 Java unit checks and nine
original-query integration cases. Native tests cover large-buffer refusal before computation,
output ownership through the last slice, scalar broadcasts, empty batches, conditional input
selection, projected-pattern validation and maximal supported pattern workspace. Generated SQL
checks compare complete ordered RowData changelog bytes across all four RowKinds, Unicode,
line terminators, null/empty values, missing captures, group zero, alternatives, nested extraction,
CASE and filters, including wide payloads.

Three Flink-generated Calc stages are compared with one fused native execution tree across
empty, single-row, seven-row and 3,001-row batches. The harness compares complete ordered
changelogs and timestamp envelopes, registered metric surfaces, latency, watermarks/status,
pre-barrier callbacks and terminal paths. Adapted upstream regex SQL cases check values and
nullable result types. Additional cases preserve the pinned Flink 2.3 behavior for invalid
patterns: whole-plan Flink fallback returns null. The newer reference checkout's planning-time
rejection of invalid literals is not substituted for that pinned runtime behavior.

Original SQL and the catalog SELECT are equal. Collecting/blackhole validation at 50,000 events
passes on both configurations and parallelism 1/4, including full output/result digests, positive
equal blackhole counts and ordered changelog digests at parallelism one. Parallel source readers
may interleave independent rows differently. Fixed-arrival operator tests compare every ordered
changelog byte. No benchmark-scale byte-parity claim is inferred from output counts alone.

Every completed native fork reports whole-plan acceleration and positive native-plan/Calc
activity; Flink reports zero native activity. Blackhole counts match in every completed pair.

| Backend configuration | Events | Kind | Blackhole records per engine/fork | Native plan / Calc batches |
| --- | ---: | --- | ---: | --- |
| hashmap | 1,000,000 | measured, three forks | 876,394 | 144 / 144, 146 / 146, 148 / 148 |
| hashmap | 10,000,000 | measured, three forks | 8,763,345 | 1262 / 1262, 1270 / 1270, 1264 / 1264 |
| hashmap | 20,000,000 | profile only | 17,528,381 | 2506 / 2506 |
| rocksdb | 1,000,000 | measured, three forks | 876,394 | 144 / 144, 148 / 148, 152 / 152 |
| rocksdb | 10,000,000 | measured, three forks | 8,763,345 | 1274 / 1274, 1272 / 1272, 1262 / 1262 |
| rocksdb | 20,000,000 | profile only | 17,528,381 | 2514 / 2514 |

## Separate mixed JVM/native profiles

Both configurations have complete 20M-event profile pairs, longer than the largest measured
cases. JFR, CPU/allocation collapsed stacks, per-engine flame graphs and differential flame
graphs are retained locally. Shares below use all process CPU samples and are inclusive and
overlapping. JNI includes downstream native execution; source polling includes the chained
pipeline and must not be read as source-generation cost alone. Zero means no matching sample.

| Inclusive CPU sample category | hashmap 20M F / SF (%) | rocksdb 20M F / SF (%) |
| --- | ---: | ---: |
| native extraction wrapper | 0.000 / 8.137 | 0.000 / 7.921 |
| DataFusion/Arrow regex | 0.000 / 7.605 | 0.000 / 7.178 |
| native regex compilation | 0.000 / 0.504 | 0.000 / 0.561 |
| native regex matching | 0.000 / 6.120 | 0.000 / 5.692 |
| DataFusion list-element extraction | 0.000 / 0.518 | 0.000 / 0.743 |
| Java regex | 25.213 / 0.056 | 25.108 / 0.028 |
| Java regex compilation | 7.794 / 0.042 | 7.518 / 0.000 |
| memory budget callbacks | 0.000 / 0.154 | 0.000 / 0.112 |
| garbage collection | 1.779 / 1.905 | 1.850 / 1.963 |
| row copy | 23.846 / 16.064 | 25.192 / 16.459 |
| RowData-to-Arrow writing | 0.000 / 8.221 | 0.000 / 8.075 |
| Arrow C Data / JNI, inclusive | 0.000 / 16.148 | 0.000 / 15.940 |
| native plan lowering | 0.000 / 0.000 | 0.000 / 0.000 |
| DataFusion frames, inclusive | 0.000 / 14.524 | 0.000 / 14.216 |
| DataFusion functions and expressions | 0.000 / 13.347 | 0.000 / 12.926 |
| Arrow gather | 0.000 / 0.000 | 0.000 / 0.000 |
| Arrow output view access | 0.000 / 4.426 | 0.000 / 4.472 |
| Nexmark generator | 1.995 / 2.773 | 2.282 / 2.874 |
| source RowData conversion | 23.198 / 29.678 | 22.563 / 30.142 |
| source deterministic payload generation | 18.252 / 24.314 | 18.095 / 23.693 |
| source polling, including chained pipeline | 84.905 / 75.742 | 85.312 / 76.980 |
| RocksDB, inclusive | 0.000 / 0.000 | 0.000 / 0.000 |
| native artifact loading | 0.000 / 0.560 | 0.000 / 0.799 |
| JIT compilation | 8.566 / 13.599 | 8.296 / 12.842 |

CPU samples (Flink / StreamFusion): hashmap 9,725 / 7,140; RocksDB configured 9,511 / 7,133.
Percentages are not elapsed-time ratios and cannot alone attribute the measured speedup.

Java regex accounts for approximately 25% of Flink samples, including 7.5–7.8% in compilation.
DataFusion/Arrow regex accounts for 7.2–7.6% of native-run samples; native regex compilation
is 0.5–0.6%. Pinned Flink 2.3 compiles its extraction pattern per call, while DataFusion's
scalar-pattern kernel compiles per batch. The supported capture adaptation preserves results
while reusing that vectorized path. List-element extraction stays below 0.8%, memory callbacks
below 0.2%, and native plan lowering has no matching samples. These profiles do not justify
replacing those library kernels or adding another regex cache implementation.

Native-run source RowData conversion accounts for about 30% of samples, including 24% in the
existing deterministic payload generator. Its absolute sample counts are close between engines;
the larger native percentage partly reflects a smaller total. Frequent leaves include string
builder capacity checks, payload mixing and integer modulo. Source polling's 76–77% inclusive
share also contains downstream operators, so it must not be treated as an independent bottleneck.
The original generator and the shared deterministic RowData adapter were preserved in both engines.

Row-copy frames account for about 16% of native samples, RowData-to-Arrow writing for 8%, and
Arrow-backed output access for 4.4–4.5%. Boundary handling remains an opportunity for separate,
general work with ownership and Flink parity checks; those opportunities are not exhausted.
This checkpoint retains the production Arrow/DataFusion path and does not change the source,
blackhole sink or copy semantics to improve a query-specific score.

For RocksDB-configured profiles only, the established launcher loads the verified plugin with
JVM `System.load` before benchmark main for symbol resolution. Measured forks never use that
launcher. Q21 has no keyed native state; these profiles do not measure RocksDB state access.

## Method, artifacts and limits

Both engines use parallelism 4, mini-batching disabled, one-second exactly-once checkpoints,
no restarts, UTC, 1 GiB managed memory and `OPERATOR:70,STATE_BACKEND:70,PYTHON:30`.
JVM flags are `-Xms1g -Xmx1g -XX:MaxDirectMemorySize=2g -XX:ActiveProcessorCount=4`;
no CPU affinity is applied. RocksDB is configured with Flink's default checkpoint settings, but this query has no keyed state.
The benchmark also uses Flink's existing `table.optimizer.multi-join.enabled=false` option in
both engines; it has no join to affect in Q21. No builds run alongside measured or profiled forks.

Timing includes Java-only counter reset, setup, EXPLAIN preflight, native initialization when
selected, cluster startup, execution and cleanup. It excludes JVM launch, argument parsing
and build time. The host is WSL2 Linux, Intel Core i7-12650H, 16 logical CPUs, approximately
7.6 GiB RAM and 2 GiB swap, Java 24.0.2. Native artifacts use release optimization, native
CPU features, frame pointers and profiling symbols without reducing optimization. CPU baseline
fingerprint: `44dd0ad765af32a3`. Async-profiler 4.5 uses CPU sampling at 10 ms, Java non-safepoint
sampling, native DWARF unwinding, JFR output and allocation sampling at 2 MiB.

Verified benchmark JAR artifact SHA-256 values:

- Native runtime: `34fa78f3c6c61e1f6e518157af557fb5c5bf1509a4907bdd63f329b9a3aff03b`.
- RocksDB plugin: `fe1af76cd4e48dc789eca1eb720d1fdea5b67d40f465956401c39a6653f08862`.

Upstream Flink is `c0f8d1a1e09f209885a88f9c19ceb9d9e9870283`, with only the approved
planner/class-loading installation and complete-StreamGraph memory callback. Nexmark is clean
at `6b3646c3baec701f1fa74baf938d235f742e5d3c`. The source is the deterministic RowData adapter
and the sink is unmodified Flink blackhole. No Kafka service or connector benchmark is involved.

Exact commands, metadata, results, counters and profiles are under
`streamfusion-nexmark-benchmarks/target/measurements/q21/4da78e06/`. All planned forks completed. Longer profiles are neither unprofiled performance comparisons nor
proof of state restore capacity. Other deployment sizes and mini-batch mode
are outside this measurement.

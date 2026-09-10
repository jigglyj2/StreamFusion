---
title: Q22 RowData release comparison
description: DataFusion URL splitting, exact Flink parity and separate-JVM release measurements.
---

Original Q22 passes ordinary whole-plan admission on both backend configurations at
`0a1d8f3f08042908a882138673a9170596e90a37`. The catalog preserves the original SELECT and
blackhole schema. Q22 is stateless: configuring RocksDB here does not establish RocksDB
state-performance or recovery-capacity evidence.

The previous `SPLIT_INDEX` implementation used a handwritten string loop despite documentation
promising DataFusion. This checkpoint restores actual DataFusion `string_to_array` and
`array_element` computation. DataFusion conditional expressions adapt Flink's zero-based index,
including negative and maximum INTEGER values. Testing caught that DataFusion rejects the
converted maximum index, while Flink returns null; the adapter excludes it before extraction.
Empty tokens remain distinct from missing fields. `split_part` alone cannot replace this
composition because it returns an empty string for a missing field. The supported delimiter
subset and fallback conditions are documented under
[projections](/StreamFusion/operators/select-where/projections/).

One coarse reservation covers growing split/list buffers, selection, extraction and scalar
broadcasting before kernel execution, then shrinks to output ownership and survives slices.
No Flink budget is enlarged, no reservation is bypassed, and no per-allocation JNI accounting
is introduced. Adjacent Calc stages continue to exchange Arrow batches inside one native tree.

## Unprofiled release measurements

Each case uses three fresh-JVM pairs in F/SF, SF/F, F/SF order. Times are end-to-end seconds;
MAD is median absolute deviation. Ratios divide Flink median time by StreamFusion median time.
Every fork is retained; profiled timings are excluded.

| Backend configuration | Events | Flink median [range]; MAD (s) | StreamFusion median [range]; MAD (s) | Throughput ratio |
| --- | ---: | ---: | ---: | ---: |
| hashmap | 1,000,000 | 5.213 [5.141, 5.239]; 0.026 | 6.128 [5.985, 6.254]; 0.126 | 0.851× |
| hashmap | 10,000,000 | 12.373 [12.156, 12.395]; 0.022 | 12.159 [11.975, 12.250]; 0.092 | 1.018× |
| rocksdb | 1,000,000 | 5.200 [5.176, 5.204]; 0.004 | 6.027 [6.018, 6.067]; 0.009 | 0.863× |
| rocksdb | 10,000,000 | 12.433 [12.392, 12.581]; 0.041 | 12.110 [12.047, 12.926]; 0.063 | 1.027× |

All three 1M pairs favor Flink in each configuration, with disjoint ranges. At 10M events,
StreamFusion has a slightly better median, but ranges overlap and only two of three pairs
favor StreamFusion in each configuration. Treat 1.018× / 1.027× as near parity, not a robust
performance win. Every planned measured and profiled fork completed. These measurements do
not establish a performance ceiling.

## Correctness and acceleration

The checkpoint passed 12 native checks, 13 Java unit checks and nine original-query integration
cases. Native checks cover null/empty/trailing fields, Unicode and multibyte delimiters, INTEGER
extremes, scalar broadcasting, empty batches, conditional selection, refusal before large
allocation, dense-token workspace and ownership through the final output slice. Shared managed
scalar checks also pass.

Generated SQL tests compare complete ordered RowData changelog bytes for three seeds and all
four RowKinds, including nested splits, CASE, filters, wide values and delimiter-dense input.
Adapted cases from Flink 2.3's `ScalarFunctionsTest` cover the admitted string-delimiter behavior
and fallback for integer/null delimiter modes. Existing split parity tests remain enabled.

Three Flink-generated Calc stages are compared with one fused native tree across empty,
single-row, seven-row and 3,001-row batches. The harness checks complete ordered changelogs,
timestamp envelopes, registered metric surfaces, latency, watermarks/status, pre-barrier
callbacks and terminal paths. Its common string-stage machinery is shared with the existing
regex tests; the two expression families retain separate fixtures and both metric tests pass.

Original SQL and the catalog SELECT are equal. Collecting and blackhole validation at 50,000
events passes on both configurations at parallelism 1/4, including output/result digests,
equal positive blackhole counts and ordered changelog digests at parallelism one. Parallel
source readers can interleave independent rows differently; fixed-arrival tests compare every
ordered byte. Counts alone do not establish benchmark-scale byte parity.

Every native fork reports whole-plan acceleration and positive native-plan/Calc activity;
Flink reports zero native activity. Blackhole counts match in every pair.

| Backend configuration | Events | Kind | Blackhole records per engine/fork | Native plan / Calc batches |
| --- | ---: | --- | ---: | --- |
| hashmap | 1,000,000 | measured, three forks | 920,000 | 144 / 144, 144 / 144, 150 / 150 |
| hashmap | 10,000,000 | measured, three forks | 9,200,000 | 1260 / 1260, 1276 / 1276, 1266 / 1266 |
| hashmap | 20,000,000 | profile only | 18,400,000 | 2510 / 2510 |
| rocksdb | 1,000,000 | measured, three forks | 920,000 | 148 / 148, 148 / 148, 148 / 148 |
| rocksdb | 10,000,000 | measured, three forks | 9,200,000 | 1268 / 1268, 1268 / 1268, 1264 / 1264 |
| rocksdb | 20,000,000 | profile only | 18,400,000 | 2506 / 2506 |

## Separate mixed JVM/native profiles

Both configurations have complete 20M-event profile pairs, longer than the measured cases.
JFR, CPU/allocation collapsed stacks, per-engine flame graphs and differential flame graphs
are retained locally. Shares below use all process CPU samples and are inclusive and overlapping.
JNI includes downstream native execution; source polling includes the chained pipeline.
Zero means no matching sample. Percentages are not elapsed-time ratios and cannot alone
attribute a speedup.

| Inclusive CPU sample category | hashmap 20M F / SF (%) | rocksdb 20M F / SF (%) |
| --- | ---: | ---: |
| native split adapter | 0.000 / 7.442 | 0.000 / 7.613 |
| DataFusion string splitting | 0.000 / 6.179 | 0.000 / 6.324 |
| DataFusion list-element extraction | 0.000 / 1.005 | 0.000 / 1.084 |
| Java string splitting | 9.118 / 0.000 | 8.647 / 0.000 |
| memory budget callbacks | 0.000 / 0.163 | 0.000 / 0.123 |
| garbage collection | 2.668 / 2.336 | 2.822 / 2.058 |
| row copy | 32.991 / 19.324 | 34.124 / 20.316 |
| RowData-to-Arrow writing | 0.000 / 8.514 | 0.000 / 8.573 |
| Arrow C Data / JNI, inclusive | 0.000 / 10.932 | 0.000 / 10.768 |
| native plan lowering | 0.000 / 0.000 | 0.000 / 0.014 |
| DataFusion frames, inclusive | 0.000 / 9.112 | 0.000 / 9.259 |
| DataFusion functions and expressions | 0.000 / 8.175 | 0.000 / 8.368 |
| scalar-to-array conversion | 0.000 / 0.081 | 0.000 / 0.096 |
| Arrow gather | 0.000 / 0.000 | 0.000 / 0.000 |
| Arrow output view access | 0.000 / 7.998 | 0.000 / 7.709 |
| Nexmark generator | 2.906 / 3.096 | 2.745 / 2.936 |
| source RowData conversion | 30.636 / 30.839 | 29.622 / 29.534 |
| source deterministic payload generation | 14.241 / 24.810 | 23.990 / 24.019 |
| source polling, including chained pipeline | 83.104 / 75.638 | 82.207 / 75.103 |
| RocksDB, inclusive | 0.000 / 0.000 | 0.000 / 0.000 |
| native artifact loading | 0.000 / 0.557 | 0.000 / 0.823 |
| JIT compilation | 9.193 / 13.770 | 9.468 / 14.321 |

CPU samples (Flink / StreamFusion): hashmap 7,984 / 7,364; RocksDB configured 7,795 / 7,290.

The native split adapter, including both library kernels, accounts for 7.4–7.6% of samples.
DataFusion splitting contributes 6.2–6.3%, with leaves in AVX2 `memchr`, delimiter iteration and
Arrow string builders; extraction contributes about 1%. Flink's Java splitting contributes
8.6–9.1%, with leaves in string searching, array copying and token-list growth. Memory callbacks
remain below 0.2%, scalar broadcasting below 0.1%, and plan lowering has zero/one sample.
These profiles do not justify another custom splitting algorithm or elaborate temporary-memory
instrumentation.

Larger native-run shares are source RowData conversion at 29.5–30.8%, row-copy frames at
19.3–20.3%, RowData-to-Arrow writing at about 8.5%, and Arrow-backed output access at 7.7–8.0%.
The source includes deterministic payload generation. Its inlined-frame attribution differs
between forks, so subcategory percentages must not be read as a source-algorithm speedup.
Output-path leaves include interface dispatch, Arrow string access, Flink serializer copying
and the existing sink writer. Source-poll percentages include this chained work.

Further general opportunities remain in boundary handling and avoiding repeated materialization
of equivalent split results. They would require ownership, memory and Flink-parity validation;
this checkpoint does not claim those opportunities are exhausted. It keeps the DataFusion
computation, original generator, shared deterministic source adapter, unmodified blackhole sink
and required Flink copy behavior. No URL-layout shortcut or private dependency modification was
introduced to turn the near-parity result into a benchmark win.

For RocksDB-configured profiles only, the established launcher loads the verified plugin with
JVM `System.load` before benchmark main for symbol resolution. Measured forks never use that
launcher. There are no RocksDB CPU samples or keyed-state operations in this stateless query.

## Method, artifacts and limits

Both engines use parallelism 4, mini-batching disabled, one-second exactly-once checkpoints,
no restarts, UTC, 1 GiB managed memory and `OPERATOR:70,STATE_BACKEND:70,PYTHON:30`.
JVM flags are `-Xms1g -Xmx1g -XX:MaxDirectMemorySize=2g -XX:ActiveProcessorCount=4`;
no CPU affinity is applied. RocksDB is configured with Flink's default checkpoint settings, but this query has no keyed state.
The benchmark also uses Flink's existing `table.optimizer.multi-join.enabled=false` option in
both engines; it has no join to affect in Q22. No builds run alongside measured or profiled forks.

Timing includes Java-only counter reset, setup, EXPLAIN preflight, native initialization when
selected, cluster startup, execution and cleanup. It excludes JVM launch, argument parsing
and build time. The host is WSL2 Linux, Intel Core i7-12650H, 16 logical CPUs, approximately
7.6 GiB RAM and 2 GiB swap, Java 24.0.2. Native artifacts use release optimization, native
CPU features, frame pointers and profiling symbols without reducing optimization. CPU baseline
fingerprint: `44dd0ad765af32a3`. Async-profiler 4.5 uses CPU sampling at 10 ms, Java non-safepoint
sampling, native DWARF unwinding, JFR output and allocation sampling at 2 MiB.

Verified benchmark JAR artifact SHA-256 values:

- Native runtime: `4bb5f0c035898f29490958d88f1821216cd3ffb984e36f3fb10976e95708e63a`.
- RocksDB plugin: `fe1af76cd4e48dc789eca1eb720d1fdea5b67d40f465956401c39a6653f08862`.

Upstream Flink is `c0f8d1a1e09f209885a88f9c19ceb9d9e9870283`, with only the approved
planner/class-loading installation and complete-StreamGraph memory callback. Nexmark is clean
at `6b3646c3baec701f1fa74baf938d235f742e5d3c`. The source is the deterministic RowData adapter
and the sink is unmodified Flink blackhole. No Kafka service or connector benchmark is involved.

Exact commands, metadata, results, counters and profiles are under
`streamfusion-nexmark-benchmarks/target/measurements/q22/0a1d8f3f/`. All planned forks completed. Longer profiles are neither unprofiled performance comparisons nor
proof of state restore capacity. Other deployment sizes and mini-batch mode
are outside this measurement.

---
title: Q19 RowData release comparison
description: Original auction Top-10, DataFusion selection, shared-state parity and release measurements.
---

Original Q19 passes ordinary whole-plan admission on both state backends. Release
`b5dece9a3b7aba0cb4c1644096602a43a68e46d2` runs the unchanged price-descending auction Top-10
SELECT and original sink schema, including the rank number and no primary key. The catalog's
previous extra tie-breakers were removed because they changed the query's result semantics.

DataFusion orders Arrow-encoded sort keys and persisted arrival sequences once per batch, then
performs bounded selection over fixed-width priorities for each arrival. StreamFusion adapts
those selections to Flink's exact intermediate rank/changelog transitions. Wide payloads stay
outside repeated selection kernels. Adjacent Calc and Top-N stages exchange Arrow batches in
one native tree with stable metric identities and no intermediate JVM transfer.

State metadata is read in a batch, ordered candidate ranges are loaded before computation,
and dirty state is flushed in one batch. Only new final candidates are encoded; displaced entries
are deleted, unchanged payloads stay in the backend, and losing-only batches perform no writes.
Large retained-state workspace and amplified output buffers are reserved before state writes.
These changes apply to the verified constant-range operator subset, including offsets; there
is no Q19-specific rank limit, execution branch, runtime setting or upstream operator patch.

## Unprofiled release measurements

Each completed case uses three fresh-JVM pairs, alternating F/SF, SF/F, F/SF. Times are
end-to-end seconds; MAD is median absolute deviation. Ratios divide Flink median time by
StreamFusion median time, so values above 1 favor StreamFusion. All forks are retained;
profile timings are excluded.

| Backend | Events | Flink median [range]; MAD (s) | StreamFusion median [range]; MAD (s) | Throughput ratio |
| --- | ---: | ---: | ---: | ---: |
| hashmap | 1,000,000 | 11.062 [5.743, 12.225]; 1.162 | 10.470 [6.776, 15.845]; 3.693 | 1.057× |
| hashmap | 2,000,000 | 20.287 [8.061, 34.873]; 12.225 | 8.017 [7.730, 8.171]; 0.154 | 2.530× |
| rocksdb | 1,000,000 | 7.440 [7.298, 16.556]; 0.142 | 6.941 [6.840, 27.227]; 0.101 | 1.072× |
| rocksdb | 4,000,000 | 46.096 [45.094, 52.212]; 1.003 | 12.139 [11.971, 42.070]; 0.167 | 3.798× |

At 1M events, median throughput is close on both backends and the ranges overlap widely.
Only one of the three in-memory pairs and two of the RocksDB pairs favor StreamFusion.
The 27.227-second native RocksDB fork and every other slow fork remain in the results.
At 2M in-memory events, all three pairs favor StreamFusion, but Flink varies from 8.061 to
34.873 seconds and the engine ranges overlap. The 2.530× median ratio needs that qualification.
At 4M RocksDB events, all three pairs favor StreamFusion and the ranges are disjoint. Its
3.798× median ratio includes a native fork lasting 42.070 seconds, versus approximately
12 seconds for the other two. These local measurements do not establish a universal speedup
or a performance ceiling.

The first 4M in-memory Flink fork failed with a TaskManager heartbeat timeout and produced no
benchmark result. That case stopped before a native fork. This is not an established OOM
diagnosis or a native capacity result; no ratio is reported for it. The completed 2M in-memory
case uses the same heap, configuration and release artifacts.

A separate 3M in-memory profile completed on Flink but failed on StreamFusion when its native
state consumer exhausted its existing Flink allowance: an additional 810,777 bytes was denied
with 117,165,524 bytes already reserved and 8,207 bytes available. This is a demonstrated native
budget limit for that fork, not an accelerated 3M capacity claim. Its incomplete pair supplies
no performance ratio. The budget and reservation checks were preserved.


## Correctness and acceleration

The final shared-runtime prerequisite passed 27 native checks and 109 focused Java checks.
Generated comparisons against Flink's `AppendOnlyTopNFunction` cover complete changelog bytes,
nullable composite ordering, cutoff ties, offsets, all output flags, multiple batch sizes and
both backends. Shared-runtime tests cover complete metric surfaces and deterministic values,
latency semantics, Arrow topology, output ownership, large-history/output memory refusal,
old-state migration, canonical backend switches, aligned/unaligned checkpoints, 1-to-2-to-1
rescaling, incremental SST reuse and actual two-channel Arrow IPC replay with full Top-10 sets.

The admission checkpoint adds one planner test and nine original-SQL planning/production
integration cases. At 50,000 events, materialized result bytes match on both backends and
parallelism 1/4. Single-source jobs also match every ordered changelog byte and blackhole record
count. Independent parallel channels can change intermediate rankings; those jobs do not assert
identical transient changelogs. Fixed-arrival operator tests do compare the complete changelog
and timestamp envelopes. No additional sort keys or source changes resolve ties artificially.

See [Top-N](/StreamFusion/operators/top-n/) for supported types, memory, metrics, state and
fallback contracts. Variable ranges, global rank/LIMIT, other strategies, unsupported types,
TTL, asynchronous state and mini-batching remain gated. Whole-plan fallback is retained.

Every completed native fork reports whole-plan acceleration and positive native activity;
Flink reports zero native activity. Blackhole counts match between engines in every completed
pair. These counts supplement separate collecting validation; they are not a byte-parity proof
at benchmark scale.

| Backend | Events | Kind | Blackhole records per engine/fork | Native plan / Calc batches |
| --- | ---: | --- | ---: | --- |
| hashmap | 1,000,000 | measured, three forks | 1,452,163 | 434 / 142, 416 / 136, 422 / 138 |
| hashmap | 2,000,000 | measured, three forks | 2,904,701 | 800 / 264, 786 / 258, 782 / 258 |
| hashmap | 2,000,000 | profile only | 2,904,701 | 804 / 264 |
| hashmap | 2,500,000 | profile only | 3,622,199 | 986 / 322 |
| rocksdb | 1,000,000 | measured, three forks | 1,452,163 | 464 / 152, 440 / 144, 455 / 148 |
| rocksdb | 4,000,000 | measured, three forks | 5,795,051 | 1618 / 530, 1579 / 520, 1590 / 518 |
| rocksdb | 2,000,000 | profile only | 2,904,701 | 820 / 268 |
| rocksdb | 8,000,000 | profile only | 11,592,729 | 3071 / 1006 |

## Separate mixed JVM/native profiles

Complete profile pairs use 2.5M in-memory events and 8M RocksDB events, longer than their
largest completed unprofiled comparisons. Earlier 2M profile pairs are retained on both backends.
The incomplete 3M in-memory pair is retained as failure evidence and excluded from the table.
JFR, CPU/allocation collapsed stacks, per-engine flame graphs and differential flame graphs are
retained locally for completed pairs.

Shares use all process CPU samples; categories are inclusive and overlap. JNI includes downstream
execution, and DataFusion frames include stream wrappers around custom state adaptation. Zero
means no matching sample, not zero cost. Percentages from different sample totals are not
elapsed-time ratios and do not independently attribute the measured speedup to one change.

| Inclusive CPU sample category | hashmap 2.5M F / SF (%) | rocksdb 8M F / SF (%) |
| --- | ---: | ---: |
| native Top-N | 0.000 / 12.357 | 0.000 / 18.121 |
| DataFusion Top-N selection adapter | 0.000 / 5.355 | 0.000 / 6.976 |
| DataFusion/Arrow sort kernels | 0.000 / 3.612 | 0.000 / 4.767 |
| ordered-state write helpers | 0.000 / 1.616 | 0.000 / 1.598 |
| memory budget callbacks | 0.000 / 0.760 | 0.000 / 0.584 |
| garbage collection | 14.710 / 3.169 | 4.050 / 2.154 |
| row copy | 25.411 / 10.837 | 25.677 / 12.868 |
| RowData-to-Arrow writing | 0.000 / 1.394 | 0.000 / 1.807 |
| Arrow C Data / JNI, inclusive | 0.000 / 13.625 | 0.000 / 34.088 |
| native plan lowering | 0.000 / 0.000 | 0.000 / 0.014 |
| DataFusion frames, inclusive | 0.000 / 11.977 | 0.000 / 19.038 |
| DataFusion functions and expressions | 0.000 / 0.253 | 0.000 / 0.514 |
| Arrow gather | 0.000 / 2.123 | 0.000 / 2.640 |
| Arrow output view access | 0.000 / 6.559 | 0.000 / 7.935 |
| source polling, inclusive | 26.860 / 20.722 | 30.722 / 26.098 |
| RocksDB, inclusive | 0.000 / 0.000 | 20.314 / 19.427 |
| RocksDB index reads | 0.000 / 0.000 | 0.400 / 5.753 |
| RocksDB decompression | 0.000 / 0.000 | 2.523 / 5.892 |
| native artifact loading | 0.000 / 1.236 | 0.000 / 0.750 |
| JIT compilation | 23.285 / 32.224 | 11.085 / 15.981 |

CPU samples (Flink / StreamFusion): hashmap 4,140 / 3,156; RocksDB 9,752 / 7,196.

DataFusion Top-N selection accounts for 5.355% / 6.976% of native-run samples (hashmap /
RocksDB), within complete native Top-N shares of 12.357% / 18.121%. Native leaves include
allocation/release, Arrow selection and Flink changelog adaptation. The profiles do not identify
a dominant compute kernel requiring another algorithm rewrite. The general DataFusion ordering
and bounded-selection path is retained; no handwritten Top-10 specialization replaces it.

Source polling remains substantial at 20.722% / 26.098% in native runs. Row copying accounts
for 10.837% / 12.868%, RowData-to-Arrow writing for 1.394% / 1.807%, and output views for
6.559% / 7.935%. Memory-budget callbacks remain below 0.8%, and native plan lowering is
negligible in these samples. In-memory JVM compilation is still 32.224% of native samples;
Flink GC is 14.710%, versus native-run GC at 3.169%. These observations do not prove the
cause of the wide unprofiled dispersion or of the failed 4M baseline.

At 8M RocksDB events, storage calls account for 20.314% of Flink samples and 19.427% of native
samples. Native index reads and decompression account for 5.753% and 5.892%. Ordered range
access and repeated small selection arrays remain possible targets for future measured work,
within the existing memory/compute rules. This checkpoint does not claim those opportunities
are exhausted. Source generation, row-copying semantics and the unmodified sink were preserved.


For RocksDB profiles only, a launcher loads the same verified plugin through JVM `System.load`
before benchmark main, allowing async-profiler to resolve symbols in the library normally
loaded from Rust via `dlopen`. Measured forks never use that launcher. Profiled throughput is excluded.

## Method, artifacts and limits

Both engines use parallelism 4, mini-batching disabled, one-second exactly-once checkpoints,
no restarts, UTC, 1 GiB managed memory and `OPERATOR:70,STATE_BACKEND:70,PYTHON:30`.
JVM flags are `-Xms1g -Xmx1g -XX:MaxDirectMemorySize=2g -XX:ActiveProcessorCount=4`;
no CPU affinity is applied. RocksDB uses Flink's default full/private-file checkpoint strategy.
No builds run alongside measured or profiled forks.

Timing includes Java-only counter reset, setup, EXPLAIN preflight, native initialization when
selected, cluster startup, execution and cleanup. It excludes JVM launch, argument parsing
and build time. The host is WSL2 Linux, Intel Core i7-12650H, 16 logical CPUs, approximately
7.6 GiB RAM and 2 GiB swap, Java 24.0.2. Native artifacts use release optimization, native
CPU features, frame pointers and profiling symbols without reducing optimization. CPU baseline
fingerprint: `44dd0ad765af32a3`. Async-profiler 4.5 uses CPU sampling at 10 ms, Java non-safepoint
sampling, native DWARF unwinding, JFR output and allocation sampling at 2 MiB.

Verified benchmark JAR artifact SHA-256 values:

- Native runtime: `5ccf5d0e2760506b9123d7cb7ef54d14eb0f9dd5a4f0a3eedd723e7e15113823`.
- RocksDB plugin: `fe1af76cd4e48dc789eca1eb720d1fdea5b67d40f465956401c39a6653f08862`.

Upstream Flink is `c0f8d1a1e09f209885a88f9c19ceb9d9e9870283`, with only the approved
planner/class-loading installation and complete-StreamGraph memory callback. Nexmark is clean
at `6b3646c3baec701f1fa74baf938d235f742e5d3c`. The source is the deterministic RowData adapter
and the sink is unmodified Flink blackhole. No Kafka service or connector benchmark is involved.

Exact commands, metadata, results, counters and profiles are under
`streamfusion-nexmark-benchmarks/target/measurements/q19/b5dece9a/`. Failed cases remain recorded
alongside completed cases. Longer profiles are neither unprofiled performance comparisons nor
proof of restore capacity at their event counts. Other deployment sizes and mini-batch mode
are outside this measurement.

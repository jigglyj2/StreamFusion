---
title: RowData to blackhole measurements
description: Measuring native execution separately from changelog validation.
---

`NexmarkBlackholeBenchmark` runs the deterministic bounded Nexmark RowData source into Flink's
unmodified SQL `blackhole` connector. It uses the same query SQL, schemas, checkpoint settings,
parallelism, and state backend configuration as the collecting RowData harness. StreamFusion
uses ordinary whole-plan admission and its Arrow source and sink boundary adapters.

The blackhole consumes rows without serializing, hashing, retaining, or materializing them.
Its normal Flink changelog negotiation excludes UPDATE_BEFORE. Both engines use that contract;
complete changelog validation belongs in the separate collecting-sink and controlled parity tests.
Blackhole timings alone establish no output correctness claim.

Build the release/native-CPU artifacts and install the reactor dependencies before using the
standalone classpath, as described on the [Nexmark benchmark page](/StreamFusion/benchmarks/nexmark/).
Invoke one query, engine, and backend per JVM:

```shell
java --add-opens=java.base/java.nio=ALL-UNNAMED -cp "$BENCHMARK_CLASSPATH" \
  tech.streamfusion.benchmark.nexmark.NexmarkBlackholeBenchmark \
  1000000 q0 streamfusion hashmap 4
```

The arguments are event count, query, engine (`flink` or `streamfusion`), backend (`hashmap` or
`rocksdb`), optional parallelism (default four), and optional mode (`run` or `explain`). The
`explain` mode prints the ordinary physical plan and admission reasons without starting the job.
A StreamFusion measurement fails when preflight reports fallback or execution records no native
plan batches. It never labels a fallback timing as accelerated performance.

`NEXMARK_BLACKHOLE` reports end-to-end wall time, including table setup, planning preflight, local
cluster startup, native artifact initialization for StreamFusion, execution, and cleanup.
Diagnostic counter reads/resets are Java-only and do not load StreamFusion into the Flink
baseline. JVM process launch and argument parsing are outside the reported timer; the runner
retains separate whole-process wall times. Input-event throughput divides the configured source
event count by that time; it is neither output-row throughput nor a steady-state measurement.
It also reports runtime mode, mini-batching, backend, parallelism, and native invocation counts.

Use identical JVM and Flink settings, at least three unprofiled forks per engine, and alternating
engine order. Record the commit and machine/runtime configuration and report median plus
dispersion. Profile a separate longer fork with mixed JVM/native sampling. Keep raw measurements,
JFR recordings, collapsed stacks, and flame graphs under the benchmark module's `target/`.
Q0–Q2 are stateless, so their RocksDB-labelled runs do not measure RocksDB state performance.
See [query checkpoints](/StreamFusion/benchmarks/query-checkpoints/) for current admission.

## Q3 in-memory measurements, September 7, 2026

These historical tables predate the diagnostic-counter timing correction. The counter reset
initialized StreamFusion's core library in both engines before the reported timer started.
They compare job setup and execution after that initialization; they are not cold native-startup
comparisons. Corrected measurements and profiles are required before the Q3 delivery checkpoint.

Commit `2a73881b` restores consumed-field projection at the RowData source edge of common
native regions. This is a general projection optimization; the query SQL and join algorithm
are unchanged. Q3 uses ordinary binary INNER equi-join admission. RocksDB remains on whole-plan
fallback pending its configuration parity work, so these results are an **in-memory-only
checkpoint**, not completed Q3 coverage across both backends.

Each cell below summarizes three unprofiled, fresh JVMs per engine, ordered Flink/StreamFusion,
StreamFusion/Flink, Flink/StreamFusion. Times include setup, planning, startup, execution and
cleanup. MAD is median absolute deviation. Throughput counts generated input events.

| Input events | Engine | Median seconds | Min–max seconds | MAD seconds | Median events/s |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 million | Flink | 4.910 | 4.823–5.070 | 0.086 | 203,684 |
| 1 million | StreamFusion | 5.276 | 5.232–5.311 | 0.035 | 189,523 |
| 10 million | Flink | 12.677 | 11.447–12.815 | 0.138 | 788,809 |
| 10 million | StreamFusion | 11.871 | 10.563–11.951 | 0.080 | 842,392 |

At one million events StreamFusion takes 7.5% longer; at ten million it has 6.8% higher
input throughput. The longer-run ranges overlap, so this is a modest observed advantage,
not evidence of a universal speedup. Before projection, commit `61e89057` measured 5.733 seconds
for StreamFusion (5.581–5.787, MAD 0.054), versus Flink's 5.027 seconds (4.971–5.057, MAD 0.030)
at one million events. Those earlier measurements used the same method and machine.

The machine was a WSL2 Linux VM on an Intel Core i7-12650H, reporting 16 logical CPUs and
about 7.6 GiB RAM. JVM: OpenJDK 24.0.2+12-54, `-Xms1g -Xmx1g`, 2 GiB maximum direct memory,
`-XX:ActiveProcessorCount=4`, and the Arrow-required `java.nio` opening. Both engines used
parallelism four, 1 GiB Flink managed memory, consumer weights
`OPERATOR:90,STATE_BACKEND:10,PYTHON:30`, disabled mini-batching, and one-second exactly-once
checkpoints. Native libraries used Rust release optimization with `target-cpu=native`, frame
pointers and `debuginfo=1`. Artifact SHA-256 values and the complete commands, classpath, CPU
features and runtime configuration are retained with the measurements.

Every StreamFusion fork reported `Accelerated: yes`. One-million-event runs recorded 841–866
native plan batches and 292–300 Calc batches; ten-million-event runs recorded 7,530–7,546
and 2,524–2,530 respectively. Flink recorded zero native batches. The opt-in collecting-sink
Q3 comparison and generated join/source-boundary tests validate changelog parity separately.

### CPU evidence

Separate ten-million-event forks used async-profiler 4.5, 10 ms CPU sampling, Java non-safepoint
sampling, native DWARF unwinding and JFR output. Their timings are excluded from the table above.
The projected StreamFusion profile recorded 7,528 native plan batches and 2,522 Calc batches.

| Inclusive CPU category | Flink | StreamFusion before projection | StreamFusion after projection |
| --- | ---: | ---: | ---: |
| RowData serializer / binary row copying | 26.33% | 13.30% | 12.02% |
| RowData-to-Arrow writer | 0% | 9.02% | 4.89% |
| Arrow C Data / JNI execution boundary | 0% | 11.72% | 5.65% |
| DataFusion execution | 0% | 7.64% | 2.81% |
| Native streaming join | 0% | 1.94% | 1.44% |
| Arrow-backed row access / sink adapter | 0% | 0.10% | 0.13% |
| Native plan lowering | 0% | 0 sampled | 0 sampled |

Percentages use all process CPU samples: 4,782 for the current Flink fork, 5,247 for the
previous StreamFusion fork and 4,516 for the projected fork. Categories overlap: the JNI
execution boundary includes downstream native computation, and DataFusion includes custom
physical operators. Zero sampled lowering does not mean zero lowering cost. Source polling
and its synchronous downstream path account for 57.40% of Flink samples and 52.06% of projected
StreamFusion samples; JIT compilation accounts for 25.14% and 28.85%. The source and JVM startup
remain substantial costs. Projection reduces both Arrow writing and native work on wide input
batches without changing the execution model.

Raw logs, metadata, summaries, JFR recordings, collapsed CPU/allocation stacks, per-engine
flame graphs, differential flame graphs and the category-matching script remain under
`streamfusion-nexmark-benchmarks/target/measurements/q3-memory/`. Subdirectories are
`one-million`, `profiles`, `projected-one-million`, `projected-ten-million`, and
`projected-profiles`; generated artifacts are intentionally not checked into Git.

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
cluster startup, execution, and cleanup. Input-event throughput divides the configured source
event count by that time; it is neither output-row throughput nor a steady-state measurement.
It also reports runtime mode, mini-batching, backend, parallelism, and native invocation counts.

Use identical JVM and Flink settings, at least three unprofiled forks per engine, and alternating
engine order. Record the commit and machine/runtime configuration and report median plus
dispersion. Profile a separate longer fork with mixed JVM/native sampling. Keep raw measurements,
JFR recordings, collapsed stacks, and flame graphs under the benchmark module's `target/`.
Q0–Q2 are stateless, so their RocksDB-labelled runs do not measure RocksDB state performance.
See [query checkpoints](/StreamFusion/benchmarks/query-checkpoints/) for current admission.

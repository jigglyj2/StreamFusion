---
title: Nexmark benchmarks
description: The north-star Kafka-in/Kafka-out comparison.
---

The north-star benchmark compares StreamFusion with native Flink using the Nexmark streaming workload. Both engines consume the same officially generated Nexmark events from Kafka and write results back to Kafka with exactly-once delivery.

## Benchmark matrix

| State backend | Mini-batching |
| --- | --- |
| In-memory hash map | Off |
| In-memory hash map | On |
| RocksDB | Off |
| RocksDB | On |

Each case reports input-record throughput for native Flink and StreamFusion. Optimization work must keep the implementation simple and remain close to Flink's architecture while preserving result parity.

## Query suite

SQL definitions live in `streamfusion-nexmark-benchmarks/src/main/resources/nexmark/queries`. The suite tracks the queries supported by the targeted Flink release, including queries that produce updating results and therefore require an upsert-capable Kafka sink.

## Code-owned integration

The integration command provisions Kafka with Testcontainers, generates official Nexmark events, invokes both engines, reads only committed output, validates the result, and renders a throughput table. This keeps broker setup, workload generation, execution, and measurement in one reproducible path.

Run the Docker-backed integration profile with:

```shell
mvn -pl streamfusion-nexmark-benchmarks -am -Pbenchmark-integration verify
```

The benchmark is intentionally opt-in and does not run in the normal pull-request or push workflow. It provisions Kafka and runs both engines, so it should be invoked for deliberate performance and integration validation rather than for unrelated changes.

## RowData boundary variant

The Kafka-free variant uses the generator and `RowData` deserializer from the local
[`nexmark`](https://github.com/nexmark/nexmark) checkout. A thin benchmark-only table-source adapter
marks finite `events.num` runs as bounded and signals completion after assigning all four upstream
Source V2 splits; it does not replace the upstream generator or reader checkpoint state. Those
splits emit Flink internal `RowData`, and a black-hole table sink consumes `RowData` after the SQL
plan. Both source and query run at parallelism four, and checkpointing uses `EXACTLY_ONCE`. This variant isolates
planner and native-operator throughput from broker, JSON, and result-materialization costs. Because
the black-hole sink does not retain results, it is not a result-parity test and does not replace the
Kafka-in/Kafka-out north-star benchmark.

Current all-or-nothing coverage can fully accelerate q0 (pass-through), q1 (decimal currency
conversion), q2 (selection), and q22 (URL directory extraction). q3, q4, q5, q7, q8, q9, q11, q12, q13,
q15, q16, q17, q19, q20, and q23 require an unimplemented join, aggregation, or rank operator. q10
uses unsupported `DATE_FORMAT`; q14 uses a Java UDF, mixed decimal
arithmetic beyond the q1 conversion shape, and timestamp calendar extraction; q21 uses Java-regex
semantics. Those queries remain whole-plan Flink fallback.

Build the local Nexmark connector against this project's Flink version:

```shell
mvn -f /root/data/nexmark/pom.xml -pl nexmark-flink \
  -Dflink.version=2.3.0 -DskipTests package
```

Then run all four currently accelerable queries through both Flink and StreamFusion:

```shell
mvn -pl streamfusion-nexmark-benchmarks -am \
  -Pbenchmark-integration,rowdata-nexmark-integration \
  -Dnexmark.generator.jar=/root/data/nexmark/nexmark-flink/target/nexmark-flink-0.3-SNAPSHOT.jar \
  -Dit.test=LocalRowDataNexmarkBenchmarkIT verify
```

`LocalRowDataNexmarkBenchmark` also accepts an event count, comma-separated query list, and an
engine selector (`flink`, `streamfusion`, or `both`) for standalone measurements. It reports
end-to-end elapsed time, input-event throughput, native calc batches, and native group-aggregate
batches. The `group-aggregate` and `select-distinct` cases exercise both native state backends over
the bounded bid stream; they are focused operator workloads rather than numbered Nexmark queries.
Performance reports
must come from separate, unprofiled JVM forks built with release-mode native code; profiler runs
are diagnostic artifacts rather than benchmark measurements.

On the September 1, 2026 local release/native-CPU `select-distinct` run over two million generated
events, the native in-memory path processed 365,779 events/s versus Flink's 377,002 events/s
(97.0% parity). Native RocksDB processed 351,723 events/s versus Flink RocksDB's 343,327 events/s
(a 2.4% gain). Separate JFR samples for both native backends were dominated by Nexmark generation,
RowData materialization, and exchange-boundary work; they did not expose an obvious
SELECT-DISTINCT state-loop allocation or Java hot spot. These are local diagnostic results, not
portable performance claims.

## Q18 RowData profiling target

The initial stateful path also has an opt-in Q18-shaped microbenchmark that bypasses Kafka and
feeds the same Flink `RowData` representation used at the production boundary. It compares the
native keep-last operator with Flink's `RowTimeDeduplicateFunction`; benchmark measurements still
compile the native runtime in release mode with the local CPU feature set.

```shell
mvn -pl streamfusion-flink -am \
  -Dstreamfusion.q18.benchmark=true \
  -Dstreamfusion.q18.engine=streamfusion \
  -Dstreamfusion.q18.backend=hashmap \
  -Dstreamfusion.q18.events=2000000 \
  -Dstreamfusion.q18.iterations=3 \
  -Dstreamfusion.q18.jfr=/tmp/streamfusion-q18.jfr \
  -Dtest=StreamFusionDeduplicateQ18BenchmarkTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Use `-Dstreamfusion.q18.engine=flink` for the baseline and
`-Dstreamfusion.q18.backend=rocksdb` for either engine's RocksDB path. Every iteration creates a
fresh backend, reports rows per second and a result checksum, and the final line reports the median.
JFR profiling of the first implementation identified managed-memory
accounting scans and redundant output-row copies; incremental accounting and ownership transfer
removed those costs. Larger 4,096-row batches and projecting only Q18's key/order columns both
regressed or failed to improve throughput, so the current implementation deliberately retains the
simpler 1,024-row full-row boundary.

On the August 31, 2026 local release/native-CPU run, the comparable Flink baseline processed
995,392 rows/s. StreamFusion measured 841,482 rows/s before the final ownership fix and 952,203
rows/s after transferring the already-owned output `BinaryRowData` instead of copying it again,
or about 95.7% of that Flink baseline. These are local diagnostic numbers rather than portable
performance claims. The remaining prominent JFR allocation sites are Flink `MemorySegment` wraps
and ownership copies at the reusable-`RowData` boundary; removing those copies would be unsafe
unless the source contract guarantees non-reuse.

For native RocksDB, profiling found that the optional component copied Arrow-owned keys and values
into temporary Rust objects before constructing the RocksDB batch, and that it retained RocksDB's
default write-ahead log even though Flink state recovery is defined by checkpoints and input replay.
The component now borrows the Arrow buffers through its versioned C ABI and disables WAL just as
Flink's RocksDB keyed-state backend does. An attempted pre-`multi_get` duplicate-key index was
removed after it reduced throughput on the mostly-unique Q18 stream.

On the same local release/native-CPU machine, three fresh-database RocksDB iterations over two
million rows produced a median of 202,877 rows/s for StreamFusion and 175,865 rows/s for Flink,
a 15.4% StreamFusion gain with identical checksums. A longer single five-million-row run, which
crossed into compaction, measured 128,548 rows/s versus 87,293 rows/s respectively. As above, these
numbers are diagnostic results for this machine, not portable performance guarantees.

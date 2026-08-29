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
q15, q16, q17, q18, q19, q20, and q23 require an unimplemented join, aggregation, rank, or
deduplication operator. q10 uses unsupported `DATE_FORMAT`; q14 uses a Java UDF, mixed decimal
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
end-to-end elapsed time, input-event throughput, and native calc batch count. Performance reports
must come from separate, unprofiled JVM forks built with release-mode native code; profiler runs
are diagnostic artifacts rather than benchmark measurements.

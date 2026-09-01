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
plan. Both source and query run at parallelism four, and checkpointing uses `EXACTLY_ONCE`. This
variant isolates planner and native-operator throughput from broker and JSON costs. The benchmark
result sink serializes and hashes the complete sorted changelog rather than discarding it. The
bounded adapter seeds each source split deterministically and restores the upstream
generator position, so Flink and StreamFusion receive identical events. This remains a Kafka-free
operator benchmark and does not replace the Kafka-in/Kafka-out north-star benchmark.

Current all-or-nothing coverage can fully accelerate q0 (pass-through), q1 (decimal currency
conversion), q2 (selection), q8 (tumbling aggregates plus Window Join), q11 (event-time session
aggregation), and q22 (URL directory extraction). q3, q4, q5, q7, q9, q13, q15, q16, q17, q19,
q20, and q23 still require an unsupported non-window join, interval join, rank, or surrounding
operator. q10
uses unsupported `DATE_FORMAT`; q14 uses a Java UDF, mixed decimal
arithmetic beyond the q1 conversion shape, and timestamp calendar extraction; q21 uses Java-regex
semantics. Q12's processing-time SQL is catalogued, but a max-speed bounded source completes before
its first ten-second timer and therefore produces an empty result; it is not counted as result or
acceleration evidence. Those queries remain whole-plan Flink fallback.

Build the local Nexmark connector against this project's Flink version:

```shell
mvn -f /root/data/nexmark/pom.xml -pl nexmark-flink \
  -Dflink.version=2.3.0 -DskipTests package
```

Then run the complete currently accelerable query set through both Flink and StreamFusion:

```shell
mvn -pl streamfusion-nexmark-benchmarks -am \
  -Pbenchmark-integration,rowdata-nexmark-integration \
  -Dnexmark.generator.jar=/root/data/nexmark/nexmark-flink/target/nexmark-flink-0.3-SNAPSHOT.jar \
  -Dit.test=LocalRowDataNexmarkBenchmarkIT verify
```

`LocalRowDataNexmarkBenchmark` also accepts an event count, comma-separated query list, and an
engine selector (`flink`, `streamfusion`, or `both`) for standalone measurements. It reports
end-to-end elapsed time, input-event throughput, native calc batches, native group-aggregate
batches, native window-aggregate batches, native Window Join batches, and a row count plus SHA-256
for the full result changelog. The `group-aggregate` and `select-distinct` cases exercise both native state backends over
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

On the September 1, 2026 local release/native-CPU Q11 run over five million deterministic events,
StreamFusion's in-memory session aggregate processed 648,986 events/s versus Flink's 627,817
events/s. Direct native RocksDB processed 526,714 events/s versus Flink RocksDB's 361,980 events/s.
All four runs produced 99,930 result rows and SHA-256
`7bd08ff1fbf1713ed178f847657729a5b69e4fae28d0f7626a5fc71327217d62`. Native CPU profiling
found and removed two quadratic paths: replaying every historical session contribution and scanning
the complete timer set after every registration. Post-fix memory profiles are led by timer snapshot,
session merge, Arrow filtering, state-batch, and key encoding work rather than one dominant loop.
RocksDB samples are primarily skip-list lookup/insertion, comparison, compaction, and `MultiGet`.
JFR allocation profiles for both backends are dominated by the Nexmark generator, Flink `RowData`
and binary-string materialization, memory-segment wrapping/copying, and Arrow envelopes; all custom
Rust state, timer, scratch, RocksDB cache/write-buffer, and exported Arrow allocations remain charged
to Flink managed memory. These are local diagnostic results, not portable performance claims.

On the same machine, a parallelism-four Q8 release run over one million deterministic events
produced 10,562 rows and SHA-256
`5e77d99014bf46d7710eec84aaf795005ccdf71a9c023a2094c6e837a2207fab` in all four cases.
StreamFusion memory processed 169,345 events/s versus Flink's 180,375 events/s (93.9% parity).
Direct native RocksDB processed 157,744 events/s versus Flink RocksDB's 76,588 events/s (2.06x).
Separate JFR runs, excluded from those measurements, found no dominant Java Window Join loop: CPU
samples were led by Arrow/RowData field access, Nexmark generation, binary-row materialization, and
Flink unsafe-memory checks. Allocation samples were led by the required opaque `BinaryRowData`
copies, memory-segment wrappers, generated strings/rows, Arrow foreign-buffer wrappers, and source
deserialization. Native state, timer, scratch, output, RocksDB cache, and write-buffer allocations
are covered by host-memory reservation tests and remain charged to Flink managed memory. These are
local diagnostic results, not portable performance claims.

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

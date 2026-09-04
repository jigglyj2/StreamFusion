# Nexmark benchmark driver

The driver compares native Flink and StreamFusion for the four state backend and
mini-batch combinations. It delegates cluster lifecycle and Nexmark execution to an
executable so deployments can supply their Kafka and Flink topology without embedding
environment-specific assumptions in the benchmark.

The executable receives `--engine`, `--state-backend`, `--mini-batch`,
`--checkpointing-mode EXACTLY_ONCE`, `--source kafka`, `--sink kafka`,
`--changelog-sink upsert-kafka`,
`--kafka-bootstrap`, `--nexmark-home`, and `--queries`. It must print:

```
NEXMARK_THROUGHPUT records_per_second=12345.67
```

Run the packaged driver with:

```
java -jar target/streamfusion-nexmark-benchmarks-0.1.0-SNAPSHOT.jar \
  --executor /path/to/run-nexmark-case \
  --kafka-bootstrap localhost:9092 \
  --nexmark-home /root/data/nexmark \
  --queries q0,q1 \
  --output benchmarks.txt
```

The executor should use the regular Kafka sink for append-only queries and the upsert
Kafka sink for updating queries. An upsert sink table must declare a non-enforced
primary key matching the query's stable unique key. If a query has no stable unique
key in its output, adapt its output schema to retain that key rather than inventing
one or silently collapsing distinct rows.

## Direct SQL job

The benchmark's Flink job is implemented by `NexmarkSqlJob`. It loads the selected
SQL from `src/main/resources/nexmark`, creates bounded Kafka input and exactly-once
Kafka output tables, configures the hashmap state backend with mini-batching disabled,
and waits for the Flink job to finish. The five arguments are the Kafka bootstrap
address, input topic, output topic, query name, and `flink` or `streamfusion` engine.

## RowData SQL job

`LocalRowDataNexmarkBenchmark` removes both Kafka edges. A benchmark-only adapter around the
generator in `/root/data/nexmark` marks a finite `events.num` run as bounded and signals completion
after assigning its four checkpointed Source V2 splits. The upstream generator, RowData
deserializer, reader, and split checkpoint state remain in use. The SQL plan runs at parallelism four
by default and a benchmark result sink serializes and hashes both the complete sorted `RowData`
changelog and its final materialized multiset. Checkpointing uses exactly-once mode and task restart is disabled so resource failures
surface instead of contaminating a timing with retries. It currently runs the fully accelerable q0,
q1, q2, q8, q11, q12, q22, group-aggregate, select-distinct, top-n, limit, over-aggregate,
over-aggregate-event-time, over-aggregate-processing-time, and incremental-group-aggregate queries through both unmodified Flink
and StreamFusion. The focused
workloads use the Nexmark bid stream to exercise keyed `COUNT(*)`/`SUM`/`AVG`/`MIN`/`MAX`, filtered
and counted `DISTINCT` calls, integral and decimal AVG paths, partitioned non-window Top-10, global
`LIMIT/OFFSET`, and ordered unbounded-preceding
OVER aggregation; they are not official numbered Nexmark queries. `over-aggregate-event-time`
retains the bid rowtime attribute so watermarks drive native event timers, while `over-aggregate`
casts the same value to a regular timestamp for the non-time path. The processing-time case retains
the bid filter and nested-row projection below its synthetic `PROCTIME()` order field.
`incremental-group-aggregate` enables Flink's split-DISTINCT optimization and covers the complete
local aggregate, expand, incremental aggregate, and final aggregate pipeline. Wall-clock checkpoint
flushes can change valid intermediate update boundaries from run to run, so standalone output
reports both `output_sha256` for the raw changelog and `materialized_sha256` for the final multiset.
Use controlled SQL parity tests to validate byte-for-byte intermediate changelog semantics.

Build the generator against Flink 2.3 and run all supported cases with:

```
mvn -f /root/data/nexmark/pom.xml -pl nexmark-flink \
  -Dflink.version=2.3.0 -DskipTests package

mvn -pl streamfusion-nexmark-benchmarks -am \
  -Pbenchmark-integration,rowdata-nexmark-integration \
  -Dnexmark.generator.jar=/root/data/nexmark/nexmark-flink/target/nexmark-flink-0.3-SNAPSHOT.jar \
  -Dit.test=LocalRowDataNexmarkBenchmarkIT verify
```

Use `install` instead of `package` before a standalone invocation that consumes
`target/classpath.txt`; the generated classpath names reactor dependencies in the local Maven
repository, and an older installed planner JAR would otherwise produce an explicit fallback rather
than measure the just-built code.

For standalone measurements, invoke `LocalRowDataNexmarkBenchmark` with the event count,
comma-separated query names, `flink`, `streamfusion`, or `both`, `hashmap`, `rocksdb`, or
`both`, and an optional parallelism. Use parallelism one when comparing the complete streaming
Top-N changelog: independent multi-input network scheduling can produce different but equivalent
intermediate Top-N updates. Its stable output includes elapsed time, input-event throughput, native calc batches, and
native deduplicate, group-aggregate, Top-N, window-aggregate, window-join, regular-join, and
interval-join batches, plus native OVER-aggregate batches. Set
`-Dstreamfusion.nexmark.jfr=/absolute/path/top-n.jfr` to capture the JVM profile and allocation
samples for a standalone run. Run performance
measurements from a release/native-CPU build and use separate JVM invocations for each engine;
the `both` mode is primarily a convenience smoke test.

Set `-Dstreamfusion.nexmark.mini-batch=true` to enable count-triggered mini-batching for a RowData
run. `-Dstreamfusion.nexmark.aggregate-phase=ONE_PHASE` or `TWO_PHASE` fixes Flink's aggregate phase
strategy; the default is `AUTO`. These are benchmark controls applied identically to Flink and
StreamFusion, not deployment settings.

`-Dstreamfusion.nexmark.mini-batch-size=<records>` controls the count trigger and
`-Dstreamfusion.nexmark.checkpoint-interval-ms=<milliseconds>` controls the exactly-once checkpoint
interval; their defaults are 5,000 and 1,000 milliseconds. The optional
`-Dstreamfusion.nexmark.managed-memory-mb=<MiB>` sets Flink's ordinary TaskManager managed-memory
size for both engines and defaults to 1,024 MiB. Increase it for high-cardinality DISTINCT profiles
instead of allowing either implementation to allocate state outside Flink's budget. The optional
standard Flink `-Dtaskmanager.memory.managed.consumer-weights=OPERATOR:90,STATE_BACKEND:10,PYTHON:30`
property overrides the harness default. The default favors Arrow operator scratch because the
bounded workloads have a small RocksDB working set; it is applied identically to both engines.
The optional
`-Dstreamfusion.nexmark.debug-rows=true` and `-Dstreamfusion.nexmark.debug-plan=true` switches print
the collected rows and optimized SQL plan for diagnosis and should remain off for measurements.

For example, after the integration-profile build:

```sh
java ... tech.streamfusion.benchmark.nexmark.LocalRowDataNexmarkBenchmark \
  1000000 group-aggregate streamfusion rocksdb
```

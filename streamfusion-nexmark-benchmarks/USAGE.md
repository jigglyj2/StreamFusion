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
and a black-hole table sink consumes `RowData`. Checkpointing uses exactly-once mode. It currently
runs the fully accelerable q0, q1, q2, and q22 queries through both unmodified Flink and StreamFusion.
The additional `group-aggregate` case uses the Nexmark bid stream to exercise keyed `COUNT(*)`,
`SUM`, `MIN`, and `MAX`; it is a focused state-operator benchmark rather than an official numbered
Nexmark query.
Because the black-hole sink does not retain results, this variant measures execution throughput
rather than output parity.

Build the generator against Flink 2.3 and run all four with:

```
mvn -f /root/data/nexmark/pom.xml -pl nexmark-flink \
  -Dflink.version=2.3.0 -DskipTests package

mvn -pl streamfusion-nexmark-benchmarks -am \
  -Pbenchmark-integration,rowdata-nexmark-integration \
  -Dnexmark.generator.jar=/root/data/nexmark/nexmark-flink/target/nexmark-flink-0.3-SNAPSHOT.jar \
  -Dit.test=LocalRowDataNexmarkBenchmarkIT verify
```

For standalone measurements, invoke `LocalRowDataNexmarkBenchmark` with the event count,
comma-separated query names, `flink`, `streamfusion`, or `both`, and `hashmap`, `rocksdb`, or
`both`. Its stable output includes elapsed time, input-event throughput, native calc batches, and
native group-aggregate batches. Run performance
measurements from a release/native-CPU build and use separate JVM invocations for each engine;
the `both` mode is primarily a convenience smoke test.

For example, after the integration-profile build:

```sh
java ... tech.streamfusion.benchmark.nexmark.LocalRowDataNexmarkBenchmark \
  1000000 group-aggregate streamfusion rocksdb
```

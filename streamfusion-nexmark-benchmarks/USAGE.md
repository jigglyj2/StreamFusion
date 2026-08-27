# Nexmark benchmark driver

The driver compares native Flink and StreamFusion for the four state backend and
mini-batch combinations. It delegates cluster lifecycle and Nexmark execution to an
executable so deployments can supply their Kafka and Flink topology without embedding
environment-specific assumptions in the benchmark.

The executable receives `--engine`, `--state-backend`, `--mini-batch`,
`--checkpointing-mode EXACTLY_ONCE`, `--source kafka`, `--sink kafka`,
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

The selected queries must support an append-only Kafka sink. The default `q0,q1`
pair does; changelog queries require an upsert-capable sink adapter.

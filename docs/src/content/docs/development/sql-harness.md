---
title: SQL test harness
description: Verifying streaming SQL behavior against Apache Flink.
---

The SQL harness is the primary correctness loop for planner and operator development. Each test executes equivalent streaming SQL through unmodified Flink planning and through the StreamFusion planner hook, then compares results rather than physical plan text.

Plan snapshots are intentionally not the contract: StreamFusion is expected to replace parts of the plan. Observable result parity is the contract.

## Run the harness

The harness requires the small StreamFusion planner-factory patch to be applied to Apache Flink 2.3. Once the patched planner artifacts are installed locally, run:

```shell
mvn -pl streamfusion-flink-sql-tests -am test
```

GitHub Actions checks out the matching Flink release, applies the patch, installs the required planner artifacts, and runs the harness on every push and pull request. It builds both native libraries required by the Java tests, including the RocksDB state plugin. It then adds the built StreamFusion runtime and planner extension to Flink's own `flink-table-planner` test classpath and runs every upstream `runtime/stream/**/*ITCase` and `runtime/batch/**/*ITCase` through Flink's MiniCluster test infrastructure. Streaming and batch are separate invocations with separate acceleration audit logs; each fails unless it executes at least one accelerated plan, preventing either suite from becoming an accidentally all-fallback green run. StreamFusion-owned parity tests additionally declare whether each query is expected to accelerate and fail if an eligible query silently falls back.

To reproduce the upstream portion after installing StreamFusion artifacts and applying
`dev/flink/2.3.0-streamfusion-sql-suite.patch` to the matching Flink checkout:

```shell
dev/integration/run-flink-sql-suite.sh /path/to/flink
```

## Correctness policy

- Compare complete result sets, including changelog behavior where applicable.
- Preserve exact byte-level output parity when data crosses external connectors.
- Exercise normal Flink fallback for unsupported accelerated operators.
- Do not use textual plan equality as a substitute for output correctness.

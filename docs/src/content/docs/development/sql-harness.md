---
title: SQL test harness
description: Verifying streaming SQL behavior against Apache Flink.
---

SQL parity capture serializes schema-converted rows with Flink's `RowDataSerializer`,
including RowKind, nulls, nested values, and field boundaries. It preserves arrival order
by default. Display strings are not a parity encoding: for example, SQL NULL and the string
`'null'` can have identical display text. Tests comparing an unordered insert-only relation
must request that comparison explicitly; that mode rejects updating changelogs.

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

The downstream compatibility patch changes only the upstream test classpath and test helpers.
Flink's helpers sometimes cast the installed planner directly to `PlannerBase`; StreamFusion
installs a `Planner` facade. A test-only accessor unwraps that facade for relational-plan and
execution-environment inspection. `TestingTableEnvironment` retains the facade as its actual
planner, and translation still goes through StreamFusion, including native-state setup and
whole-plan admission. This adaptation does not change Flink's operator implementations or SQL
assertions, and it is not part of a production Flink dependency build.

## Correctness policy

- Compare complete result sets, including changelog behavior where applicable.
- Preserve exact byte-level output parity when data crosses external connectors.
- Exercise normal Flink fallback for unsupported accelerated operators.
- Do not use textual plan equality as a substitute for output correctness.

SQL fixtures without an output ordering contract explicitly compare unordered INSERT relations.
They reject updating records in that mode. Parallel or mini-batched keyed fixtures may compare
independent keys in canonical key order while preserving every key's complete changelog sequence;
they declare the key fields explicitly. Global sorting and single-key update-order tests keep ordered
capture. These choices affect test interleaving only and do not canonicalize floating-point values.

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

GitHub Actions checks out the matching Flink release, applies the patch, installs the required planner artifacts, and runs the harness on every push and pull request.

## Correctness policy

- Compare complete result sets, including changelog behavior where applicable.
- Preserve exact byte-level output parity when data crosses external connectors.
- Exercise normal Flink fallback for unsupported accelerated operators.
- Do not use textual plan equality as a substitute for output correctness.

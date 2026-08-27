---
title: Planning
description: StreamFusion's whole-plan eligibility and fallback contract.
---

StreamFusion uses an **all-or-nothing** planning rule. A plan is accelerated only when every internal relational node is replaced by a StreamFusion physical operator. StreamFusion does not create mixed Flink/StreamFusion execution islands inside a plan.

```text
Flink source
  → RowData-to-Arrow boundary
  → StreamFusion operator
  → StreamFusion operator
  → Arrow-to-RowData boundary
  → Flink sink
```

Sources and sinks are the only boundary exceptions. A connector may eventually supply a native StreamFusion source or sink. Otherwise, the planner requires an explicit transpose:

- `StreamFusionRowDataToArrow` converts records produced by a Flink source into native Arrow batches.
- `StreamFusionArrowToRowData` converts native Arrow batches back into records accepted by a Flink sink.

Both transpose operators are currently planning skeletons. Their runtime conversion, memory ownership, batching, changelog handling, and checkpoint integration remain TODO. Consequently, no production SQL plan is accelerated yet.

## Eligibility algorithm

The planner walks the complete candidate plan and records a StreamFusion implementation or a rejection reason for every node:

1. Every internal node must map to a StreamFusion physical operator.
2. Every source and sink must map to a native StreamFusion connector or the appropriate transpose boundary.
3. Any rejected node rejects the entire plan.
4. A rejected plan is delegated unchanged to Flink.

This deliberately prioritizes result parity and operational clarity over partial acceleration.

## EXPLAIN diagnostics

StreamFusion appends an acceleration section to Flink's normal explanation:

```text
== StreamFusion Acceleration ==
Accelerated: no
Plan reason: all-or-nothing coverage failed; the entire plan will use Flink.
Operator rejections:
- StreamExecCalc [INTERNAL]: scalar function JSON_VALUE is not implemented
- StreamExecGroupAggregate [INTERNAL]: retractable SUM is not implemented
```

The plan-level reason explains why acceleration was rejected. Each operator-level entry identifies the Flink operator that could not be replaced and gives its specific reason. Until Flink-plan conversion is connected to the new eligibility model, live SQL explanations state that conversion and the boundary transposes are not implemented.

## Flink runner integration test

The process-level integration test builds a standalone SQL job JAR and installs the `streamfusion-flink` JAR into the `lib/` directory of an Apache Flink 2.3 distribution. It also replaces the distribution's table API JAR with the matching artifact containing the minimal planner-factory hook. The test then submits the job through the real CLI:

```shell
flink run -t local \
  -c tech.streamfusion.flink.runner.PlannerHookIntegrationJob \
  streamfusion-flink-runner-tests.jar
```

The submitted job executes SQL and fails unless exactly one StreamFusion planner was created and that planner translated the job. This tests process classloading and JAR installation in addition to SQL behavior. Future integration artifacts—native libraries, connector JARs, and more complex submitted jobs—belong in this same runner-level path.

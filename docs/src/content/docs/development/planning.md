---
title: Planning
description: StreamFusion's whole-plan eligibility and fallback contract.
---

StreamFusion uses an **all-or-nothing** planning rule. A plan is accelerated only when every internal relational node is replaced by a StreamFusion physical operator. StreamFusion does not create mixed Flink/StreamFusion execution islands inside a plan.

```text
Flink source
  → lightweight RowData Arrow batch view
  → StreamFusion operator
  → StreamFusion operator
  → lightweight RowData Arrow batch view
  → Flink sink
```

Sources and sinks are the only boundary exceptions. A connector may eventually supply a native StreamFusion source or sink. Otherwise, it supplies a `FlinkRowDataArrowBatchView`: a lightweight interface that describes a batch and exposes its Flink `RowData` without making the boundary itself a physical transpose operator.

Creating this Java view must not copy row payloads. A native boundary implementation may still need to materialize Arrow column buffers once when a source is genuinely row-based—row and column memory layouts cannot be relabeled into one another. If a connector already owns Arrow-compatible columnar buffers, its view should import or retain those buffers instead. The sink side follows the inverse ownership protocol through the same boundary abstraction.

The runtime view implementation, Arrow materialization, memory ownership, batching, changelog handling, and checkpoint integration remain TODO. Consequently, no production SQL plan is accelerated yet.

## Native batch pipeline

StreamFusion follows the execution shape used by Apache DataFusion Comet:

1. Consecutive native operators are fused into one native plan rather than alternating between JVM and Rust operators.
2. The native plan is lowered to a DataFusion `ExecutionPlan` tree.
3. Parent and child operators communicate through `SendableRecordBatchStream`-style streams of Arrow `RecordBatch` values.
4. Arrow arrays retain their buffers through reference-counted ownership, so passing a batch to the next native operator copies only lightweight batch/ownership metadata—not column buffers.
5. JVM input crosses once at the native-plan edge using an Arrow C Stream-style boundary; output crosses once at the other edge.

This is a **zero-copy handoff** guarantee between adjacent Rust operators. It is not a claim that every operator is allocation-free: a projection can often reuse input arrays, while a sort, join, aggregation, or computed expression may necessarily create new output buffers. What is forbidden is serialization, RowData conversion, or defensive copying merely to pass an existing Arrow batch from one native operator to the next.

## Eligibility algorithm

The planner walks the complete candidate plan and records a StreamFusion implementation or a rejection reason for every node:

1. Every internal node must map to a StreamFusion physical operator.
2. Every source and sink must map to a native StreamFusion connector or a Flink RowData Arrow batch view.
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

The plan-level reason explains why acceleration was rejected. Each operator-level entry identifies the Flink operator that could not be replaced and gives its specific reason. Until Flink-plan conversion is connected to the new eligibility model, live SQL explanations state that conversion, boundary views, and native Arrow materialization are not implemented.

## Flink runner integration test

The process-level integration test builds a standalone SQL job JAR and installs the `streamfusion-flink` JAR into the `lib/` directory of an Apache Flink 2.3 distribution. It also replaces the distribution's table API JAR with the matching artifact containing the minimal planner-factory hook. The test then submits the job through the real CLI:

```shell
flink run -t local \
  -c tech.streamfusion.flink.runner.PlannerHookIntegrationJob \
  streamfusion-flink-runner-tests.jar
```

The submitted job executes SQL and fails unless exactly one StreamFusion planner was created and that planner translated the job. This tests process classloading and JAR installation in addition to SQL behavior. Future integration artifacts—native libraries, connector JARs, and more complex submitted jobs—belong in this same runner-level path.

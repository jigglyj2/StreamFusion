---
title: Planning
description: StreamFusion's whole-plan eligibility and fallback contract.
---

StreamFusion uses an **all-or-nothing** planning rule. A plan is accelerated only when every internal relational node is replaced by a StreamFusion physical operator. StreamFusion does not create mixed Flink/StreamFusion execution islands inside a plan.

Like Comet, StreamFusion performs this selection with a physical-plan rule and distinct
accelerator nodes. Flink first builds its normal exec graph. The StreamFusion graph
processor proves whole-plan eligibility and replaces eligible `StreamExecCalc` nodes
with `StreamFusionExecCalc` nodes. The original Flink nodes are neither modified nor
given native execution branches; if eligibility fails, the original graph is returned
unchanged. Each StreamFusion exec node then creates its corresponding StreamFusion
runtime operator.

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

The Arrow boundary follows PyFlink's lightweight model: `ColumnarRowData` moves a reusable row index over Flink column vectors backed by Arrow vectors. Its implemented type matrix includes compatible scalar, temporal, decimal128, string, binary, array, map, nested-row, and null types. Arrow C Data release ownership is implemented at the single-calc boundary. Flink managed-memory allocation and checkpoint-aware native state remain TODO.

## Native batch pipeline

StreamFusion follows the execution shape used by Apache DataFusion Comet:

1. Consecutive native operators are fused into one native plan rather than alternating between JVM and Rust operators.
2. The native plan is lowered to a DataFusion `ExecutionPlan` tree.
3. Parent and child operators communicate through `SendableRecordBatchStream`-style streams of Arrow `RecordBatch` values.
4. Arrow arrays retain their buffers through reference-counted ownership, so passing a batch to the next native operator copies only lightweight batch/ownership metadata—not column buffers.
5. JVM input crosses once at the native-plan edge using an Arrow C Stream-style boundary; output crosses once at the other edge.

This is a **zero-copy handoff** guarantee between adjacent Rust operators. It is not a claim that every operator is allocation-free: a projection can often reuse input arrays, while a sort, join, aggregation, or computed expression may necessarily create new output buffers. What is forbidden is serialization, RowData conversion, or defensive copying merely to pass an existing Arrow batch from one native operator to the next.

Here, **fused** describes the shared native plan boundary; it does not mean kernel
fusion or concurrent operations over one batch. StreamFusion follows Comet's
vectorized model. Each physical operator consumes a batch and completes before its
parent consumes the output: a chain of three calc nodes remains calc → calc → calc.
The stages use ordinary DataFusion execution operators and remain separately visible
for metrics, diagnostics, and parity tests.

## Protobuf plan handoff

StreamFusion follows Comet's split between plan transport and data transport. The Java
planner constructs a typed tree of native operators, expressions, schemas, and semantic
options, serializes that tree with Protocol Buffers, and passes the bytes to the native
runtime. Rust decodes the message and lowers each node to a DataFusion `ExecutionPlan`
or a custom StreamFusion operator.

Only the root of a connected native block needs to carry the serialized tree. The
protobuf schema is a versioned compatibility contract, not a serialization of Flink or
DataFusion implementation classes. Every field that affects Flink semantics—types,
nullability, overflow and error behavior, time zones, collations, changelog mode, and
operator-specific options—must be represented explicitly. Rust must reject an unknown
version, operator, expression, enum, or required semantic option; the Java planner then
keeps the whole plan on Flink and reports the reason through EXPLAIN.

Protocol Buffers carries control data only. Arrow C Data and C Stream interfaces carry
runtime batches, so plan serialization does not introduce a per-batch Java/native copy.

The initial implementation order is deliberately narrow:

1. integer projection and `>=` filtering, proving protobuf, JNI, Arrow row views, lifecycle, and byte parity end to end;
2. column selection and reordering across general schemas;
3. boolean filtering with complete SQL null semantics;
4. literals and individually allow-listed scalar expressions.

Unsupported expressions reject the calc and therefore the complete plan. More complex
stateless operators come only after this path passes generated parity cases and the
corresponding Flink SQL harness coverage.

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

The plan-level reason explains why acceleration was rejected. Each operator-level entry identifies the Flink operator that could not be replaced and gives its specific reason. The current live explanation is still conservative and reports fallback while operator-level eligibility diagnostics are being connected to physical translation. Tests therefore prove acceleration with an execution counter in addition to byte parity; plan text is not used as proof.

## Flink runner integration test

The process-level integration test builds a standalone SQL job JAR and installs the `streamfusion-flink` JAR into the `lib/` directory of an Apache Flink 2.3 distribution. It also installs the separate StreamFusion planner extension into Flink's isolated planner loader and applies the minimal generic planner-extension hooks. It does not replace any Flink exec node or runtime operator class. The test then submits the job through the real CLI:

```shell
flink run -t local \
  -c tech.streamfusion.flink.runner.PlannerHookIntegrationJob \
  streamfusion-flink-runner-tests.jar
```

The submitted job executes an eligible integer filter/projection and fails unless exactly one StreamFusion planner was created, a `StreamFusionExecCalc` selected the native runtime, and at least one DataFusion calc batch executed through JNI. It then executes an unsupported arithmetic calc and proves that the result comes from Flink without incrementing the native counter. This tests acceleration selection, unchanged fallback, process classloading, native-library packaging, and JAR installation. Future integration artifacts—connector JARs and more complex submitted jobs—belong in this same runner-level path.

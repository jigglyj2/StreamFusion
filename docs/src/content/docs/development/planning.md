---
title: Planning
description: StreamFusion's whole-plan eligibility and fallback contract.
---

StreamFusion uses an **all-or-nothing** planning rule. A plan is accelerated only when every internal relational node is replaced by a StreamFusion physical operator. StreamFusion does not create mixed Flink/StreamFusion execution islands inside a plan.

Like Comet, StreamFusion performs this selection with a physical-plan rule and distinct
accelerator nodes. Flink first builds its normal exec graph. The StreamFusion graph
processor proves whole-plan eligibility and replaces eligible `StreamExecCalc` and
`StreamExecUnion` nodes with distinct `StreamFusionExecCalc` and
`StreamFusionExecUnion` nodes. The original Flink nodes are neither modified nor
given native execution branches; if eligibility fails, the original graph is returned
unchanged. At translation time, the outermost `StreamFusionExecCalc` collects every
adjacent StreamFusion Calc below it, preserves their input-to-output order, and creates
one Flink runtime operator for the connected chain. A non-StreamFusion node ends the
chain and therefore defines a native-plan boundary.

Streaming `UNION ALL` is a deliberate exception to native lowering, but not to physical
coverage. Flink defines it as zero-work topology wiring, so `StreamFusionExecUnion`
preserves the `UnionTransformation` instead of introducing a DataFusion merge operator.
This keeps Flink in control of watermarks, barriers, scheduling, and input interleaving;
accelerated native blocks remain on the union's individual branches.

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

The Arrow boundary follows PyFlink's lightweight model: `ColumnarRowData` moves a reusable row index over Flink column vectors backed by Arrow vectors. Its implemented type matrix includes compatible scalar, temporal, decimal128, string, binary, array, map, nested-row, and null types. Arrow C Data release ownership is implemented at the fused Calc-chain boundary. Flink managed-memory allocation and checkpoint-aware native state remain TODO.

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

Adjacent Calc fusion is implemented. Java encodes `Calc(Calc(...Input))` as one native
protobuf tree, and Rust recursively lowers it into distinct DataFusion filter and
projection stages. Each input batch crosses JNI once for the complete chain. The hidden
input ordinal used to preserve Flink `RowKind` is projected through every stage and is
removed only at the outer output boundary. Identity-chain tests also compare Arrow
buffer addresses and prove that adjacent projections retain the same underlying buffer.

## Protobuf plan handoff

StreamFusion follows Comet's split between plan transport and data transport. The Java
planner constructs a typed tree of native operators, expressions, schemas, and semantic
options, serializes that tree with Protocol Buffers, and passes the bytes to the native
runtime. Rust decodes the message and lowers each node to a DataFusion `ExecutionPlan`
or a custom StreamFusion operator.

As in Comet's `exprToProto` model, Calc expressions use one recursive, typed serializer.
Projection and filter positions do not maintain separate allow-lists: a filter merely
requires the root expression to return `BOOLEAN`. Supported expressions can therefore be
nested beneath comparisons, null checks, boolean operators, search ranges, `LIKE`, and
other supported parents. The serializer validates the Flink/Calcite type at every node;
being supported in isolation does not waive a parent's semantic restrictions.

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
Plan reason: the entire plan will use Flink.
Fallback: root[0]/StreamExecCalc: projection[0]/TRIM.operand[0]: Calcite type is not supported
```

The plan-level reason explains the all-or-nothing decision. Every uncovered physical node
is reported with its path through the exec graph. A rejected Calc additionally reports
`projection[n]` or `condition` and descends through function operands to the expression
that failed typed protobuf serialization. Eligible plans report `Accelerated: yes`.
Execution-counter and byte-parity tests remain the proof that a selected native node ran;
EXPLAIN describes the planner decision rather than replacing runtime verification.

## Native exchange contract

A native exchange does not replace Flink distribution. Flink still declares the shuffle,
maximum parallelism, downstream parallelism, checkpoint barriers, watermarks, and rescaling
topology. StreamFusion may accelerate the data path only when it can preserve those declarations
exactly.

The wire shape is Arrow Flight-inspired: the versioned exchange plan carries one Arrow schema,
then the channel sends Arrow record-batch messages that refer to that schema. A batch is never
converted to `RowData` or serialized row by row between native nodes.
Within one process, Arrow C Stream ownership passes reference-counted buffers directly. Across a
network edge, Arrow IPC/Flight-style buffers are framed because bytes must cross the network, but
the schema is not rebuilt or resent for every batch. Each Flink network record contains an Arrow
IPC record-batch metadata message and its columnar body, independently decodable against the
versioned schema stored in the exchange plan. It does not contain an IPC schema message. Dictionary
encoded batches currently fall back because exact support requires per-channel dictionary state.

Flink's record envelope remains part of the data contract. StreamFusion appends a non-null
`__streamfusion_row_kind` byte column using Flink's stable `RowKind.toByteValue()` encoding and,
when records carry timestamps, a nullable `__streamfusion_stream_record_timestamp` 64-bit column.
Appending this envelope reuses the existing Arrow data-column buffers. Watermarks, watermark
statuses, latency markers, and checkpoint barriers remain ordered Flink network control events;
they are never disguised as rows in the Arrow stream.

Hash routing first produces lightweight per-key-group selection vectors over one shared input
batch. Keeping each frame within one stable Flink key group allows Flink's `RANGE` channel-state
mapper to redistribute recovered in-flight data when parallelism changes. Only immediately before
a key-group frame crosses the network does StreamFusion gather that selection into contiguous
Arrow arrays. This is an intentional columnar gather, not row serialization; it preserves input
order and keeps the row-kind and timestamp envelope aligned. The runtime partitioner maps the
frame's key group to the current downstream parallelism rather than persisting a stale destination.

Keyed exchange uses Flink's key-group identity, not DataFusion's partition hash. The core native
exchange code reproduces the two-stage Flink calculation: hash the exact Flink `BinaryRowData`
key bytes with `BinarySegmentUtils.hashByWords`, then apply `MathUtils.murmurHash` and reduce by
the operator's configured maximum parallelism. This is the compatibility path implemented by
Paimon Rust's BinaryRow encoder/hash and Fluss Rust's Flink Murmur utility. Cross-language fixtures
lock both hashing stages to Flink 2.3.

The planner now selects a distinct `StreamFusionExecExchange` for hash and singleton distributions
when the complete plan passes the all-or-nothing eligibility check. Its runtime remains a
Flink-owned writer -> `PartitionTransformation` -> reader topology. The writer transposes a batch,
native Rust encodes supported scalar keys and emits stable key-group frames, Flink transports those
frames and their control events, and the reader exposes the decoded Arrow batch as lightweight
`RowData` views. The default maximum parallelism is Flink's own lower-bound default of 128, and
the partitioner advertises Flink's `RANGE` state mapper so restored in-flight frames can be remapped
after rescaling.

Hash keys currently support booleans, integral and floating-point numbers, decimals, character and
binary strings, dates, times, and both timestamp families, including nullable and composite keys.
`ARRAY`, `MAP`, `MULTISET`, and `ROW` hash keys fall back with an EXPLAIN reason. Those complex types
may still appear in non-key columns. Boundary types that Arrow cannot represent over Flink's full
value domain, and dictionary-encoded IPC batches that would require channel dictionary state, also
fall back rather than approximate Flink behavior. A real local Flink runner test exercises the
native writer, Flink network partition, and native reader at parallelism two.

## Flink runner integration test

The process-level integration test builds a standalone SQL job JAR and installs the `streamfusion-flink` JAR into the `lib/` directory of an Apache Flink 2.3 distribution. It also installs the separate StreamFusion planner extension into Flink's isolated planner loader and applies the minimal generic planner-extension hooks. It does not replace any Flink exec node or runtime operator class. The test then submits the job through the real CLI:

```shell
flink run -t local \
  -c tech.streamfusion.flink.runner.PlannerHookIntegrationJob \
  streamfusion-flink-runner-tests.jar
```

The submitted job executes an eligible integer filter/projection and fails unless exactly one StreamFusion planner was created, a `StreamFusionExecCalc` selected the native runtime, and at least one DataFusion calc batch executed through JNI. It then executes an unsupported arithmetic calc and proves that the result comes from Flink without incrementing the native counter. This tests acceleration selection, unchanged fallback, process classloading, native-library packaging, and JAR installation. Future integration artifacts—connector JARs and more complex submitted jobs—belong in this same runner-level path.

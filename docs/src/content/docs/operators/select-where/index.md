---
title: SELECT & WHERE
description: Acceleration coverage for stateless Flink SQL projections and filters.
sidebar:
  order: 2
---

**Current status:** Partially accelerated. Standalone supported Calc stages and homogeneous
Calc chains remain eligible. Other adjacent native combinations and plans containing persistent
native state temporarily fall back as a whole; see
[architecture admission](/StreamFusion/development/architecture-admission/).

StreamFusion replaces both `StreamExecCalc` and `BatchExecCalc` with distinct accelerator
nodes and plans an eligible Calc as sequential DataFusion operators over Arrow
batches: an optional `FilterExec`, followed by `ProjectionExec`. The entire Calc falls
back to Flink if either stage contains an unsupported expression or type.

Adjacent eligible Calc nodes execute as one nested native plan. They remain sequential
and independently represented—Calc then Calc, without kernel fusion—but an Arrow batch
passes directly from one DataFusion stage to the next. The chain performs one JNI/Arrow
C Data import at its input and a pull-based Arrow C Stream at its output; it does not materialize
intermediate `RowData` or copy an existing batch merely for operator handoff.

Calc now supplies its own protobuf fragment to the same planner interface as Expand, UNNEST,
and row replication. Shared region discovery and recursive native lowering connect these nodes;
the Calc exec classes no longer build specialized Calc/UNNEST/replication combinations. Existing
bounded-join planner lifecycle adapters remain pending migration. Streaming joins and deduplication
use the same fragment interface and shared keyed-region runtime, without Calc-specific dispatch
or a separate lifecycle-owner fusion hook. New protocol-v2 Calc fragments
project SQL payload and preserve native RowKind/ordinal metadata independently. Mixed-stage
production admission is still gated as described above.

The common Calc builder now receives the task's Flink memory pool. Its managed scalar
allocation policies cover `REPEAT` growth and an explicit set of floating-point math kernels
([memory policy details](../../development/memory-and-configuration/)). They admit workspace before
calling the unchanged DataFusion kernels and retain output credit with Arrow buffers, while
preserving function ordering and interval metadata. Both nested expressions and adjacent Calc stages still use the standard
DataFusion projection/filter tree; this is not a fusion recipe. Filter gathers now have their own
pre-consumption workspace envelope around the cached DataFusion kernel. Other expression workspaces
and Arrow Java import accounting still need audit. The shared native stream edge now reuses
registered buffer leases; this is not complete Calc memory-admission evidence.

Projection roots also admit scalar broadcasting before DataFusion materializes its output.
Array results remain shared, and cached literals are borrowed without per-batch string/binary
clones. Typed-null projections now include local-zone timestamps, intervals and multisets, with
generated changelog parity across all 22 payload families. These changes do not lift the remaining
persistent-state or mixed-region admission gates.

- [Projections](projections/) lists supported result expressions and types.
- [Filters](filters/) lists supported predicates and SQL null behavior.

The test suite reflects over every public scalar definition in the pinned Flink 2.3 dependency and
requires one recorded decision: native execution, explicit fallback, canonical Calcite rewrite, or
non-Calc planner/runtime handling. Upgrading Flink or adding a public scalar therefore fails the
focused catalog test until its behavior and documentation are reviewed.

Streaming and bounded Calc use the same Java expression serializer, versioned protobuf,
DataFusion physical operators, managed-memory pool, and Arrow boundary. There is no second
row-oriented batch implementation. Java serializes the Calc chain as a recursive protobuf operator tree. The JVM/native boundary uses Arrow C Data, and
the native result is exposed to Flink as reusable Arrow-backed `ColumnarRowData` views.

See the [Flink 2.3 SELECT & WHERE documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/select/).

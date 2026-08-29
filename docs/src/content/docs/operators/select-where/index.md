---
title: SELECT & WHERE
description: Acceleration coverage for stateless Flink SQL projections and filters.
sidebar:
  order: 2
---

**Current status:** Partially accelerated.

StreamFusion plans an eligible Calc as sequential DataFusion operators over Arrow
batches: an optional `FilterExec`, followed by `ProjectionExec`. The entire Calc falls
back to Flink if either stage contains an unsupported expression or type.

Adjacent eligible Calc nodes execute as one nested native plan. They remain sequential
and independently represented—Calc then Calc, without kernel fusion—but an Arrow batch
passes directly from one DataFusion stage to the next. The chain performs one JNI/Arrow
C Data import at its input and one export at its output; it does not materialize
intermediate `RowData` or copy an existing batch merely for operator handoff.

- [Projections](projections/) lists supported result expressions and types.
- [Filters](filters/) lists supported predicates and SQL null behavior.

The test suite reflects over every public scalar definition in the pinned Flink 2.3 dependency and
requires one recorded decision: native execution, explicit fallback, canonical Calcite rewrite, or
non-Calc planner/runtime handling. Upgrading Flink or adding a public scalar therefore fails the
focused catalog test until its behavior and documentation are reviewed.

Java serializes the Calc chain as a recursive protobuf operator tree. The JVM/native boundary uses Arrow C Data, and
the native result is exposed to Flink as reusable Arrow-backed `ColumnarRowData` views.

See the [Flink 2.3 SELECT & WHERE documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/select/).

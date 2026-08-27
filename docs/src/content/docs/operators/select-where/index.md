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

- [Projections](projections/) lists supported result expressions and types.
- [Filters](filters/) lists supported predicates and SQL null behavior.

Java serializes the Calc as protobuf. The JVM/native boundary uses Arrow C Data, and
the native result is exposed to Flink as reusable Arrow-backed `ColumnarRowData` views.

See the [Flink 2.3 SELECT & WHERE documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/select/).

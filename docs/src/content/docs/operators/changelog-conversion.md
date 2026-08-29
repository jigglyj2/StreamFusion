---
title: Changelog conversion
description: Acceleration coverage and fallback behavior for Flink SQL Changelog conversion.
sidebar:
  order: 19
---

**Current status:** Partially accelerated. Planner-inserted `DropUpdateBefore` has a distinct
StreamFusion implementation; explicit changelog conversion functions remain on Flink.

**Future acceleration target:** Not currently a compute target.

## SQL example

```sql
SELECT *
FROM TABLE(FROM_CHANGELOG(TABLE raw_changes, DESCRIPTOR(op)));
```

## Acceleration and fallback

`FROM_CHANGELOG` and `TO_CHANGELOG` remain in Flink. When Flink inserts
`StreamExecDropUpdateBefore` because a downstream SQL operator or sink does not require retraction
records, StreamFusion accepts that node and drops only `UPDATE_BEFORE`. `INSERT`, `UPDATE_AFTER`,
and `DELETE` retain their values, `RowKind`, timestamp, and order. Watermarks and other control
events remain under Flink. Other changelog-normalization nodes still cause whole-plan fallback.

## Implementation

`StreamFusionExecDropUpdateBefore` selects a focused JVM runtime operator because `RowKind` is
Flink changelog metadata rather than an Arrow data column. It does not create an Arrow boundary
merely to test the row kind. Adjacent native columnar stages still use Arrow batches normally.
Generated operator coverage compares all four row kinds and verifies timestamp and watermark
ordering. Native execution never collapses updates or deletes into append-only rows.

See the [Flink 2.3 Changelog conversion documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/changelog/).

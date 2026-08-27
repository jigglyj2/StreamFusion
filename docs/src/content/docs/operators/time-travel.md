---
title: Time travel
description: Acceleration coverage and fallback behavior for Flink SQL Time travel.
sidebar:
  order: 20
---

**Current status:** Not accelerated; executed by Flink.

**Can it be accelerated?** No; it is catalog and planning behavior.

## SQL example

```sql
SELECT * FROM orders\nFOR SYSTEM_TIME AS OF TIMESTAMP '2026-01-01 00:00:00';
```

## Acceleration and fallback

Flink resolves the historical table snapshot. StreamFusion may accelerate eligible operators after resolution, but does not replace catalog lookup or snapshot selection.

## Implementation

No Rust implementation. This stays in Flink to preserve connector, catalog, time-zone, and planning semantics.

See the [Flink 2.3 Time travel documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/time-travel/).


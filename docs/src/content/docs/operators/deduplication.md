---
title: Deduplication
description: Acceleration coverage and fallback behavior for Flink SQL Deduplication.
sidebar:
  order: 16
---

**Current status:** Not accelerated; executed by Flink.

**Future acceleration target:** Yes.

## SQL example

```sql
SELECT * FROM (\n  SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_time DESC) AS row_num\n  FROM events\n) WHERE row_num = 1;
```

## Acceleration and fallback

Accelerate Flink's recognized first-row or last-row deduplication pattern when keys, time ordering, and update mode are supported. General ranking expressions and unsupported state retention fall back.

## Implementation

Keep only the selected row and ordering value per key in custom Rust state. Avoid materializing a full Top-N structure.

See the [Flink 2.3 Deduplication documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/deduplication/).

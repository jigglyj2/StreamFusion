---
title: Changelog conversion
description: Acceleration coverage and fallback behavior for Flink SQL Changelog conversion.
sidebar:
  order: 19
---

**Current status:** Not accelerated; executed by Flink.

**Can it be accelerated?** Not currently a compute target.

## SQL example

```sql
SELECT * FROM TABLE(\n  FROM_CHANGELOG(TABLE raw_changes, DESCRIPTOR(op))\n);
```

## Acceleration and fallback

FROM_CHANGELOG and TO_CHANGELOG remain in Flink. Adjacent relational operators may accelerate if row kinds can cross the native boundary without information loss.

## Implementation

Preserve Flink RowKind and operation mappings at the boundary. Native execution must never collapse updates or deletes into append-only rows.

See the [Flink 2.3 Changelog conversion documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/changelog/).


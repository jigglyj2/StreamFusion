---
title: WITH clause
description: Acceleration coverage and fallback behavior for Flink SQL WITH clause.
sidebar:
  order: 4
---

**Current status:** Not accelerated; executed by Flink.

**Can it be accelerated?** Not directly. A CTE is a planning construct rather than a runtime operator.

## SQL example

```sql
WITH expensive AS (\n  SELECT * FROM bid WHERE price > 1000\n)\nSELECT COUNT(*) FROM expensive;
```

## Acceleration and fallback

Flink expands and optimizes the CTE. StreamFusion may accelerate eligible operators in the resulting plan. Recursive, reused, or otherwise unsupported expansions remain entirely in Flink.

## Implementation

No native CTE implementation is planned. Keeping name resolution and expansion in Flink avoids duplicating planner behavior.

See the [Flink 2.3 WITH clause documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/with/).


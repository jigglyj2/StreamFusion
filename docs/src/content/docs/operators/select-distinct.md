---
title: SELECT DISTINCT
description: Acceleration coverage and fallback behavior for Flink SQL SELECT DISTINCT.
sidebar:
  order: 3
---

**Current status:** Not accelerated; executed by Flink.

**Can it be accelerated?** Yes.

## SQL example

```sql
SELECT DISTINCT bidder\nFROM bid;
```

## Acceleration and fallback

Accelerate when keys have a parity-tested Arrow representation and the required changelog can be emitted exactly. Unsupported key types, unbounded state without a compatible retention policy, or changelog mismatches force fallback.

## Implementation

Use DataFusion distinct for bounded input. Streaming input needs custom Rust keyed state, with state lifecycle and snapshots controlled through Flink.

See the [Flink 2.3 SELECT DISTINCT documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/select-distinct/).


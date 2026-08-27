---
title: Model inference
description: Acceleration coverage and fallback behavior for Flink SQL Model inference.
sidebar:
  order: 21
---

**Current status:** Not accelerated; executed by Flink.

**Can it be accelerated?** Provider-dependent.

## SQL example

```sql
SELECT * FROM TABLE(\n  ML_PREDICT(TABLE observations, MODEL fraud_model)\n);
```

## Acceleration and fallback

Accelerate only when a supported model provider has an equivalent native runtime and output/error semantics. Remote providers, unsupported tensor types, or provider-specific configuration fall back.

## Implementation

A future native provider could consume Arrow batches directly and avoid row conversion. Until parity and lifecycle behavior are proven, Flink's model provider remains authoritative.

See the [Flink 2.3 Model inference documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/model-inference/).


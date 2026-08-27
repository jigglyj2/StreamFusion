---
title: Vector search
description: Acceleration coverage and fallback behavior for Flink SQL Vector search.
sidebar:
  order: 22
---

**Current status:** Not accelerated; executed by Flink.

**Can it be accelerated?** Potentially.

## SQL example

```sql
SELECT * FROM VECTOR_SEARCH(\n  TABLE documents, DESCRIPTOR(embedding),\n  TABLE queries, DESCRIPTOR(embedding), 10\n);
```

## Acceleration and fallback

Accelerate when vector type, distance metric, Top-K semantics, connector/index behavior, and result ordering are supported. Provider-specific indexes or unsupported metrics fall back.

## Implementation

Use DataFusion vector expressions or custom SIMD/index kernels over Arrow arrays. Keep connector-managed index access in Flink unless the native implementation preserves its consistency model.

See the [Flink 2.3 Vector search documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/vector-search/).


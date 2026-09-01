---
title: SELECT DISTINCT
description: Acceleration coverage and fallback behavior for Flink SQL SELECT DISTINCT.
sidebar:
  order: 3
---

**Current status:** Accelerated for timer-free streaming `SELECT DISTINCT`.

## SQL example

```sql
SELECT DISTINCT bidder
FROM bid;
```

## Acceleration and fallback

Flink lowers streaming `SELECT DISTINCT` to a keyed group aggregate with no aggregate calls.
StreamFusion recognizes that exact physical shape and stores one signed occurrence count for each
distinct row. The first `INSERT` or `UPDATE_AFTER` emits `INSERT`, duplicate accumulations only
increment the count, and the final matching `UPDATE_BEFORE` or `DELETE` emits `DELETE`. A
retraction for absent state is ignored, matching Flink after cleanup or state expiry.

Nullable scalar, character/binary, decimal, temporal, local-zoned timestamp, interval, array, map,
multiset, row, distinct, structured, and nested keys use the same complete logical-type coverage as
native keyed state. Types with a native Flink-compatible Arrow key encoding are encoded in Rust;
the remainder are passed as opaque canonical `BinaryRowData` key bytes. Output columns are gathered
directly from the input Arrow batch, so Java neither decodes state nor reconstructs complex values.

State TTL, mini-batching, async state, and Flink's changelog-state wrapper remain explicit fallback
conditions. Global zero-column distinct plans are not accepted by this keyed implementation.

## Implementation

The counted-membership transition has its own native operator module and reuses StreamFusion's
backend-neutral keyed-state interface. `HashMapStateBackend` selects the native in-memory byte map;
`EmbeddedRocksDBStateBackend` selects direct native RocksDB. Each Arrow batch deduplicates its keys
with `ahash`, performs one multi-get, applies rows in changelog order, and performs one batched
write/delete operation.

Both backends use Flink key groups and the same canonical state payload. Consequently SELECT
DISTINCT inherits cross-backend canonical savepoints, aligned and unaligned checkpoints,
incremental RocksDB SST reuse, and 1-to-N/N-to-1 rescaling from native keyed group state. Its Arrow,
state, scratch, RocksDB cache/write-buffer, checkpoint, and restore memory is charged to Flink
managed memory. Logical input/output and changelog counters, batched state operations, backend,
managed-memory, checkpoint, restore, and failure metrics are exposed under the operator's
StreamFusion metric group.

See the [Flink 2.3 SELECT DISTINCT documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/select-distinct/).

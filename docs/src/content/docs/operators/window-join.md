---
title: Window join
description: Acceleration coverage and fallback behavior for Flink SQL Window join.
sidebar:
  order: 10
---

**Current status:** Accelerated for Flink's event-time Window Join physical node.

## SQL example

```sql
SELECT l.window_start, l.id, r.value
FROM TABLE(TUMBLE(TABLE left_input, DESCRIPTOR(ts), INTERVAL '10' SECOND)) l
JOIN TABLE(TUMBLE(TABLE right_input, DESCRIPTOR(ts), INTERVAL '10' SECOND)) r
ON l.id = r.id
AND l.window_start = r.window_start
AND l.window_end = r.window_end;
```

## Acceleration and fallback

StreamFusion accelerates attached `TUMBLE`, `HOP`, `CUMULATE`, and `SESSION` inputs when Flink
recognizes the equality predicates on the join keys, `window_start`, and `window_end` as a Window
Join. Inner, left, right, full, semi, and anti join modes use Flink's generated remaining-condition
code, null-key filtering, and output row layout. Arbitrary nullable scalar and nested join keys and
payloads use schema-aware Arrow-row state and are covered byte-for-byte across the complete Flink
logical-type surface.

`INSERT`, `UPDATE_AFTER`, `UPDATE_BEFORE`, and `DELETE` inputs use exact multiset semantics,
including duplicate rows. Results are emitted once when the coalesced two-input watermark closes a
window. Flink 2.3 currently rejects updating children before it creates a Window Join physical
node, so SQL-reachable plans are append-only; the complete native changelog contract is covered
directly for restore/rescaling and is ready if that planner restriction changes. Each side has an
independent late-record counter and rate. Flink does not plan
processing-time Window Join; a non-attached strategy, async state, changelog-state wrapping, or an
unsupported surrounding physical node produces an explicit whole-plan fallback reason.

## Implementation

Both network inputs remain Arrow IPC frames until they enter the two-input operator, so raw Arrow
batches never cross a Flink network edge. Rust computes the Flink key group and performs one
backend multi-get and one atomic write per incoming side batch. It keeps ordered duplicate Arrow
rows for both sides and returns one nullable Arrow candidate batch when a timer fires. Java applies
the generated Flink join condition through zero-copy Arrow-backed `RowData` views, then gathers the
final Arrow result directly from the candidate vectors. It does not serialize state as
`BinaryRowData`, transpose timer output, or reconstruct payload rows on the JVM.

The managed in-memory and direct RocksDB backends share canonical per-key-group row and timer
bytes. Aligned and unaligned checkpoints, cross-backend savepoints, and key-group redistribution
therefore preserve both sides and pending timers; direct RocksDB checkpoints retain incremental SST
reuse. State, timers, encoded Arrow rows, hash collections, temporary join materialization, and
exported Arrow buffers are charged through Flink managed memory.

See the [Flink 2.3 Window join documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-join/).

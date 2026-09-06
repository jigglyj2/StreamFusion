---
title: Window aggregation
description: Acceleration coverage and fallback behavior for Flink SQL Window aggregation.
sidebar:
  order: 7
---

**Current status:** Partially accelerated for native `TUMBLE`, `HOP`, `CUMULATE`, and `SESSION`
aggregation, including Flink's legacy group-window physical node.

## SQL example

```sql
SELECT window_start, bidder, SUM(price)
FROM TABLE(TUMBLE(TABLE bid, DESCRIPTOR(dateTime), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, bidder;
```

## Acceleration and fallback

StreamFusion accelerates direct time-attribute window aggregation for event time and processing
time. It recognizes both Flink's one-phase node and its default local-aggregate, exchange,
global-aggregate shape. For Flink's two-phase plan, StreamFusion preserves all three stages: a
state-free native local aggregate emits one opaque canonical accumulator per key and base slice,
the existing key-group exchange partitions those partials, and the native global aggregate merges
them into keyed window state. Partial batches stay in Arrow and do not cross JNI between adjacent
native stages.
It also accelerates an aggregate over already attached `window_start` and `window_end` columns, as
produced by a preceding window aggregate. Each attached pair is one exact namespace; it is not
assigned to overlapping windows a second time. This covers the nested hopping aggregation in
Nexmark q5 while keeping both stages as independently observable nodes in one fused native tree.
Legacy SQL/Table API time windows lower to the same canonical native state machine. Legacy Table
API processing-time row-count tumbling and sliding windows are also accelerated; Flink 2.3's SQL
grammar does not expose numeric row-count intervals, but its Table API and physical executor do.
In bounded mode, Flink's hash- and sort-based legacy time-window executors are accelerated in both
one- and two-phase forms. StreamFusion never substitutes one form for the other. A Flink one-phase
node becomes one stateful native aggregate; a Flink local/exchange/global plan retains all three
physical stages. In the two-phase form the native local phase emits opaque accumulator bytes, the
Arrow exchange partitions them by Flink key group, and the native global phase merges them. In the
one-phase form the final node consumes either the existing in-task Arrow stream or, when Flink
requires redistribution, decodes the Arrow IPC frame directly at its native-plan edge. Tumbling and
pane-based sliding windows share these paths and emit only at end of input. As in Flink's bounded
executor, intermediate watermarks are forwarded as control records and do not fire or evict
windows before end of input.
Flink auxiliary grouping values in a bounded two-phase plan travel beside the true grouping key in
that opaque Arrow partial. They are restored by the global phase but do not participate in hash
partitioning, key-group assignment, or keyed-state identity.
The native calls are `COUNT(*)`, `COUNT(value)`, `SUM`, `AVG`, `MIN`, and `MAX`, including SQL
`FILTER (WHERE ...)` with nullable Boolean predicates. `AVG` accepts every Flink numeric input,
supports retractions, and merges its sum/count buffers across session namespaces. Keys use Arrow's canonical row encoding and
include nullable scalar, decimal, temporal, binary, array, map, multiset, row, and nested SQL
values. Input `INSERT`, `UPDATE_BEFORE`, `UPDATE_AFTER`, and `DELETE` kinds are supported when Flink
selects retractable accumulators.

Window start, end, row-time, and processing-time properties are supported. Null event timestamps,
late rows, watermark cleanup, timer ordering, offsets, negative epochs, `TIMESTAMP_LTZ`, configured
local time zones, and daylight-saving gaps/overlaps follow Flink. Session windows perform
transitive merging and keep Flink's merged namespace when a bridging row retracts.

Legacy early/late firing, distinct/approximate or user-defined aggregate calls, async state,
changelog-state wrapping, and unsupported surrounding physical nodes produce an explicit
whole-plan fallback reason. Row-count windows do not expose time window properties, matching
Flink's legacy contract.

## Implementation

The Java planner serializes the complete physical contract in the versioned plan protobuf. Rust
batches records by key and window, computes Flink key groups, and uses the shared opaque keyed-state
interface. Both the managed in-memory backend and the optional direct RocksDB component perform
batched reads and atomic batched mutations without per-record JNI state calls.
Row-count windows store their per-key element index in the same canonical key-group envelope; each
input batch performs one batched index read, one batched window-state read, and one atomic write.
Time windows perform one batched state read and one atomic state/timer write per input batch. The
hot input loop reuses Flink BinaryRow-key and assigned-window scratch buffers, while canonical
state keys receive owned storage only when a new key/window is staged.

The local half of a two-phase time window is deliberately state-free across Arrow batches. Its
temporary hash table and output buffers are charged to the local stage's Flink managed-memory
share. The global half is the sole owner of canonical keyed state and timers, so aligned and
unaligned checkpointing, savepoint restoration, backend switching, and rescaling use exactly the
same state format as one-phase execution. One-phase execution owns that canonical keyed state
directly. RocksDB performs one batched read and one atomic batched write per incoming raw or partial
Arrow batch.

Timer firing is bounded to 4,096 namespaces per output batch. If one watermark or bounded
end-of-input closes more windows, Java repeatedly advances the same native timer frontier and emits
multiple Arrow batches. This keeps state reads, mutations, and output allocation inside Flink's
managed-memory grant without changing deterministic timer order.

A backend-neutral native timer service stores event-time and processing-time timers per key group.
Its canonical bytes travel with raw keyed snapshots, so aligned and unaligned checkpoints,
cross-backend canonical savepoints, and 1-to-2-to-1 rescaling preserve pending windows. Direct
RocksDB checkpoints reuse immutable SST handles incrementally. Timer, state, scratch, and exported
Arrow allocations are charged through Flink managed memory. Append-only sessions merge compact
accumulators; changelog sessions additionally retain the exact event contributions required for
retractions. Stateful window aggregates request the same relative Flink operator-memory weight as
native deduplication, grouped aggregation, and Top-N; this prevents nested attached-window plans
from receiving a stateless stage's undersized share while retaining Flink's single managed-memory
budget and admission control.

Attached-window coverage includes generated byte-for-byte SQL parity, all four changelog kinds in
the native state test, and canonical memory-to-RocksDB restoration. The restoration test also
asserts that no additional HOP namespaces survive after the single attached namespace fires.
Bounded one- and two-phase coverage compares Flink and StreamFusion results for fixed-width
tumbling, variable-width tumbling, and pane-based sliding aggregates on both state backends. The
tests force Flink's phase strategy: one-phase runs require a zero local-stage count, while
two-phase runs require both native phase counters to be nonzero. A nested aggregate case forces an
auxiliary grouping field through the local/exchange/global contract. Each sliding case additionally
runs with distributed parallelism four to exercise the key-group exchange. The direct one-phase
and framed global operators have aligned, unaligned, and canonical savepoint restore coverage,
including memory-to-RocksDB and RocksDB-to-memory restore; the framed path also covers 1-to-2-to-1
rescaling restore.

See the [Flink 2.3 Window aggregation documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-agg/).

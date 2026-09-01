---
title: Window aggregation
description: Acceleration coverage and fallback behavior for Flink SQL Window aggregation.
sidebar:
  order: 7
---

**Current status:** Partially accelerated for native `TUMBLE`, `HOP`, `CUMULATE`, and `SESSION`
aggregation.

## SQL example

```sql
SELECT window_start, bidder, SUM(price)
FROM TABLE(TUMBLE(TABLE bid, DESCRIPTOR(dateTime), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, bidder;
```

## Acceleration and fallback

StreamFusion accelerates direct time-attribute window aggregation for event time and processing
time. It recognizes both Flink's one-phase node and its default local-aggregate, exchange,
global-aggregate shape; the latter is collapsed so original Arrow rows reach one native operator.
The native calls are `COUNT(*)`, `COUNT(value)`, `SUM`, `MIN`, and `MAX`. Keys use Arrow's canonical
row encoding and include nullable scalar, decimal, temporal, binary, array, map, multiset, row, and
nested SQL values. Input `INSERT`, `UPDATE_BEFORE`, `UPDATE_AFTER`, and `DELETE` kinds are supported
when Flink selects retractable accumulators.

Window start, end, row-time, and processing-time properties are supported. Null event timestamps,
late rows, watermark cleanup, timer ordering, offsets, negative epochs, `TIMESTAMP_LTZ`, configured
local time zones, and daylight-saving gaps/overlaps follow Flink. Session windows perform
transitive merging and keep Flink's merged namespace when a bridging row retracts.

Legacy group-window nodes, distinct/filter/approximate or user-defined aggregate calls, async
state, changelog-state wrapping, and unsupported surrounding physical nodes produce an explicit
whole-plan fallback reason.

## Implementation

The Java planner serializes the complete physical contract in the versioned plan protobuf. Rust
batches records by key and window, computes Flink key groups, and uses the shared opaque keyed-state
interface. Both the managed in-memory backend and the optional direct RocksDB component perform
batched reads and atomic batched mutations without per-record JNI state calls.

A backend-neutral native timer service stores event-time and processing-time timers per key group.
Its canonical bytes travel with raw keyed snapshots, so aligned and unaligned checkpoints,
cross-backend canonical savepoints, and 1-to-2-to-1 rescaling preserve pending windows. Direct
RocksDB checkpoints reuse immutable SST handles incrementally. Timer, state, scratch, and exported
Arrow allocations are charged through Flink managed memory. Append-only sessions merge compact
accumulators; changelog sessions additionally retain the exact event contributions required for
retractions.

See the [Flink 2.3 Window aggregation documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-agg/).

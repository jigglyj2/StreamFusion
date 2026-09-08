---
title: Window Top-N
description: Acceleration coverage and fallback behavior for Flink SQL Window Top-N.
sidebar:
  order: 15
---

**Current status:** Temporarily uses whole-plan Flink fallback under the
[architecture admission requirements](/StreamFusion/development/architecture-admission/). The native paths
described below are retained for development and direct parity tests; SQL planning does not select them.

**Retained implementation scope:** Implementation for Flink's event-time Window Top-N physical node.

## SQL example

```sql
SELECT *
FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY window_start, window_end ORDER BY total DESC
  ) AS rank_num
  FROM window_totals
)
WHERE rank_num <= 3;
```

## Acceleration and fallback

Constant-range `ROW_NUMBER` plans over attached `TUMBLE`, `HOP`, `CUMULATE`, and `SESSION` windows
have retained native implementations, with or without the rank-number output column. All partition-key and payload types
supported by the RowData/Arrow boundary are accepted. Sort fields retain Flink's generated
comparison semantics in Rust, including ascending/descending direction, null placement, binary
string ordering, decimal/temporal semantics, composite values, and stable input-order ties.

`RANK`, `DENSE_RANK`, variable rank ranges, processing-time Window Top-N (which Flink does not plan),
async-state mode, and Flink's changelog-state wrapper fall back with an EXPLAIN reason.

## Implementation

Native memory or direct RocksDB stores candidates using Arrow's schema-aware row format.
All four input RowKinds are applied, so a retraction can expose a previously displaced candidate.
The ordered path described below uses batched metadata reads, bounded identity scans, and one
atomic mutation batch per input. The comparator compatibility path retains whole-window buffering
and sorting. Both export visible Arrow columns plus RowKind metadata directly. Java does not
serialize sort keys or rows and does not reconstruct timer output.

Flink 2.3 rejects updating input before constructing the Window Top-N physical node, so current SQL
plans reach this path as append-only. Direct native changelog, restore, and rescaling tests cover
all four RowKinds for compatibility with future planner shapes.

State and timers use the canonical backend-neutral savepoint representation and the same aligned,
unaligned, incremental RocksDB, restore, and rescaling lifecycle as other native keyed operators.

See the [Flink 2.3 Window Top-N documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/window-topn/).

## Ordered candidate state

For scalar non-floating sort columns, native window rank stores each candidate under an
Arrow-encoded sort key with a stable sequence tie-breaker. Small window metadata and an exact
payload/sequence retraction index are separate entries. The latter retains payload identity bytes
as keys so duplicate retractions select the oldest matching candidate without scanning a window.
This trades additional index space for targeted reads.

At each input batch, the operator reads window metadata and only identities that the batch
retracts, performs transitions in input order, and commits candidate/index/timer mutations together.
At watermark time it scans candidates in order, decodes only ranks in the requested output range,
and deletes all candidate and identity entries. Cleanup still scans the entire expired window and
budgets the accumulated deletion batch. Equal sort keys preserve insertion order. Floating and
nested sort keys retain the existing Flink comparator path.

Both the ordered in-memory backend and RocksDB use the same versioned key/value representation.
Old whole-window values can fire directly or migrate atomically when touched by a new input batch.
Generated tests compare serialized output with Flink SQL after null-key retractions and restoration
in both backend directions. Existing production admission and recovery gates remain in force.

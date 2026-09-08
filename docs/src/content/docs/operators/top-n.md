---
title: Top-N
description: Acceleration coverage and fallback behavior for Flink SQL Top-N.
sidebar:
  order: 14
---

**Current status:** Temporarily uses whole-plan Flink fallback under the
[architecture admission requirements](/StreamFusion/development/architecture-admission/). The native paths
described below are retained for development and direct parity tests; SQL planning does not select them.

**Retained implementation scope:** Implementation for Flink streaming `ROW_NUMBER` Top-N, including partitioned and
global Top-N, constant ranges (including `OFFSET`), variable per-partition upper bounds, rank-number
output, and Flink's append-fast, update-fast, and retract strategies.
Bounded SQL `RANK` is also implemented through Flink's
local-sort/local-rank/hash-exchange/global-sort/global-rank plan.

Flink's unordered global `StreamExecLimit` specialization uses the same physical node and native
runtime. See [LIMIT](../limit/) for its counter-state and saturation behavior.

## SQL example

```sql
SELECT *
FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY bidder ORDER BY price DESC
  ) AS rank_num
  FROM bid
)
WHERE rank_num <= 3;
```

## Acceleration and fallback

StreamFusion replaces `StreamExecRank` only when Flink selected `ROW_NUMBER` and a constant or
variable rank range. Flink 2.3 does not implement streaming `RANK` or `DENSE_RANK` in this physical
operator, so those shapes retain Flink's own planning error/fallback rather than being approximated.
General OVER expressions remain separate operators.

For bounded `RANK`, the planner retains Flink's hash or singleton exchange and replaces the paired
local/global sort-rank stages with one keyed, tie-aware bounded selection. It only performs this
rewrite when local and global partition/order keys match, the local range begins at one and covers
the global cutoff, and the exchange establishes final key ownership. Otherwise the whole plan
falls back. SQL rank gaps and all ties at the cutoff are preserved; `OFFSET`, an optional BIGINT rank
column, duplicate rows, and INSERT, UPDATE_BEFORE, UPDATE_AFTER, and DELETE physical records are
supported. Every Arrow-supported type is accepted as payload. Rank keys accept the same complete
Flink-comparable type surface documented for [ORDER BY](../order-by/).

Stored payloads remain Arrow columns, including nested arrays, maps, multisets, and rows. Rust
implements Flink's comparison semantics for every logical family that Flink accepts in this
`ORDER BY`, including null placement, decimals, NaN and signed zero, temporal values, binary data,
and UTF-8 strings. Complex partition keys use an opaque Flink binary-key sidecar; Java never
interprets native state or transposes the payload. Retract mode matches complete rows and preserves
duplicate insertion order with a persisted sequence number.

## Implementation

Bounded rank computation now uses DataFusion's RANK partition evaluator, both for an already
sorted physical input and for terminal keyed bounded rank output. The evaluator persists across
Arrow batches and resets at Flink partition boundaries; Flink still owns rank-range filtering,
physical RowKinds and state/recovery. Floating-point order keys, including nested floats, retain
the Flink peer adapter: its generated comparator treats signed zeros and NaN/finite pairs as
peers, whereas DataFusion scalar equality compares floating bits. Generated native/SQL parity
tests cover integer peers and partition boundaries split across batches. This compute change
does not remove the existing planner admission gates.


Each incoming Arrow batch crosses JNI once. Rust computes Flink-compatible key groups, reads the
touched partitions, maintains candidate sets, and commits changes in one backend batch. For
supported scalar sort keys (excluding floats), sort columns are Arrow-row-encoded once per input
or restored batch and compared as bytes. Top-level null placement is independent of ascending or
descending direction, matching Flink.

These partitions store small versioned metadata separately from individually ordered candidate
entries. RocksDB uses its bytewise comparator; the in-memory implementation uses a B-tree.
Unchanged candidates are not rewritten. Legacy whole-partition values migrate when first touched.
Float/nested sort keys retain the custom comparator and whole-partition representation; unordered
LIMIT retains its existing specialized behavior. Native state never passes through JNI.

Top-N still loads the retained candidate set of each touched partition and uses a sorted candidate
vector. This change does not implement RisingWave's bounded low/middle/high cache or eliminate
large retractable-group working sets. Terminal rank discovery scans only partition metadata,
then reads the corresponding ordered candidates. Payload decoding/output remain vectorized.

The implementation was gut-checked against RisingWave's non-window Top-N state/cache split: both
keep deterministic `(ORDER BY, remaining primary key)` ordering and retain enough state to refill
the visible range after a retraction. StreamFusion preserves Flink's comparator contract in Rust
and its backend-neutral key-group state contract instead of adopting RisingWave's table-specific
low/middle/high caches. Arroyo currently provides windowed Top-N operators, but no corresponding
unbounded non-window SQL Top-N implementation. The transformation requests a stateful relative
weight of eight from Flink's existing `OPERATOR` managed-memory pool, preventing wide-row state
from being constrained to the share intended for a stateless unary stage without introducing a
separate StreamFusion memory budget.

Both backends use canonical key-group savepoints and support 1-to-N-to-1 rescaling, aligned and
unaligned checkpoints, and cross-backend restoration. RocksDB regular checkpoints are incremental
and reuse completed SST handles. The operator declares Flink operator managed memory; native state,
scratch buffers, Arrow transfers, and RocksDB's cache are governed by that allocation.

The Flink metric surface follows the selected rank strategy: `topn.invalidTopSize` is always
present; append-fast and update-fast also expose `topn.cache.hitRate` and `topn.cache.size`, while
retract mode does not. StreamFusion's separate metric subgroup reports comparator calls,
loaded/committed/expired groups, invalid retractions, logical changelog counts, native state batches,
managed-memory usage, and checkpoint/restore data. Standard input/output counters count logical
rows rather than internal Arrow batches.

Bounded rank uses the same raw keyed-state and canonical checkpoint implementation. Its terminal
drain is emitted in managed Arrow batches. In addition to logical I/O it publishes the three Flink
sort gauges (`memoryUsedSizeInBytes`, `numSpillFiles`, and `spillInBytes`) with actual managed-memory
usage and zero spill values, plus additive loaded/committed group, comparator, invalid-retraction,
emitted-row, native-invocation, backend, and checkpoint diagnostics.

See the [Flink 2.3 Top-N documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/topn/).

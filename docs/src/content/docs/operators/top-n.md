---
title: Top-N
description: Acceleration coverage and fallback behavior for Flink SQL Top-N.
sidebar:
  order: 14
---

**Current status:** The verified append-only, partitioned `ROW_NUMBER` Top-1 subset accelerates
through the shared native runtime on memory and default RocksDB state. Other Top-N/Rank subsets
retain whole-plan fallback under the [architecture admission requirements](/StreamFusion/development/architecture-admission/).

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
WHERE rank_num <= 1;
```

## Acceleration and fallback

Production selection requires Flink's append-fast strategy, constant range `[1,1]`, a nonempty
partition key and explicit ordering. Partition keys accept BIGINT/INTEGER; payload and order
columns accept BIGINT, INTEGER, VARCHAR and TIMESTAMP(3) without processing-time attributes.
Nullable keys, mixed order directions, optional rank output and optional UPDATE_BEFORE are
verified. State TTL, asynchronous state and mini-batching must be disabled; a nondefault
`table.exec.rank.topn-cache-size` retains fallback because its cache configuration is not
represented by the native point-state implementation. Common backend and metric-option gates
still apply. EXPLAIN reports the reason for every unsupported node in the complete fallback plan.

Global rank/LIMIT, larger/variable ranges, retract/update-fast strategies and other types remain
gated. Flink 2.3 does not implement streaming `RANK` or `DENSE_RANK` in this physical operator;
those shapes retain Flink's own planning error. The following broader implementations remain
available for direct development/parity tests and are not production-admitted.

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


The retained append-only `ROW_NUMBER` Top-1 path now delegates winner selection to DataFusion's
sort kernel and cumulative `MIN` window expression. It sorts Arrow-encoded order keys with stable
arrival ordinals, then evaluates fixed-width priority ordinals per touched partition. An old winner
precedes new equal keys; each strict improvement retains its original input position for Flink's
INSERT/UPDATE changelog. The cumulative result never repeats a large winning string key for every
later row. A 64 KiB winner followed by 4,095 arrivals stays below 4 MiB of observed native allocation
in a constrained-memory regression. Other ranges, retract/update strategies, bounded-final modes
and comparator-incompatible types keep their existing implementations and planner gates.

Generated Flink FastTop1Function comparisons cover complete per-arrival changelog bytes, nullable
partition/sort keys, mixed sort directions, timestamp endpoints, distinct payloads at tied keys,
optional rank output and multiple Arrow batch sizes on both backends.

The shared append-only Top-1 binding now composes between native Calc stages. Java emits a
protobuf fragment and binds its state to the common native runtime; intermediate Arrow output
retains its native memory credit without transferring it to Java. Native tests cover both envelope
versions, triggering-record timestamps on UPDATE_BEFORE/UPDATE_AFTER, canonical cross-backend
restore, active-invocation exclusion, cancellation, invalid changelog input and coarse large-buffer
admission failure. Invalid shared modes and schemas are rejected before backend construction.

Generated comparisons against Flink's FastTop1Function cover the complete registered stage metric
surface, logical I/O counts, ordered changelog with timestamp envelopes, watermarks/status, latency
markers, pre-barrier and terminal controls across both backends and all rank-output/update-before
flags. The cache gauges retain Flink 2.3's registration-time definitions: its FastTop1 helper
registers an empty cache and captured request/hit counters, yielding size 0 and hit rate 1.0.
The topology guard verifies one Arrow runtime and one state owner for Calc → Top-1 → Calc.

The shared Top-1 path now stores one winner in one point value per partition, matching Flink's
FastTop1 ValueState specialization. One backend read batch loads every touched winner; one write
batch stores only partitions whose final winner changed. Losing arrivals write no payload or
sequence metadata. Only final changed winners are gathered and Arrow-row-encoded for state;
intermediate changelog transitions still retain every original arrival. This is a bounded
single-candidate state specialization, not an opaque growing Top-N partition value.

The point value reuses versioned SFTN v5 and reads v4/v5 snapshots. Older ordered Top-1 entries
are loaded and removed on their first touched batch; subsequent batches perform no range scans.
General Top-N keeps its ordered index and separate candidate payloads. Both memory and RocksDB
instrumentation verify one read batch for 128 keys, zero writes for losing arrivals, and one
changed-winner write. Coarse batch admission covers overlapping sort keys, winner gathers,
state encoding and output buffers.

Generated recovery tests compare complete changelog bytes against uninterrupted Flink across
canonical backend switches, aligned and unaligned checkpoints, incremental RocksDB snapshots,
and 1→2→1 key-group rescaling. Real Flink task tests capture Arrow IPC channel state between
barriers and replay winning updates exactly once alongside the restored native state. Both
backends run three input seeds, including nullable keys, equal order keys and timestamp envelopes.

Generated SQL tests require ordinary selection and match complete collected changelog bytes
on both backends, including nullable/tied keys and parallelism one/two. Official Q9 integration
requires acceleration and identical final keyed result bytes at parallelism one/four on both
backends. Its independently scheduled join inputs can produce different intermediate winning-bid
transitions even across repeated unmodified Flink runs. The fixed-arrival operator tests retain
complete changelog comparisons; the independent-job test does not claim identical transient
changelogs. The shared fragment remains restricted to the production subset described above. The existing `topNComparatorCalls`
diagnostic counts adapter comparator calls; it does not count comparisons inside DataFusion kernels.
The [Q9 release comparison](/StreamFusion/benchmarks/q9-rowdata/) documents bounded measurements,
profiles, substantial timing variation and the remaining million-event join-state capacity limit.


Arrow batches cross JNI only at native-plan edges; adjacent native stages share Arrow buffers. Rust computes Flink-compatible key groups, reads the
touched partitions, maintains candidate sets, and commits changes in one backend batch. For
supported scalar sort keys (excluding floats), sort columns are Arrow-row-encoded once per input
or restored batch and compared as bytes. Top-level null placement is independent of ascending or
descending direction, matching Flink.

The retained general Top-N partitions store small versioned metadata separately from individually ordered candidate
entries. RocksDB uses its bytewise comparator; the in-memory implementation uses a B-tree.
Unchanged candidates are not rewritten. Legacy whole-partition values migrate when first touched.
Float/nested sort keys retain the custom comparator and whole-partition representation; unordered
LIMIT retains its existing specialized behavior. Native state never passes through JNI.

The retained general Top-N path still loads the retained candidate set of each touched partition and uses a sorted candidate
vector. This change does not implement RisingWave's bounded low/middle/high cache or eliminate
large retractable-group working sets. Terminal rank discovery scans only partition metadata,
then reads the corresponding ordered candidates. Payload decoding/output remain vectorized.

The implementation was gut-checked against RisingWave's non-window Top-N state/cache split: both
keep deterministic `(ORDER BY, remaining primary key)` ordering and retain enough state to refill
the visible range after a retraction. StreamFusion preserves Flink's comparator contract in Rust
and its backend-neutral key-group state contract instead of adopting RisingWave's table-specific
low/middle/high caches. Arroyo currently provides windowed Top-N operators, but no corresponding
unbounded non-window SQL Top-N implementation. The legacy standalone transformation requests a stateful relative
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

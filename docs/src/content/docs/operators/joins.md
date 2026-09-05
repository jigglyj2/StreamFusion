---
title: Joins
description: Acceleration coverage and fallback behavior for Flink SQL Joins.
sidebar:
  order: 9
---

**Current status:** Partially accelerated for bounded hash/adaptive/nested-loop joins and for
synchronous regular, multi-way, time-bounded, and temporal streaming joins.

## SQL example

```sql
SELECT b.bidder, b.price, p.name
FROM bid AS b
JOIN person AS p ON b.bidder = p.id;
```

## Acceleration and fallback

StreamFusion currently accelerates Flink's synchronous regular streaming `INNER`, `LEFT`,
`RIGHT`, `FULL`, `SEMI`, and `ANTI` joins when both sides use non-unique multiset state. A join may
combine its non-empty equi key with a generated residual predicate; the residual is evaluated over
the concatenated left/right row with SQL three-valued Boolean semantics, including treating `NULL`
as non-matching.
Stored rows may use any Arrow-representable Flink scalar or nested logical type, while join keys
may use every such type that Flink accepts for SQL equality. The native operator accepts the
complete insert/update-before/update-after/delete changelog, retains
duplicates, applies Flink's per-key null filtering, and reproduces Flink's null-padding and
association-count transitions.

Flink `BatchExecHashJoin` and `BatchExecAdaptiveJoin` equality joins use the same native two-sided
counted state with terminal output. `BatchExecNestedLoopJoin` uses one singleton key group and
evaluates its complete predicate as a vectorized residual condition. Bounded `INNER`, `LEFT`,
`RIGHT`, `FULL`, `SEMI`, and `ANTI` results are emitted as insert-only Arrow batches after both
inputs end. Duplicate rows, null join semantics, residual predicates, and all four input row kinds
are supported. Terminal output is capped at 16,384 rows per Arrow batch, including for one hot
cross-product key.

Flink `StreamExecIntervalJoin` plans are accelerated for constant row-time or processing-time
bounds and `INNER`, `LEFT`, `RIGHT`, or `FULL` joins. Both sides retain timestamp-ordered
multisets in native keyed state. Native event-time or processing-time cleanup timers delay outer
null rows until no future match can arrive, and retractions reverse both joined and previously
emitted outer rows using Flink-compatible association counts. Keys and stored rows have the same
complete Arrow-representable scalar and nested type coverage as regular joins.

Flink `StreamExecTemporalJoin` plans are accelerated for event-time `INNER` and `LEFT` temporal
table joins and processing-time `INNER` temporal table-function joins. Event-time probes retain
their left changelog until the two-input watermark makes the version lookup final; processing-time
probes read the current right version and honor Flink's idle-state retention interval. The native
operator accepts all four row kinds on either input, applies Flink null-key filtering, and supports
generated residual join conditions. A failed residual predicate drops an inner result or emits a
null-padded right row for a left join. Keys and stored rows support every Arrow-representable Flink
scalar and nested logical type.

Flink `StreamExecMultiJoin` plans are accelerated when all join predicates are represented by its
attribute-based equi-join map and all inputs share a non-empty partition key. The native recursive
operator supports Flink's `INNER` and `LEFT` chain shapes, duplicate multiset rows, all four row
kinds, SQL-null join semantics, and the null-padding retraction/insertion transitions of chained
left joins. Stored payloads and predicate fields accept every Arrow-representable Flink scalar and
nested logical type.

A two-input `StreamExecMultiJoin` with a common equi key is lowered to the regular native join.
This preserves its full generated residual condition and covers Flink's physical form for official
Nexmark q4 and q9. Three-or-more-input multi-joins continue to use the recursive operator and still
require every predicate to be represented by the attribute map.

The containing plan falls back to Flink with an EXPLAIN reason when a streaming regular join has no
usable equi key, state TTL, mini-batch execution, asynchronous state, changelog-state wrapping, or a
planner-provided unique/upsert key. Interval joins additionally fall back for a residual non-equi
condition, non-constant bounds, semi/anti join modes, mini-batching, asynchronous state, or
changelog-state wrapping. Temporal joins fall back for right/full/semi/anti modes, asynchronous
state, or changelog-state wrapping; Flink itself rejects processing-time temporal table joins, and
only its temporal table-function form is accepted there. Lookup joins remain Flink-owned. A
bounded nested-loop scalar-subquery join also remains on Flink because its single-row cardinality
failure contract is not yet native. These are explicit unimplemented shapes, not approximations.
Multi-way joins additionally fall back for residual predicates outside the attribute map,
planner-provided unique/upsert keys, non-zero state TTL, mini-batching, asynchronous state, or
changelog-state wrapping.

## Implementation

The planner replaces an eligible streaming or bounded join with a distinct StreamFusion exec node and sends
a versioned protobuf join contract to Rust. Each input crosses a native Arrow exchange edge; the
join itself receives Arrow batches and returns Arrow batches without a RowData loop or per-record
JNI call. A regular streaming join followed by one or more eligible Calc nodes is lowered as one
persistent native join handle with a reusable DataFusion Calc tail. Those stages exchange the
join's Arrow `RecordBatch` directly in Rust, so the fused edge adds neither a Java operator nor an
additional JNI round trip. The same path is used by bounded hash and nested-loop joins. Other
stateful operator families currently retain their own native handles and therefore do not yet
claim cross-operator fusion.

Rust stores an ordered multiset for both input sides under a Flink-compatible key group. One input
batch performs one distinct batched state read and one atomic batched write. The same opaque state
contract runs on managed native memory or direct native RocksDB. Canonical key-group snapshots move
between those backends and across parallelism, while ordinary RocksDB checkpoints use the shared
incremental-SST lifecycle. Aligned and unaligned checkpoints preserve join state and the two input
watermark frontiers. Regular joins do not register timers. Interval joins use the shared native
timer service and materialize dirty timer groups into canonical keyed state at snapshot boundaries,
avoiding repeated whole-group timer serialization during normal batch processing. Both variants
coalesce and forward watermarks using Flink's two-input rule.

Bounded hash/adaptive joins partition both sides by the planned Flink equality key. Bounded
nested-loop joins discard Flink's broadcast/ANY exchange wrapper and install a native singleton
exchange because the complete cross-product condition is evaluated in Rust. Neither path builds a
second Java hash table or sorter. Both memory and direct RocksDB state perform one distinct batched
read and one atomic batched write per incoming Arrow frame. Aligned, unaligned, and canonical
cross-backend restoration use the same SFS1 key-group bytes as streaming regular joins, and
ordinary RocksDB checkpoints retain incremental SST reuse.

The exchange frame is decoded directly by the native bounded join, so it does not become an Arrow
Java batch merely to cross JNI again. Primitive keys are encoded and assigned to Flink key groups
in Rust. For ROW and other Flink-equality-comparable complex keys, the exchange retains its already
computed opaque Flink `BinaryRowData` sidecar through this one native consumer edge; the same
transport also supports ARRAY, MAP, and MULTISET keys when used through a lower-level Flink runtime
contract that admits them. Rust consumes the bytes for equality and key-group ownership, and the
sidecar is never exposed in SQL output or by an ordinary exchange reader.
Aligned exchange can therefore continue coalescing key groups per destination, while unaligned
exchange can retain one frame per key group without changing the state format.

Regular-join residual predicates are encoded in the same versioned protobuf expression contract as
Calc and lowered to a DataFusion physical expression. The operator collects every candidate pair
for one input Arrow batch, decodes those pairs into one Arrow batch, and evaluates the predicate
once vectorially while retaining Flink's per-record state-transition order. Association counts and
outer/semi/anti transitions count only accepted candidates. Predicate scratch, state, and exported
Arrow output are all charged to the operator's Flink managed-memory reservation.
The regular and interval join transformations each request a stateful relative weight of eight
from Flink's existing `OPERATOR` pool; this is a share of the configured task memory, not a separate
StreamFusion memory setting.

Temporal joins use that same backend-neutral keyed-state and timer interface. Each incoming Arrow
batch performs one distinct batched state read and one atomic batched write. Right-side event-time
versions and pending left probes are encoded as opaque Arrow rows per Flink key group; the native
timer service releases and cleans them at the combined watermark. Processing-time state keeps the
current version plus its cleanup deadline. Canonical key-group savepoints restore interchangeably
between memory and RocksDB and across parallelism; aligned and unaligned checkpoints retain state,
timers, and both watermark frontiers, while ordinary RocksDB checkpoints reuse unchanged SSTs.
Residual conditions are evaluated by Flink's generated condition over Arrow-backed row views. When
all candidates pass, the bridge transfers the native output buffers without copying them; mixed
pass/fail results copy only the selected or null-padded output required by the predicate.

Multi-way joins reuse the same native keyed-state interface through a V2 multiple-input operator.
Each incoming Arrow batch performs one distinct backend multi-get and one atomic write batch for
all touched common keys. Per-input ordered multisets are encoded as opaque Arrow rows, while equi
predicate fields use opaque Flink binary-key sidecars so nested and non-native hash types never
cross JNI per row. The canonical key-group representation restores across parallelism and between
managed memory and direct RocksDB; RocksDB uses the shared incremental-SST checkpoint path.
The operator requests its scratch allowance from Flink's ordinary `OPERATOR` managed-memory pool
in proportion to its input count. State, decode scratch, recursive candidate traversal, and exported
Arrow output remain covered by that reservation; candidate traversal borrows stored rows rather
than cloning them, and native output reuses the columns produced by Arrow row decoding instead of
performing a second identity gather. Source-edge Arrow allowances scale with the physical nested
vector tree, so complete logical-type payloads remain admitted without creating a separate memory
budget.

Deterministic native transition tests cover every join type, residual acceptance and rejection,
duplicates, retractions, null keys, residual restore after rescaling, and canonical residual-state
migration from memory to RocksDB with batched I/O. Bounded coverage additionally checks terminal
insert-only results for all six join modes, pre-terminal retractions, null filtering, vectorized
residuals, hot-key output chunking, memory accounting, key-group rescaling, every checkpoint mode,
all four memory/RocksDB restore combinations, and direct exchange ingestion with an opaque ARRAY
key. Generated bounded SQL parity tests cover all six join modes, duplicate keys, nested payloads,
outer null padding, and a residual condition.
Interval coverage additionally exercises every
supported logical type as both a key and stored value, pending event-time and processing-time
timers, aligned and unaligned checkpoints, canonical cross-backend savepoints, and incremental
RocksDB restore. The generated SQL parity suite uses INNER and SEMI regular joins because their
result changelog is invariant to the test harness's independent two-input source scheduling;
outer and anti transition ordering is checked with controlled native input order. A deterministic
constant-bound interval-join workload compares the complete Flink and StreamFusion changelogs on
both state backends. Temporal tests cover both time modes, all row kinds, residual inner/left
semantics, every supported key and state type, managed-memory admission, metric parity, canonical
memory/RocksDB restoration, rescaling, aligned and unaligned checkpoints, and incremental RocksDB
SST reuse. Its SQL parity test compares the complete changelog against Flink on both backends.
Multi-way join tests cover three-input duplicate inner joins, ordered chained-left null-padding
transitions, missing retractions, SQL-null predicates, key-group rescaling, and canonical
state migration in all four memory/RocksDB source and target combinations. Java operator-harness
coverage checks the complete logical-I/O, row-kind, state-I/O, checkpoint, failure, watermark,
timer, and backend metric surface; aligned and unaligned restore on both backends; incremental
RocksDB SST reuse; and one-to-two-subtask key-group redistribution. Generated SQL tests require an
accelerated EXPLAIN and non-zero native batch count for both inner and left shapes; the left SQL
fixture uses disjoint right inputs so its complete changelog is deterministic despite independent
bounded-source scheduling.

Official Nexmark q4 and q9 integration cases compare the final keyed table against Flink on both
state backends, require accelerated EXPLAIN output, and require non-zero native regular-join plus
aggregate or Top-N batch counters. The result collector retains separate raw sorted and
arrival-order changelog hashes; its primary-key-aware materialization applies upserts before sorting
so a legal `UPDATE_AFTER` is not miscounted as another table row.

See the [Flink 2.3 Joins documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/joins/).

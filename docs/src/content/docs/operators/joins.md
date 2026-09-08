---
title: Joins
description: Acceleration coverage and fallback behavior for Flink SQL Joins.
sidebar:
  order: 9
---

**Current status:** Partial. Synchronous binary `INNER` joins represented by Flink's
`StreamExecMultiJoin` use the shared native plan with in-memory or default RocksDB state when their complete
condition is covered by common equi keys and optional boolean combinations of direct
column/literal comparisons or null checks. Computed residual operands retain a specific workspace
fallback. Both inputs must use non-unique multiset state.
Outer joins, TTL, mini-batching, async/changelog state, enabled
state-latency metrics, and checkpointing during channel recovery retain whole-plan fallback.
RocksDB requires its optional native component, a compatible verified CPU artifact, and supported
default backend settings. Unsupported settings retain their precise fallback reason.
Sources and sinks use the normal Arrow boundary adapters; join and downstream Calc exchange Arrow
directly within one native plan. Generated changelog/metric, rescaling, checkpoint and channel replay
tests cover this path. Q3 passes ordinary admission and collecting/blackhole integration on both
backends. The [Q3 release comparison](/StreamFusion/benchmarks/q3-rowdata/) records corrected
measurements and profiles for both, including small larger-run median gains and slower smaller runs.

All other join paths described below are retained for development and direct parity tests under
[architecture admission](/StreamFusion/development/architecture-admission/).

**Retained implementation scope:** Partial implementation for bounded hash/adaptive/sort-merge/nested-loop joins and for
synchronous regular, multi-way, time-bounded, and temporal streaming joins.

Generated shared-region conformance tests also cover binary inner joins with a nullable
`TIMESTAMP(3)` range predicate evaluated by DataFusion. They exercise inclusive endpoints,
negative epochs, null and non-matching bounds, a 5,000-row fan-out crossing the bounded
expression chunks, all four RowKinds, complete registered metrics, backend-switch savepoints,
1-to-2-to-1 rescaling, incremental SST reuse, and actual aligned/unaligned channel replay.
These tests support production admission for the bounded residual-comparison subset above.

Residual evaluation now also batches candidate pairs across consecutive incoming rows. A cache
holds at most 4,096 candidate pairs and 4,096 input descriptors, with one coarse reservation for
masks/descriptors and one for the Arrow expression workspace. Incoming columns use zero-copy
slices when pair indices are contiguous and Arrow gathers otherwise. Opposite-side stored payloads
are decoded in a batch. A single larger fan-out keeps the existing bounded expression chunks.
The state transitions still consume these masks in original input order, including outer/semi/anti
association counts in retained implementations. State writes and output draining retain their
existing batch boundaries, and the persisted format is unchanged.

The Q4 release profiles before this change attributed about 6–7% of process CPU samples to JVM
memory reservation callbacks. A native regression test reproduced 4,119 budget callbacks for
1,024 residual-filtered rows; the same workload now requires fewer than 64. This is callback-count
and parity evidence, not a throughput claim. Matched release measurements follow separately.

## SQL example

```sql
SELECT b.bidder, b.price, p.name
FROM bid AS b
JOIN person AS p ON b.bidder = p.id;
```

## Acceleration and fallback

The retained native runtime implements Flink's synchronous regular streaming `INNER`, `LEFT`,
`RIGHT`, `FULL`, `SEMI`, and `ANTI` joins when both sides use non-unique multiset state; production
selection remains subject to the architecture-admission restriction above. A join may
combine its non-empty equi key with a generated residual predicate; the residual is evaluated over
the concatenated left/right row with SQL three-valued Boolean semantics, including treating `NULL`
as non-matching.
Stored rows may use any Arrow-representable Flink scalar or nested logical type, while join keys
may use every such type that Flink accepts for SQL equality. The native operator accepts the
complete insert/update-before/update-after/delete changelog, retains
duplicates, applies Flink's per-key null filtering, and reproduces Flink's null-padding and
association-count transitions.

The two-input streaming bridge executes a persistent `RegularJoinExec` and downstream native fragments
inside one reusable native plan tree. Left/right ports are explicit protobuf children; native
stages share Arrow arrays directly and only the outer output stream transfers accounting to Java.
The join's logical output counters describe its own output before downstream stages, which have
separate stage counts. Selected regular joins now use the same fragment/state-capability interface
as deduplication, with one generic keyed-region runtime and no special lifecycle-owner fusion hook.
The region retains planned exchange frames and decodes them once at its input edge.
Metric-owner lookup follows the protobuf tree rather
than assuming a Calc tail. Protocol-v2 Calc fragments and the common native envelope preserve
RowKind through downstream projections and expansions. This foundation still requires complete
large-owner/metric admission. Direct topology tests combine join, Expand, Calc and deduplication in one
region, but arbitrary internal key transformations, bounded families, timers and full multi-stateful
SQL admission remain unfinished.

Streaming output reservations now follow shared Arrow buffer owners rather than the producer's
next pull. Retained projections, nested children, and slices stay charged after the producer closes;
no payload copy is introduced. The common edge recognizes registered owners, including imported
producer-owned buffers, and does not reserve their payload again. Large kernel workspaces still
require admission before allocation.

The common state binder copies only the join's configuration, not either physical child subtree.
Construction reserves decoded-plan memory before protobuf decoding and uses shared type-shape
admission before allocating Arrow schemas and row codecs. Wide/nested-schema allocation tests and
denied-construction cleanup tests cover those allowances independently of batch scratch. Residual
expression allocation and the remaining lifetime caches still require the complete admission audit.
Canonical paged-state restore drops its validation-only decoded pages and allowance before invoking
the backend restore. Both backends have a constrained-budget regression that allows either phase
but not both workspaces simultaneously; validation still finishes before any state is replaced.

The retained streaming join admits manifest/page lookup descriptors and dirty-state mutations
once per incoming batch. Output buffer allowances grow geometrically with a 64 KiB minimum
and remain held until that output chunk is emitted, avoiding a JVM reservation for each row
or touched key. Admission still precedes allocation, and budget failures require recovery.
A 1,024-key regression checks bounded broker-call counts alongside changelog and cleanup tests.

The retained two-input streaming runtime emits bounded Arrow C Stream batches, capped at 4,096
rows with a smaller row target for wide payloads. It does not first collect the complete fan-out.
Residual predicates use bounded vectorized candidate chunks, while equality-only/null-rejected
keys need no match bitmap. Input and historical-state memory remains charged until the input batch
is fully consumed. State uses stable per-side row IDs and 64-row pages. Batch admission fetches all
manifests in one lookup, then all referenced pages in a second lookup; the single end-of-batch
write contains only changed pages and changed manifest metadata. Retracting an early row does not
shift later pages, and association-count changes rewrite only their affected pages. Historical rows
are still decoded for touched keys, so this is a write-amplification fix, not a claim of constant
read or working-set cost for arbitrarily large keys. Checkpoints cannot observe a partially drained
batch. Cancellation or failure
requires task recovery; the stream safely retains native ownership if its Java handle is released.
Logical Flink I/O counters count records across all output chunks; StreamFusion's processed-batch
diagnostic counts each input once, not each output pull.

Canonical SFS1 snapshots now carry versioned `SFJM` manifests and `SFJP` pages, identical across native
memory and RocksDB. Restoring legacy whole-key `SFRJ` v1/v2 snapshots migrates them to the paged layout.
Restore rejects missing, duplicate, orphan, and malformed page records before changing backend state.
Physical RocksDB checkpoints retain the paged layout and the existing incremental checkpoint protocol.
The `StreamFusion.stateReadBatches` diagnostic counts actual backend lookups: one for a new key batch,
two when historical pages exist. `stateWriteBatches` counts non-empty backend write batches, so a
missing-row retraction that changes no state need not increment it.

Flink `BatchExecHashJoin`, `BatchExecAdaptiveJoin`, and `BatchExecSortMergeJoin` equality joins use
the same native two-sided counted state with terminal output while retaining distinct physical-node
identities for planning and metrics. Terminal matching now uses DataFusion `HashJoinExec`, including
its outer/semi/anti algorithms and residual `JoinFilter`, instead of a handwritten pair cursor. `BatchExecNestedLoopJoin` uses one singleton key group and
evaluates its complete predicate as a vectorized residual condition. Bounded `INNER`, `LEFT`,
`RIGHT`, `FULL`, `SEMI`, and `ANTI` results are emitted as insert-only Arrow batches after both
inputs end. Duplicate rows, null join semantics, residual predicates, and all four input row kinds
are supported. Terminal output is capped at 16,384 rows per Arrow batch, including for one hot
cross-product key.

The retained `StreamExecIntervalJoin` implementation supports constant row-time or processing-time
bounds and `INNER`, `LEFT`, `RIGHT`, or `FULL` joins. Both sides retain timestamp-ordered
multisets in native keyed state. Native event-time or processing-time cleanup timers delay outer
null rows until no future match can arrive, and retractions reverse both joined and previously
emitted outer rows using Flink-compatible association counts. Keys and stored rows have the same
complete Arrow-representable scalar and nested type coverage as regular joins.

The retained `StreamExecTemporalJoin` implementation supports event-time `INNER` and `LEFT` temporal
table joins and processing-time `INNER` temporal table-function joins. Event-time probes retain
their left changelog until the two-input watermark makes the version lookup final; processing-time
probes read the current right version and honor Flink's idle-state retention interval. The native
operator accepts all four row kinds on either input, applies Flink null-key filtering, and supports
generated residual join conditions. A failed residual predicate drops an inner result or emits a
null-padded right row for a left join. Keys and stored rows support every Arrow-representable Flink
scalar and nested logical type.

The retained `StreamExecMultiJoin` implementation supports plans where all join predicates are represented by its
attribute-based equi-join map and all inputs share a non-empty partition key. The native continuation-based
operator supports Flink's `INNER` and `LEFT` chain shapes, duplicate multiset rows, all four row
kinds, SQL-null join semantics, and the null-padding retraction/insertion transitions of chained
left joins. Stored payloads and predicate fields accept every Arrow-representable Flink scalar and
nested logical type.

A two-input `StreamExecMultiJoin` with a common equi key is lowered to the regular native join.
When its entire condition is covered by the common equality keys, lowering omits the redundant
residual predicate: keyed state lookup already enforces those equalities and null filtering. This
avoids decoding candidate payloads and evaluating an Arrow comparison for each probe. Conditions
with additional comparisons retain the complete DataFusion residual, including equalities outside
the partition-key map. This applies to binary equi-joins generally, including Flink's physical forms
for Nexmark q3, q4 and q9; it does not change production admission. Three-or-more-input multi-joins use the multi-way cursor and still
require every predicate to be represented by the attribute map.

Multi-way state now uses a per-key directory and independently persisted 256-row pages. Stable
row slots preserve insertion order; only dirty payload pages and their directory are written at
batch completion. Required directories and payload pages are read in batches before computation.
The output cursor retains join-depth continuation state and emits at most 4,096 rows or roughly
1 MiB of encoded payload per pull (one oversized row requires sufficient memory credit). It never
collects the complete Cartesian product. Output leases survive the processor if a consumer keeps
a batch. Checkpoint/restore requires a healthy, fully drained stream, and partial computation
failure requires recovery. The old single-batch harness rejects fan-out needing multiple pulls.

This paged format replaces the development-only whole-key multi-join format; old multi-way
snapshots are not a supported migration boundary. Production selection remains disabled until
the cursor is connected to the common fused execution and Flink metric/checkpoint lifecycle.


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
join itself receives Arrow batches and returns Arrow batches without a Java RowData payload loop.
A regular streaming join followed by one or more eligible Calc nodes is lowered as one
persistent native join handle with a reusable DataFusion Calc tail. Those stages exchange the
join's Arrow `RecordBatch` directly in Rust, so the fused edge adds neither a Java operator nor an
additional data-plane JNI round trip. The same path is used by bounded hash and nested-loop joins.
This retained implementation is not yet a single DataFusion ExecutionPlan containing both the
persistent join and its Calc stages, and does not close the general fusion admission requirement.
Other stateful operator families currently retain their own native handles as well.

Rust stores an ordered multiset for both input sides under a Flink-compatible key group. One input
batch loads all touched manifests and then their referenced pages in at most two distinct batched
reads, followed by one atomic write batch containing only changed pages and metadata. The same opaque state
contract runs on managed native memory or direct native RocksDB. Canonical key-group snapshots move
between those backends and across parallelism, while ordinary RocksDB checkpoints use the shared
incremental-SST lifecycle. Aligned and unaligned checkpoints preserve join state and the two input
watermark frontiers. Regular joins do not register timers. Interval joins use the shared native
timer service and materialize dirty timer groups into canonical keyed state at snapshot boundaries,
avoiding repeated whole-group timer serialization during normal batch processing. Both variants
coalesce and forward watermarks using Flink's two-input rule.

The bounded DataFusion join receives each Flink equality-key partition after the canonical
multiset has applied input retractions. A nullable synthetic key preserves the partition's
mixed null-safe/null-filtered equality decision, and DataFusion evaluates any remaining SQL
predicate over Arrow columns. This retains Flink's state/checkpoint ownership while delegating
matching to DataFusion. Its hash table uses the Flink-backed DataFusion memory pool; Arrow input
and bounded output identity buffers have coarse host reservations. One native runtime is reused
across key partitions. Generated direct-native tests compare all six join kinds to Flink SQL on
memory and RocksDB, including null keys, duplicates and a retraction before terminal output.
The shared-plan/configuration/metric admission gates are unchanged.

Bounded hash/adaptive/sort-merge joins partition both sides by the planned Flink equality key. The
native sort-merge replacement uses DataFusion hash-join computation over the already keyed
terminal state rather than sorting both inputs: SQL does not promise join output order, and a downstream Flink sort still
enforces any explicit `ORDER BY`. Its output changelog and null/residual semantics remain identical.
Bounded nested-loop joins discard Flink's broadcast/ANY exchange wrapper and install a native singleton
exchange because the complete cross-product condition is evaluated in Rust. Neither path builds a
second Java hash table or sorter. Both memory and direct RocksDB state perform the same manifest/page
batched reads and dirty-page write batch per incoming Arrow frame. Aligned, unaligned, and canonical
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
Calc and lowered to a DataFusion physical expression. Streaming regular joins evaluate candidate
pairs in Arrow chunks of at most 4,096 rows, retaining only the current input row's match bitmap
while output drains. Equality-only and null-rejected keys use constant masks without allocating a
candidate bitmap. Evaluation retains Flink's per-record state-transition order. Association counts and
outer/semi/anti transitions count only accepted candidates. Predicate scratch, state, and exported
Arrow output are all charged to the operator's Flink managed-memory reservation.
ROW, ARRAY, MAP, and MULTISET values are accepted as opaque equality keys and stored payloads, but
comparisons over those nested values inside a residual expression remain Flink-owned. The planner
rejects the complete plan with that precise reason rather than approximating nested comparison
semantics in Rust.
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

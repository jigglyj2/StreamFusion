---
title: Deduplication
description: Acceleration coverage and fallback behavior for Flink SQL Deduplication.
sidebar:
  order: 16
---

**Current status:** Temporarily uses whole-plan Flink fallback under the
[architecture admission requirements](/StreamFusion/development/architecture-admission/). The native paths
described below are retained for development and direct parity tests; SQL planning does not select them.

**Retained implementation scope:** Implementation for all synchronous, timer-free Flink deduplication modes:
row-time first/last updating plans, processing-time keep-first insert-only plans, and
processing-time keep-last updating plans. Nexmark Q18 uses the row-time keep-last path.

The retained Arrow runtime now binds `DeduplicateExec` into the shared native execution context.
Adjacent nodes use recursive physical-plan lowering and shared Arrow streams; deduplication has no
operator-specific Calc fusion loop or intermediate JNI handoff. The legacy single-output C Data
facade drains that shared tree and rejects multi-batch results, which require the general stream
edge. Native metrics report stable node IDs and logical input/output counts. Its shared-region
composition capability is now admitted, but the separate stateful memory gate still prevents SQL selection.
`DeduplicateExec` now uses the common synchronous unary physical adapter for polling,
invocation exclusion, cancellation, and stream-control memory admission. Its state codec and
checkpoint format are unchanged. A failed or abandoned invocation requires recovery; constructor
failure before this kernel consumes input can retry. There is no new pair-specific fusion path.

The selected deduplication node now supplies its fragment and keyed-state capability to the common
region collector. It can share one Flink keyed runtime with another native state owner; it does not
create its own intermediate Java operator. Planned exchange frames and their decoding contracts
are retained at the region edge. Missing or incompatible routing domains are rejected. Generated
runtime checks consume hash frames and restore two deduplication owners across canonical memory/
RocksDB savepoints; production selection remains gated as stated above.

Hidden envelope ordinals compose through preceding native stages, including stored-row
`UPDATE_BEFORE` outputs. Schema/codec admission is transactional on validation failure. Native
handoff retains its output allowance; transfer to Arrow Java occurs only at the outer edge.
Output reservations now follow reference-counted Arrow buffers through retained projections/slices
and producer close. Other large buffering-consumer owners still require the common admission checks.

Protocol-3 deduplication now consumes and emits the shared owned-record envelope. Synchronous
outputs select the triggering input's timestamp presence/value, including when their SQL payload
is a stored historical `UPDATE_BEFORE` row. SQL rowtime values remain separate payload columns.
Metadata selection and owned-ordinal validation use the common envelope helper; raw and local
aggregation use that same validator. Invalid owned ordinals are rejected before state mutation,
even for an arrival that would otherwise lose the rowtime comparison. The legacy borrowed-envelope
contract and canonical state bytes are unchanged. This is an execution-contract change, not a new
fusion combination or removal of the production admission gate.

Shared-stage conformance compares all synchronous modes and both backends against actual Flink
deduplicate functions between generated Calc stages. Complete ordered changelogs, record timestamp
presence/values, watermarks/status, latency events, pre-barrier and terminal callbacks, stage IDs,
default registered metric surfaces and latency histogram semantics are covered. When update-before
output is enabled, a rowtime key's first output is `INSERT` even when insert sensitivity is disabled,
matching Flink; subsequent winners emit the requested update pair. Configured keyed-state latency
histograms and RocksDB native metrics remain a shared planning fallback, not silently missing metrics.

## SQL example

```sql
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_time DESC) AS row_num
  FROM events
) WHERE row_num = 1;
```

## Acceleration and fallback

StreamFusion accelerates Flink's recognized `ROW_NUMBER() = 1` deduplication node when it:

- orders by its single Flink `ROWTIME` or `PROCTIME` attribute and keeps the first or last row;
- has insert-only input and uses a synchronous state strategy;
- uses the output changelog selected by Flink, including optional `UPDATE_BEFORE` rows;
- has no state TTL;
- is not Flink's timer-backed row-time keep-first insert-only optimization; and
- has identical input and output row schemas.

TTL, async state, mini-batching, Flink's timer-backed row-time keep-first insert-only operator, and
general ranking expressions fall back with a specific EXPLAIN reason. Processing-time plans carry a
synthetic `PROCTIME` field in Flink's physical graph. StreamFusion folds that field out before the
native Calc/Exchange/Deduplicate tree only when neither the outer projection, predicate, nor key can
observe it; an observable processing-time value remains on Flink.

Flink's synchronous deduplicate functions require insert-only input, so accepting input retractions
for these SQL nodes would not be Flink-compatible. The underlying native byte-state processor also
has a separately tested changelog mode for `INSERT`, `UPDATE_AFTER`, `UPDATE_BEFORE`, and `DELETE`,
ready for a future Flink physical node whose contract permits those changes.

For row-time deduplication, enabling `UPDATE_BEFORE` also requires the first row for each
key to be an `INSERT`, even when insert sensitivity is disabled. Later winning rows emit
the previous row as `UPDATE_BEFORE` followed by the replacement as `UPDATE_AFTER`.

## Implementation

The production paths remain Arrow-backed between source and sink boundaries. Rust gathers selected
columns once and returns row-kind and input-ordinal envelope metadata with the batch. Row-time state
stores the ordering value per key and stores an Arrow row encoding when `UPDATE_BEFORE` output needs
the previous value. Processing-time keep-first stores membership; processing-time keep-last stores
the last Arrow row when it must distinguish inserts, suppress equal updates, or materialize the
previous value. The stateless processing-time keep-last shape forwards the Arrow batch without a
state call. Java does not interpret native keys or values. Row encodings created for state are reused
when assembling updating output instead of converting the same input columns twice.

Historical rows remain memory-accounted after state replacement or deletion while their retractions
are materialized. Their workspace is released only after the output obtains its own allowance;
the native Arrow buffer lease then survives retained slices and producer close. This is tested with
wide historical values and narrow incoming updates on both backends, including memory refusal.

All key roots accepted by the translator have generated byte-parity coverage, including nullable
boolean and numeric values, floating point, character and binary strings, compact and non-compact
decimals, date/time/timestamps, local-zoned timestamps, arrays, maps, multisets, and nested rows.
SQL year-month and day-time intervals and distinct and structured types are covered as well.
Flink's `TIMESTAMP WITH TIME ZONE` has no `RowData` field getter, and Flink cannot construct an
internal serializer for the null-only logical type. Planner-only/unresolved types and opaque
`RAW`, `VARIANT`, and `BITMAP` values remain fallback rather than pretending to have a portable
Arrow encoding.

Flink's configured keyed-state backend selects the native implementation:

- `HashMapStateBackend` uses an `ahash`/`hashbrown` in-memory byte map.
- `EmbeddedRocksDBStateBackend` loads the separately packaged native RocksDB component and uses
  one RocksDB `multi_get` plus one `WriteBatch` per incoming Arrow batch.

Both implementations use Flink-compatible key groups. Memory checkpoints and canonical savepoints
use the backend-neutral versioned SFS1 key-group format. Regular RocksDB checkpoints use Flink's
standard `IncrementalRemoteKeyedStateHandle`: immutable SSTs are shared state, mutable manifests
and logs are private state, and only SST handles from completed checkpoints become reusable. The
metadata survives Flink's durable checkpoint-metadata serializer, and restore intersects each
physical checkpoint with the subtask's assigned key-group range. This supports 1-to-N and N-to-1
rescaling. Canonical savepoints are tested for memory-to-memory, memory-to-RocksDB,
RocksDB-to-memory, and RocksDB-to-RocksDB restoration.

These supported modes have no native timers. Incoming Arrow batches are processed synchronously, so
aligned and unaligned checkpoints snapshot the same operator state; Flink's channel-state machinery
owns records still in flight for unaligned checkpoints. No native batch can remain in flight across
the snapshot call. Production Arrow tests restore both checkpoint modes across all four memory and
RocksDB source/target combinations, and exercise canonical savepoints across the same matrix.

Arrow allocations, row/state scratch, the in-memory table, RocksDB's configured cache/write buffers,
and temporary RocksDB restore readers all reserve from the operator's existing Flink managed-memory
allowance. The stateful transformation requests a larger standard `OPERATOR` consumer weight to
cover hash-table resize peaks; it does not add a StreamFusion deployment memory setting. The operator
exposes used, peak, and limit gauges. Standard Flink input/output counters are corrected from
physical Arrow batches to logical records. Focused metric tests cover every changelog kind and the
complete timer-free state surface. Additive StreamFusion metrics also report logical
processing/changelog counts, batched state operations, backend selection, checkpoint
kind/bytes/duration/failures, incremental upload and SST-reuse bytes, and restore
bytes/duration/failures.

Flink's changelog-state wrapper is currently an explicit fallback because it would obscure the
native keyed-backend adapter. This does not affect ordinary aligned or unaligned checkpoints.

See the [Flink 2.3 Deduplication documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/deduplication/).

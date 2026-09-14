---
title: Mini-batch aggregation qualification
description: Flink bundle semantics, native conformance evidence, and remaining production admission work.
---

## Current status

Mini-batch aggregation remains **production-gated**. The retained shared native implementation
accepts Arrow batches and uses DataFusion computation, but neither its direct tests nor the
test-only SQL graph probe establishes ordinary planner admission. The
[group aggregation page](/StreamFusion/operators/group-aggregation/) defines current support.

## Retractions within a bundle

Flink 2.3.0's `MiniBatchGroupAggFunction.finishBundle` removes leading retractions only when
the key has no stored accumulator. After the first accumulation, it applies every remaining
input for that key before deciding whether to emit or delete the aggregate. A count of zero
halfway through a bundle does not reset the accumulator or restart that leading-retraction rule.

For example, one bundle containing `INSERT(10), DELETE(10), UPDATE_BEFORE(20), UPDATE_AFTER(20)`
for a previously absent key finishes with count zero and emits nothing. Skipping the third input
would incorrectly emit an aggregate for one row. StreamFusion retains accumulator presence
independently of its count until the bundle flush. Entirely retract-only absent keys still produce
no output and do not prevent other keys in the bundle from being processed.

DataFusion's COUNT accumulator supports signed retract updates. The shared row kernels continue
to delegate compatible count/sum computation to DataFusion; the fix concerns Flink's accumulator
lifecycle, not a replacement arithmetic implementation. Existing Flink-specific integer AVG,
retractable-extremum, state and changelog adapters remain in place. No state encoding, boundary,
memory budget, or runtime option changes are introduced.

## Conformance coverage

- Native mini-batch tests cover a zero crossing across Arrow chunk sizes one, two and four,
  compare the final canonical state with an empty reference, and exercise memory and RocksDB.
- `SharedMiniBatchRetractionParityTest` runs the same generated records through Flink's actual
  SQL-generated mini-batch aggregate and the common native region. It compares complete ordered
  changelog bytes, record timestamps and registered metrics on both backends, including nullable
  keys/values, absent-state retractions, count triggers, checkpoint pre-barrier flush and finish.
- `SelectedMiniBatchAggregateUpstreamTest` invokes the published Flink `AggregateITCase.testGroupByAgg`
  body and assertions with mini-batching on both backends, including its failing-source recovery.
  It uses the existing development graph probe and requires native activity and Arrow topology.
  This is retained-implementation coverage, not evidence that production fallback is removed.
- Existing shared mini-batch tests separately cover control ordering, metric surfaces, downstream
  failure, canonical backend-switch restore, and aligned/unaligned state checkpoints.
- `SharedMiniBatchChannelRecoveryTest` covers two input channels and a three-record bundle on
  both backends. An unaligned checkpoint flushes at the first barrier and captures the next
  channel's Arrow input; an aligned checkpoint includes that input before its pre-barrier flush.
  Actual channel replay, subsequent retractions, count-triggered output and final partial-bundle
  output match the SQL-generated Flink operator's complete record bytes and timestamp envelopes.
  The restored task neither re-emits checkpointed output nor loses the captured pending row.
- `SharedMiniBatchRescalingTest` restores both the native tree and actual Flink aggregate
  through 1 → 2 → 1 parallelism, with an independent bundle and metric surface for each subtask.
  Generated nullable keys, mixed RowKinds and integer extrema exercise all 16 key groups.
  Flink's own key selector routes the oracle independently of the native Arrow IPC exchange.
  Tests compare complete ordered changelog bytes, timestamp envelopes and registered metrics
  after every arrival, checkpoint flush and finish. Canonical savepoints switch memory/RocksDB
  backends in both directions; aligned and unaligned state checkpoints retain their backend.
  Original Flink savepoints are repartitioned separately from StreamFusion's snapshots.

## Remaining admission work

Before ordinary selection can be enabled, complete the mini-batch ownership/admission audit
and end-to-end assigner/region control conformance. Extend channel recovery and rescaling
coverage as additional families and shapes qualify. Each subtask owns its bundle count;
one global oracle cannot establish mini-batch parity after redistribution. Qualify one-phase
and local/global/incremental families with their own Flink SQL,
generated changelog and complete metric coverage. Preserve precise fallback for any unresolved
semantic or configuration subset. Release measurements and profiling must follow the repository's
staged Nexmark requirements; correctness checks above establish no performance claim.

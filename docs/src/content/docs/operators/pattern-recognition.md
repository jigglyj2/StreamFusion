---
title: Pattern recognition
description: Acceleration coverage and fallback behavior for Flink SQL pattern recognition.
sidebar:
  order: 18
---

**Current status:** Accelerated for strict fixed processing-time sequences with current-row
predicates, one output row per match, direct field measures, and either `AFTER MATCH SKIP TO NEXT
ROW` or `AFTER MATCH SKIP PAST LAST ROW`.

## SQL example

```sql
SELECT bidder, first_auction, second_auction, third_auction
FROM bid_with_proc_time
MATCH_RECOGNIZE (
  PARTITION BY bidder
  ORDER BY proc_time
  MEASURES
    A.auction AS first_auction,
    B.auction AS second_auction,
    C.auction AS third_auction
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (A B C)
  DEFINE
    A AS A.auction IS NOT NULL,
    B AS B.auction IS NOT NULL,
    C AS C.auction IS NOT NULL
);
```

## Acceleration and fallback

StreamFusion replaces `StreamExecMatch` only when Flink has selected an ascending processing-time
order and the physical input is the normal `Calc(PROCTIME) -> Exchange -> Match` shape. The planner
folds the synthetic, unobservable processing-time column out of the native Calc and Exchange before
creating a distinct StreamFusion MATCH exec node. This follows the same all-or-nothing, separate-exec
model used by the other StreamFusion operators.

The accelerated pattern must be a non-empty strict concatenation of unique variables, such as
`A B C`. `DEFINE` expressions may inspect the current row for their own variable and must lower to
the native Calc expression set. Measures may be direct fields or zero-offset `FIRST`, `LAST`, or
`FINAL` field references. Output is `ONE ROW PER MATCH`. Both next-row and past-last-row skip
strategies preserve Flink's overlap behavior.

Quantifiers, alternatives, permutations, subsets, `ALL ROWS PER MATCH`, cross-row navigation,
computed measures, additional order fields, row-time ordering, and `WITHIN` intervals fall back
with a specific EXPLAIN reason. Row-time and `WITHIN` require the remaining CEP timer and watermark
semantics and are not approximated. Flink requires an insert-only input for streaming
`MATCH_RECOGNIZE`, so rejecting update and retraction rows is full changelog parity for this
physical node rather than a missing retraction mode.

Every Arrow-representable Flink logical family is accepted in payloads, measures, and partition
keys: nullable scalar and numeric values, strings and binary data, compact and wide decimals,
dates, times, timestamps, local-zoned timestamps, both interval families, arrays, maps, multisets,
rows, distinct types, and structured types. Complex keys use the same opaque Flink binary-key
sidecar as other stateful operators. Unsupported planner-only, null-only, raw, variant, bitmap, and
timestamp-with-time-zone values remain explicit fallback because Flink cannot provide the required
portable RowData/Arrow representation.

## State, recovery, and metrics

Rust evaluates each current-row predicate once per Arrow batch, computes Flink-compatible key
groups with `ahash`, reads every touched partition in one backend batch, advances the fixed
sequence, writes all mutations in one backend batch, and produces one Arrow result batch. Partial
matches use versioned canonical Arrow-row bytes. Java never interprets native state and there is no
per-record JNI or RowData path inside the accelerated plan.

`HashMapStateBackend` selects the managed-memory-accounted native byte map.
`EmbeddedRocksDBStateBackend` selects the separately packaged native RocksDB component and performs
one `multi_get` and one atomic `WriteBatch` per incoming Arrow batch. Both backends use the same
key-group state format for canonical savepoints and support memory-to-memory, memory-to-RocksDB,
RocksDB-to-memory, and RocksDB-to-RocksDB restore. Recovery tests cover aligned and unaligned
checkpoints, canonical savepoints, and 1-to-N-to-1 rescaling. Regular RocksDB checkpoints use
incremental Flink state handles and reuse immutable SSTs only after checkpoint completion.

The accelerated processing-time slice has no timer state: input arrival order is its processing
order, and a native batch finishes synchronously before a checkpoint snapshot. Flink owns channel
state for unaligned checkpoints. Native partial-match state, hash tables, row encoding, batch
scratch, exported Arrow buffers, and RocksDB cache/write buffers all reserve against Flink's
existing managed-memory allocation.

The Flink metric surface includes logical `numRecordsIn`, logical `numRecordsOut`, and
`numLateRecordsDropped`; the latter remains exactly zero for processing-time input. The additive
`StreamFusion` subgroup reports processed/emitted logical rows, native state read/write batches,
backend selection, managed-memory use and peak, checkpoint/restore bytes and duration, incremental
SST reuse/upload bytes, failures, and completed matches. Counters are updated per logical row rather
than per Arrow batch.

The implementation is intentionally a focused fixed-sequence machine rather than a general NFA.
DataFusion, RisingWave, and Arroyo do not currently provide an equivalent streaming SQL
`MATCH_RECOGNIZE` executor in the local reference trees, so Flink CEP is the semantic oracle while
the state and batch architecture follows the existing StreamFusion/Comet model.

See the [Flink 2.3 Pattern recognition documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/match_recognize/).

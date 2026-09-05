---
title: Set operations
description: Acceleration coverage and fallback behavior for Flink SQL Set operations.
sidebar:
  order: 11
---

**Current status:** `UNION ALL`, `UNION DISTINCT`, `INTERSECT [ALL]`, and `EXCEPT [ALL]`
are accelerated for complete eligible streaming plans. `UNION ALL` is also accelerated in
complete eligible bounded plans.

## SQL example

```sql
SELECT bidder FROM mobile_bids
UNION ALL
SELECT bidder FROM web_bids;
```

## Acceleration and fallback

`UNION ALL` is eligible when every input has the same Flink logical row type and every
internal operator in every branch is accelerated. It preserves duplicates, nulls,
changelog records, each record's `RowKind`, timestamps, and the order of records within
each individual input. Flink does not define a stable ordering between different union
inputs. A rejected node in any branch causes the entire query to fall back under the
normal all-or-nothing rule.

The common row type must also fit Arrow's complete value domain. In particular,
`TIMESTAMP(7..9)` and nested occurrences of it fall back before boundary conversion because
Arrow nanosecond timestamps cannot represent Flink's complete calendar range.

`UNION`/`UNION DISTINCT` uses native `UNION ALL`, native hash exchange, and the native
group-aggregate membership state used by `SELECT DISTINCT`. It therefore supports the
same complete Arrow key-type matrix, changelog retractions, memory/RocksDB backends,
canonical restore and rescaling contract, and managed-memory accounting as native
distinct aggregation.

Flink rewrites `INTERSECT DISTINCT` and `EXCEPT DISTINCT` to native distinct aggregation plus
a regular semi or anti join. StreamFusion preserves Flink's per-key null-filter contract: the
`IS NOT DISTINCT FROM` keys introduced by the set rewrite match two null values, while ordinary
equality join keys continue to filter nulls. The regular join's counted row multisets accept all
four Flink changelog kinds and use the selected native memory or RocksDB keyed-state backend.

Flink rewrites `INTERSECT ALL` and `EXCEPT ALL` to `UNION ALL`, native grouped `COUNT`/`SUM`
state, Calc, and its optimizer-only `$REPLICATE_ROWS$1` table function. Rust evaluates the count
once per Arrow batch and gathers the preserved input and every returned value with one selection
vector. Expansions are pulled through Arrow C Stream in at most 16,384-row batches, so one large
count cannot require a whole-result allocation or a concatenation copy. The native stream owns the
input reservation until exhaustion and its release callback is invoked exactly once. Zero and
negative counts emit no rows. The selected input ordinal is repeated with each
value, so INSERT, UPDATE_BEFORE, UPDATE_AFTER, and DELETE envelopes are retained without exposing
payload rows to Java. The following Calc is nested in the same protobuf/DataFusion plan rather
than crossing the JVM boundary between stages.

Set keys and replicated values support the complete Arrow-representable Flink type matrix used by
native distinct aggregation and regular join, including decimals, temporal and interval values,
arrays, maps, multisets, rows, and their nullable nested forms. Unsupported RAW/symbolic types,
nanosecond timestamps outside Arrow's full domain, state TTL, async state, or another unsupported
node in either branch trigger whole-plan Flink fallback with the precise EXPLAIN reason. `IN` and
`EXISTS` are accelerated when Flink lowers them to an otherwise eligible regular semi/anti join;
connector- or UDF-dependent alternatives remain on Flink.

## Implementation

The planner replaces `StreamExecUnion` with the distinct `StreamFusionExecUnion` node and
`BatchExecUnion` with the distinct `StreamFusionBatchExecUnion` node. Both use the same
schema-negotiated multi-input Arrow transport rather than maintaining a separate row-oriented
batch implementation.
Its Flink runtime is a non-keyed multiple-input operator: Flink still schedules and
multiplexes the inputs, aligns checkpoint barriers, combines watermarks, and tracks input
idleness. A multiple-input gate is an unavoidable Flink network boundary, so each native
branch first emits the same schema-negotiated Arrow IPC exchange frame used by native
hash exchange. The union decodes each frame using Flink-managed memory and forwards its
Arrow batch immediately. It does not buffer rows, transpose through `RowData`, invoke a
merge kernel, or copy the decoded column buffers merely to implement union semantics.

The frame carries the input batches' Flink `RowKind` and record-timestamp envelope as
metadata vectors. Decoding restores that envelope without reconstructing payload rows.
Flink's multiple-input operator provides the control-event ordering, combined watermark,
barrier alignment, idleness, and end-of-input behavior. Standard input and output counters are
corrected from the physical Arrow-frame count to Flink logical records on every forwarded batch;
`UNION ALL` adds no operator-specific metric surface beyond Flink's standard task/operator I/O
metrics.

The bounded coverage in this milestone is deliberately limited to `UNION ALL`; bounded
DISTINCT, INTERSECT, and EXCEPT physical rewrites remain on Flink until every node in those
batch plans has an exact StreamFusion implementation.

Intersection and difference deliberately reuse the same native keyed state as the Flink physical
rewrite rather than adding a second set-specific state format. Memory and RocksDB therefore share
the canonical key-group savepoint bytes, rescaling behavior, aligned/unaligned checkpoint support,
and incremental RocksDB checkpoint files already documented for group aggregation and regular
join. The stateless row replicator has no savepoint payload of its own. Its output allocation and
selection vector are charged to Flink managed memory.

See the [Flink 2.3 Set operations documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/sql/reference/queries/set-ops/).

---
title: Window join
description: Acceleration coverage and fallback behavior for Flink SQL Window join.
sidebar:
  order: 10
---

**Current status:** Partial. Synchronous attached event-time `INNER` window joins use DataFusion
inside the common native plan with in-memory or default RocksDB state. Whole-plan admission still
requires supported children, expressions, sources, sinks, and backend settings. Enabling Flink's
multi-join optimizer is not required.

## SQL example

```sql
SELECT l.window_start, l.id, r.value
FROM TABLE(TUMBLE(TABLE left_input, DESCRIPTOR(ts), INTERVAL '10' SECOND)) l
JOIN TABLE(TUMBLE(TABLE right_input, DESCRIPTOR(ts), INTERVAL '10' SECOND)) r
ON l.id = r.id
AND l.window_start = r.window_start
AND l.window_end = r.window_end;
```

The example accelerates only if its window-assignment children and scalar types also satisfy
their own admission rules. Support for an attached WindowJoin does not admit every TVF family.

## Acceleration and fallback

Both inputs must carry attached window ends in epoch-millisecond `BIGINT` or `TIMESTAMP(3)`
columns. The window time zone must resolve to UTC. Equality keys may be absent; otherwise they
must have matching supported boolean, integer, string, binary, or decimal types. Payloads must
be scalar. Bounded primitive comparisons, arithmetic, boolean expressions, null checks and
conditionals may form the residual predicate. Unsupported computed workspaces retain fallback.

Outer, semi and anti joins, nested payloads or unsupported keys, non-UTC window time, mini-batching,
async state and changelog-state wrapping retain explicit whole-plan fallback. Shared backend
admission also checks the original Flink memory and metric configuration. Unsupported surrounding
nodes cause the whole plan to fall back.

SQL-reachable inputs are append-only: `INSERT` and `UPDATE_AFTER` append duplicate-preserving
entries. On-time retractions are rejected; late records drop before changelog validation, matching
Flink's WindowJoin. A coalesced input watermark closes windows, emitting INSERT records with no
record timestamp. Ordinary batch EOF, end-input and checkpoint preparation do not fire windows.

## Execution, memory and state

The native WindowJoin and adjacent native operators compose in one DataFusion execution tree.
Flink network edges carry standard Arrow IPC frames, decoded once at the receiving native-plan
edge. DataFusion `NestedLoopJoinExec` and its `JoinFilter` compute closed-window results while
preserving Flink's left/right duplicate order. A complete right Arrow window is shared across
left pages of at most 256 rows, with bounded decode bytes. Each page uses a fresh DataFusion join
execution to reset its build state; adjacent native stages still exchange Arrow directly. No Java
candidate-matching loop is used.

Native state separates payload entries from an Arrow-row ordering index. Both backends use
ordered range access and batch writes; growing partition values are not rewritten on every row.
Flink partition hashing and key-group identity remain separate from sortable state keys. A left
page's entries are reclaimed only after its DataFusion output drains. The right entries, header
and timer remain until all pages finish. A partial-close cursor cannot be checkpointed: input and
checkpoint operations remain blocked during the invocation, and failure recovers Flink's previous
checkpoint. Persisted encodings are unchanged.

Coarse reservations cover retained state, large Arrow inputs and outputs, and DataFusion's
candidate/filter workspace through Flink's original managed-memory allowances. Shared buffers
are counted once. Left-side staging stays bounded rather than copying a complete growing window.
The complete right side and DataFusion candidate workspace must still fit. Small pages cap batch
capacity at their possible pair count; larger work
returns a recoverable budget error when it cannot be admitted. Temporary allocation descriptors
do not require individual reservations or per-allocation JNI calls.

Flink owns checkpoint coordination, channel state, restore and rescaling. WindowJoin restores
pending timers with its watermark reset to `Long.MIN_VALUE`, matching Flink; it does not use
window aggregates' persisted watermark clocks. Shared snapshots use the versioned `SFWF/2`
contract and reject incompatible operator contracts and earlier unadmitted shared encodings.
Tests cover canonical backend changes, aligned and unaligned snapshots, in-flight Arrow frame
replay, and 1→2 key-group redistribution on both backends. Native tests additionally close a
20,003-row left window with only 4 MiB of remaining allowance and restore a cancelled multi-page
window after earlier pages have completed.

## Metrics and validation

Each stage retains Flink's logical-record I/O counters, operator scope and identity, latency
metrics, `leftNumLateRecordsDropped`, `leftLateRecordsDroppedRate`, `rightNumLateRecordsDropped`,
`rightLateRecordsDroppedRate`, and `watermarkLatency`. Generated tests compare complete registered
metric surfaces, ordered changelog bytes and controls against actual Flink operators.

Q5 has ordinary planner, collecting-sink and blackhole integration coverage with default
WindowJoin selection. Current release measurements and mixed profiles for this path are pending;
historical Q5 results used the enabled multi-join optimizer. See [Joins](../joins/) for the detailed
compute, ownership, state and recovery contracts.

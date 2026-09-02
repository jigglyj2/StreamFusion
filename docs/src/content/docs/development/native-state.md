---
title: Native keyed state
description: Backend contract, checkpoint formats, and implementation references for native operators.
---

Native operators use a small backend-neutral Rust interface over opaque key and value bytes:
batched get, batched mutation, canonical key-group snapshot, and canonical key-group restore.
The in-memory backend can return borrowed values. The RocksDB backend implements a batch get with
one `multi_get` and a batch mutation with one `WriteBatch`.

Keys are prefixed or partitioned by the key group computed with StreamFusion's Flink-compatible
Rust key-group logic. This makes key-group ownership independent of the backend and lets Flink's
normal redistribution assign intersections during rescaling.

The optional RocksDB module is not linked into the central runtime. The runtime loads its versioned
C function table, exchanges batch requests through Arrow C Data, and calls RocksDB directly from
Rust. The component borrows Arrow-owned keys and values while constructing each native batch rather
than copying them into an intermediate object graph. No state lookup crosses JNI or asks Java to
understand the bytes. RocksDB's task-local WAL is disabled, matching Flink's keyed-state backend:
completed checkpoints plus replayable input, rather than the local database log, define recovery.

The native RocksDB component exposes RocksDB's physical checkpoint API. A transparent StreamFusion
keyed-state-backend adapter delegates ordinary Flink state to the configured HashMap or RocksDB
backend and claims only StreamFusion's marked native handles. Regular native RocksDB checkpoints
flush the checkpoint boundary to immutable SSTs and emit Flink's standard
`IncrementalRemoteKeyedStateHandle`. SSTs use shared scope; manifests, logs, and the small
StreamFusion metadata marker use exclusive scope. Empty RocksDB files are recorded in that marker
and recreated without inventing null state handles.

The adapter reuses an SST only after its checkpoint completes, carries Flink's backend UUID and
physical state-handle identities through metadata serialization, and drops pending reuse state on
abort or subsumption. Tests round-trip the handle through Flink's version-3 durable metadata
serializer, verify unchanged SST identity and reduced checkpointed bytes, restore the round-tripped
handle, and rescale native RocksDB state 1-to-2-to-1. Canonical savepoints deliberately remain SFS1
raw keyed state so they can move between the native memory and RocksDB implementations.

The focused recovery matrix is shared by native group aggregation, deduplication, SELECT DISTINCT,
non-window Top-N, window aggregation, Window Deduplicate, Window Top-N, and Window Join. Window cases include pending event-time
and processing-time timers:

| Recovery path | Memory state | RocksDB state |
| --- | --- | --- |
| Aligned checkpoint, same backend | Tested | Tested, incremental |
| Unaligned checkpoint, same backend | Tested | Tested, incremental |
| Canonical savepoint to memory | Memory → memory | RocksDB → memory |
| Canonical savepoint to RocksDB | Memory → RocksDB | RocksDB → RocksDB |
| Rescaling | 1 → 2 → 1 | 1 → 2 → 1 |
| Incremental metadata serialization and unchanged-SST reuse | Not applicable | Tested |

The operator processes a batch synchronously before the mailbox can snapshot it. Flink therefore
owns in-flight channel data for unaligned checkpoints, while the native keyed snapshot contains the
same completed state boundary as an aligned checkpoint.

## Reference gut-check

Flink's RocksDB incremental snapshot strategy is the lifecycle model: create a consistent native
checkpoint synchronously at the barrier, upload it asynchronously, reuse confirmed SST handles by
filename, upload mutable metadata privately, register shared handles with the checkpoint
coordinator, and only advance the reusable base after checkpoint completion. StreamFusion follows
that lifecycle while retaining its backend-neutral SFS1 savepoint format.

RisingWave's append-only deduplicate executor provides a useful operator-level comparison. It
projects all keys for a chunk, populates a managed cache from its state table, updates visibility
in input order, commits on barriers, and clears stale cache entries after vnode reassignment.
StreamFusion likewise preserves within-batch order and partitions state for rescaling, but its
RocksDB path intentionally uses one batch `multi_get` rather than per-key existence futures.

Arroyo does not currently expose an equivalent dedicated SQL deduplicate executor. Its
`GlobalKeyedTable` is the closest state comparison: generic keyed values live in a Rust `HashMap`,
updates are sent to an epoch checkpointer, and binary key/value Arrow arrays are written as Parquet.
That validates the opaque-byte and batch-checkpoint direction, while StreamFusion differs by using
Flink-owned key groups and snapshot lifecycle plus direct RocksDB incremental files.

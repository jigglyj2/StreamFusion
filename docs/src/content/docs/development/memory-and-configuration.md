---
title: Memory and configuration
description: How native execution participates in Flink memory accounting and configuration.
---

StreamFusion follows the host engine's resource model. Operators may execute in Rust,
but they do not own an unaccounted native-memory pool or require a second set of
deployment settings.

## Memory accounting

Flink's existing TaskManager memory configuration governs all memory used by a
StreamFusion operator, including Arrow buffers, DataFusion reservations, and custom
Rust data structures. In particular, operators participate in Flink's
operator-scoped managed-memory allocation, whose total is determined by
`taskmanager.memory.managed.size` or `taskmanager.memory.managed.fraction` and divided
using Flink's managed-memory consumer weights. StreamFusion must not treat
`taskmanager.memory.task.off-heap.size` as an extra untracked allowance.

The runtime bridge is:

1. The generated Flink transformation declares the `OPERATOR` managed-memory use case.
2. Flink assigns the operator its fraction of TaskManager managed memory.
3. A task-scoped broker reserves and releases bytes through Flink's `MemoryManager`.
   Arrow Java allocators and the native DataFusion memory pool use the same broker and
   therefore the same operator limit.
4. Every reservation and release is reflected in that adapter. A refused reservation
   asks a spill-capable operator to spill; if it cannot, execution fails with a useful
   resource error rather than allocating beyond Flink's limit.
5. Closing, cancellation, and failure release reservations, with leak checks covering
   each terminal path.

The broker, Arrow allocator, decoded protobuf plan, Tokio runtime, and DataFusion task
context live for the Flink operator's task lifetime. Native execution contexts are
closed before the Arrow allocator so outstanding DataFusion reservations return to
Flink before imported Arrow buffers are checked and released. The operator exposes its
current, peak, and assigned managed-memory bytes as Flink metrics.

This is modeled on Apache DataFusion Comet's unified off-heap path. Comet implements a
DataFusion `MemoryPool` that calls a JVM `CometTaskMemoryManager` over JNI. That adapter
registers an off-heap Spark `MemoryConsumer` and delegates acquisition and release to
Spark's `TaskMemoryManager`; a partial grant becomes a DataFusion resource error that
can trigger spilling. StreamFusion should preserve that single-authority design while
using Flink's managed-memory APIs and lifecycle instead of copying Spark-specific
configuration or task classes.

DataFusion reservations and production Arrow boundary allocations are accounted by this
bridge. Native exchange JNI output is also reserved while it is copied into its
producer-owned Java result. Ordinary Rust memory follows Comet's pattern too: Rust's
system allocator remains responsible for the physical allocation, while task-owned
plans, vectors, maps, scratch buffers, and custom operator state hold RAII reservations
from the same DataFusion pool. Dropping the Rust owner returns the reservation to Flink.

Arrow and other batch-producing kernels can learn the exact buffer size only after
building one batch. Like Comet's buffered operators, StreamFusion charges that newly
produced batch before allowing it to continue; a refused reservation drops it and fails
with a resource error. Additional copies whose size is predictable, such as concatenated
output or encoded exchange buffers, are reserved before allocation. When Rust-produced
Arrow buffers cross the C Data boundary, Arrow Java's foreign-allocation wrapper assumes
their accounting for the remainder of their lifetime. New custom Rust operators must use
the shared reservation APIs for any data structures that DataFusion's `MemoryPool` does
not already track; adding an unbounded allocator is not an acceptable fallback.

## Configuration policy

StreamFusion does not add deployment knobs for behavior Flink already configures.
Parallelism, checkpoints, state, network memory, managed/off-heap memory, connectors,
and client behavior continue to use their normal Flink settings. This keeps an
accelerated deployment operationally equivalent to the Flink job it replaces.

A StreamFusion-specific switch is justified only as an explicit feature gate for
functionality that may not yet be byte-identical to Flink. Such a switch must document
the semantic difference and default to the parity-preserving behavior. It must not be
used to create a parallel tuning surface for an existing Flink option.

## Native connector settings

A native source or sink is eligible only if it preserves the effective configuration
of the Flink/Java connector. Its adapter must translate relevant client properties—such
as security, authentication, serialization, delivery guarantees, transaction and
timeout behavior, retry policy, and broker discovery—to equivalent Rust client
settings.

Translation is semantic, not just a matching-name exercise. Tests must compare the
resolved Java and Rust configurations, including Flink defaults and derived values. If
the native library has no exact equivalent, StreamFusion must retain the Flink source
or sink and expose the unsupported setting as the fallback reason. It must never
silently accept the Rust client's default.

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

The intended runtime bridge is:

1. The generated Flink transformation declares the `OPERATOR` managed-memory use case.
2. Flink assigns the operator its fraction of TaskManager managed memory.
3. A native memory-pool adapter exposes that exact budget to DataFusion and to a shared
   StreamFusion allocator used by non-DataFusion Rust code.
4. Every reservation and release is reflected in that adapter. A refused reservation
   asks a spill-capable operator to spill; if it cannot, execution fails with a useful
   resource error rather than allocating beyond Flink's limit.
5. Closing, cancellation, and failure release reservations, with leak checks covering
   each terminal path.

This is modeled on Apache DataFusion Comet's unified off-heap path. Comet implements a
DataFusion `MemoryPool` that calls a JVM `CometTaskMemoryManager` over JNI. That adapter
registers an off-heap Spark `MemoryConsumer` and delegates acquisition and release to
Spark's `TaskMemoryManager`; a partial grant becomes a DataFusion resource error that
can trigger spilling. StreamFusion should preserve that single-authority design while
using Flink's managed-memory APIs and lifecycle instead of copying Spark-specific
configuration or task classes.

The bridge is a design requirement and is not implemented yet. Until allocations can
be completely accounted this way, native execution must not be presented as production
ready.

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

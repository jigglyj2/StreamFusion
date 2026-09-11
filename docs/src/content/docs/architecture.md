---
title: Architecture
description: How StreamFusion fits into Apache Flink.
---

StreamFusion deliberately keeps Flink in control of the distributed system. It does not replace Flink's scheduler, checkpoint coordinator, state lifecycle, recovery model, or SQL frontend.

```text
Flink SQL
   │
   ▼
Flink parser and planner ── StreamFusion planner extension
   │                              │
   │ unsupported plan             │ eligible plan
   ▼                              ▼
Flink operators             Native execution operators
                                  │
                                  ▼
                           Apache DataFusion
```

## Design boundaries

- **Planning stays in Flink.** StreamFusion integrates through a small planner factory hook maintained as a patch against the targeted Flink 2.3 release.
- **Execution may become native.** Eligible relational operators can be lowered to DataFusion or purpose-built Rust operators.
- **Flink owns correctness infrastructure.** Checkpointing, state snapshots, recovery, distribution, and job lifecycle remain Flink responsibilities.
- **Fallback is expected.** Plans or operators that cannot preserve Flink semantics continue through normal Flink processing.

## Arrow-native execution

Adjacent Rust operators form one native DataFusion execution-plan tree and pass Arrow record batches directly through native batch streams. Arrow's reference-counted arrays allow an operator to hand the next operator the same underlying buffers without serializing or copying the batch. JVM/native conversion happens only at the outer edges of the fused native plan through lightweight batch views and an Arrow C Stream-style ownership boundary.

Production native trees and shared regions use one port-tagged Arrow C Data output driver.
A tree emits on port zero; shared regions retain their independently typed exits and cooperative
scheduling. Each port negotiates its schema once per invocation. The original native stream owns
completion, cancellation, and state lifecycle, and exported batches can outlive the invocation.
This edge uses protocol version 3, which requires matching Java and native artifacts. Legacy
selection-based callers without an owned record envelope retain the Arrow C Stream adapter.

At an input boundary, StreamFusion transposes Flink internal `RowData` into Arrow vectors.
At an output boundary, Flink reads those vectors through reusable `RowData`, `ArrayData`,
`MapData`, and nested-row views; values are not copied back into `GenericRowData`. The
boundary supports Flink's Arrow-compatible scalar types, decimal128, every temporal
precision, strings and binary values, arrays, maps, nested rows, and nulls. Sliced Arrow
vectors are rebased to offset zero before crossing into Java because Java consumers do
not consistently preserve Arrow slice offsets.

When a unary native region begins with a Calc over a RowData source, the source adapter
writes only the top-level fields and nested paths referenced by that Calc's projection
and predicate. Nullable parent rows remain nullable in the flattened Arrow fields.
The planner remaps the first Calc's input references while preserving its physical
identity, metrics and control policy; DataFusion still evaluates the predicate and
expressions. Existing Arrow inputs retain their layout and shared buffers. This source
projection also applies to mixed unary regions, such as Calc followed by expansion.

For filtered Calc batches, DataFusion is the authority on row selection. Production regions
carry owned RowKind and timestamp vectors through the native filter and projection with the
payload. Their outputs use detached ordinal `-1`; Java reads the returned envelope directly.
Legacy callers with borrowed envelopes carry zero-based input ordinals and use those ordinals
to select the original record metadata. Neither path runs a parallel Java predicate or copies
row payloads to discover which rows survived.

Arrow C Data owns the cross-language contract. StreamFusion keeps one coherent,
unrelocated Arrow Java implementation: although Java package shading cannot change C
struct layouts or pointers, it does change the class names expected by Arrow Java's JNI
wrapper symbols. Allocator ownership and release responsibility remain explicit: exactly
one side releases each exported structure, and the allocator supplied by Flink remains
alive until every imported view has closed.

Zero-copy applies to the handoff of an existing batch. Operators remain free to allocate new result buffers when the operation itself requires new data, such as aggregation, sorting, joining, or evaluating a computed expression.

Native allocations remain part of Flink's resource model. DataFusion and custom Rust
operators share the managed-memory budget assigned by Flink; StreamFusion does not
create a separate off-heap allowance. See [Memory and configuration](../development/memory-and-configuration/)
for the accounting bridge, configuration policy, and native connector fallback rules.

Optional operators and connectors own and package their Rust implementations. A
versioned C ABI discovers those native components, while the Arrow C Data and C Stream
interfaces carry batches directly between them. See [Native modules and ABI](../development/native-modules/)
for packaging, compatibility, and ownership requirements.

The project is organized as optional Maven modules corresponding to these extension points. Java packages and Maven coordinates use the `tech.streamfusion` namespace.

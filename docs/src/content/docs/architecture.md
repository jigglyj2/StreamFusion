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

Zero-copy applies to the handoff of an existing batch. Operators remain free to allocate new result buffers when the operation itself requires new data, such as aggregation, sorting, joining, or evaluating a computed expression.

Native allocations remain part of Flink's resource model. DataFusion and custom Rust
operators share the managed-memory budget assigned by Flink; StreamFusion does not
create a separate off-heap allowance. See [Memory and configuration](./development/memory-and-configuration/)
for the accounting bridge, configuration policy, and native connector fallback rules.

Optional operators and connectors own and package their Rust implementations. A
versioned C ABI discovers those native components, while the Arrow C Data and C Stream
interfaces carry batches directly between them. See [Native modules and ABI](./development/native-modules/)
for packaging, compatibility, and ownership requirements.

The project is organized as optional Maven modules corresponding to these extension points. Java packages and Maven coordinates use the `tech.streamfusion` namespace.

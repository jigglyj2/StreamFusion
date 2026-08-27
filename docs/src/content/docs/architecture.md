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

The project is organized as optional Maven modules corresponding to these extension points. Java packages and Maven coordinates use the `tech.streamfusion` namespace.

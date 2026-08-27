---
title: Operators
description: StreamFusion acceleration and fallback coverage for Flink SQL operators.
sidebar:
  order: 1
---

This matrix follows the query operations documented by Flink 2.3, including the specialized operations with their own reference pages. **Fallback** means Flink plans and executes the operator normally. StreamFusion currently has a planner integration point but no native execution operators, so every operator falls back today.

| Operator | Can be accelerated? | Current status | Intended implementation |
| --- | --- | --- | --- |
| [SELECT & WHERE](select-where/) | Yes | Fallback | DataFusion expressions, projections, and filters |
| [SELECT DISTINCT](select-distinct/) | Yes | Fallback | DataFusion distinct or native keyed state |
| [WITH](with/) | Not directly | Flink planning | Inlined by Flink; accelerate resulting operators |
| [Windowing TVFs](window-tvf/) | Yes | Fallback | Native window assignment compatible with Flink |
| [Group aggregation](group-aggregation/) | Yes | Fallback | DataFusion aggregates with Flink-managed state |
| [Window aggregation](window-aggregation/) | Yes | Fallback | Native window state and DataFusion aggregate kernels |
| [OVER aggregation](over-aggregation/) | Yes | Fallback | Native ordered state and aggregate kernels |
| [Joins](joins/) | Yes, by join type | Fallback | DataFusion batch joins or custom streaming state |
| [Window joins](window-join/) | Yes | Fallback | Custom window-aware streaming join |
| [Set operations](set-operations/) | Yes, by operation | Fallback | DataFusion set kernels or native keyed state |
| [ORDER BY](order-by/) | Bounded inputs | Fallback | DataFusion sort |
| [LIMIT](limit/) | Bounded inputs | Fallback | DataFusion limit |
| [Top-N](top-n/) | Yes | Fallback | Custom keyed ranking state |
| [Window Top-N](window-top-n/) | Yes | Fallback | Custom per-window ranking state |
| [Deduplication](deduplication/) | Yes | Fallback | Custom keyed first/last-row state |
| [Window deduplication](window-deduplication/) | Yes | Fallback | Custom per-window keyed state |
| [Pattern recognition](pattern-recognition/) | Potentially | Fallback | Custom streaming NFA; no DataFusion equivalent |
| [Changelog conversion](changelog-conversion/) | Not a compute target | Flink execution | Preserve Flink row-kind conversion |
| [Time travel](time-travel/) | No | Flink planning | Catalog snapshot resolution stays in Flink |
| [Model inference](model-inference/) | Provider-dependent | Fallback | Native provider integration only when parity is proven |
| [Vector search](vector-search/) | Potentially | Fallback | DataFusion/custom vector kernels when connector semantics permit |

“Can be accelerated” describes the safe architectural target, not implemented support. Each linked page defines the eligibility boundary and expected fallback behavior.

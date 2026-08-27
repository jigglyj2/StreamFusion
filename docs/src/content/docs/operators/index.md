---
title: Operators
description: StreamFusion acceleration and fallback coverage for Flink SQL operators.
sidebar:
  order: 1
---

This matrix follows the query operations documented by Flink 2.3, including the specialized operations with their own reference pages. **StreamFusion does not accelerate any operator yet.** It currently provides a planner integration point only, so Flink plans and executes every operator normally.

| Operator | Accelerated today? | Future acceleration target | Intended implementation |
| --- | --- | --- | --- |
| [SELECT & WHERE](select-where/) | **No** | Yes | DataFusion expressions, projections, and filters |
| [SELECT DISTINCT](select-distinct/) | **No** | Yes | DataFusion distinct or native keyed state |
| [WITH](with/) | **No** | Not directly | Inlined by Flink; accelerate resulting operators |
| [Windowing TVFs](window-tvf/) | **No** | Yes | Native window assignment compatible with Flink |
| [Group aggregation](group-aggregation/) | **No** | Yes | DataFusion aggregates with Flink-managed state |
| [Window aggregation](window-aggregation/) | **No** | Yes | Native window state and DataFusion aggregate kernels |
| [OVER aggregation](over-aggregation/) | **No** | Yes | Native ordered state and aggregate kernels |
| [Joins](joins/) | **No** | By join type | DataFusion batch joins or custom streaming state |
| [Window joins](window-join/) | **No** | Yes | Custom window-aware streaming join |
| [Set operations](set-operations/) | **No** | By operation | DataFusion set kernels or native keyed state |
| [ORDER BY](order-by/) | **No** | Bounded inputs | DataFusion sort |
| [LIMIT](limit/) | **No** | Bounded inputs | DataFusion limit |
| [Top-N](top-n/) | **No** | Yes | Custom keyed ranking state |
| [Window Top-N](window-top-n/) | **No** | Yes | Custom per-window ranking state |
| [Deduplication](deduplication/) | **No** | Yes | Custom keyed first/last-row state |
| [Window deduplication](window-deduplication/) | **No** | Yes | Custom per-window keyed state |
| [Pattern recognition](pattern-recognition/) | **No** | Potentially | Custom streaming NFA; no DataFusion equivalent |
| [Changelog conversion](changelog-conversion/) | **No** | Not a compute target | Preserve Flink row-kind conversion |
| [Time travel](time-travel/) | **No** | No | Catalog snapshot resolution stays in Flink |
| [Model inference](model-inference/) | **No** | Provider-dependent | Native provider integration only when parity is proven |
| [Vector search](vector-search/) | **No** | Potentially | DataFusion/custom vector kernels when connector semantics permit |

The future-target column describes architectural possibilities, not implemented support. Each linked page defines the proposed eligibility boundary and expected fallback behavior.

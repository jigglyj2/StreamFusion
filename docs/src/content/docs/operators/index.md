---
title: Operators
description: StreamFusion acceleration and fallback coverage for Flink SQL operators.
sidebar:
  order: 1
---

This matrix follows the query operations documented by Flink 2.3, including the specialized operations with their own reference pages. Support is conservative: an unsupported expression causes the containing Calc, and therefore the all-or-nothing StreamFusion plan, to remain on Flink.

| Operator | Accelerated today? | Future acceleration target | Intended implementation |
| --- | --- | --- | --- |
| [SELECT & WHERE](select-where/) | **Partial** | Yes | DataFusion projections and filters |
| [SELECT DISTINCT](select-distinct/) | **No** | Yes | DataFusion distinct or native keyed state |
| [WITH](with/) | **No** | Not directly | Inlined by Flink; accelerate resulting operators |
| [VALUES](values/) | **Partial** (scalar literals) | Yes | Source-free native Arrow batch |
| [Windowing TVFs](window-tvf/) | **No** | Yes | Native window assignment compatible with Flink |
| [Group aggregation](group-aggregation/) | **No** | Yes | DataFusion aggregates with Flink-managed state |
| [Window aggregation](window-aggregation/) | **No** | Yes | Native window state and DataFusion aggregate kernels |
| [OVER aggregation](over-aggregation/) | **No** | Yes | Native ordered state and aggregate kernels |
| [Joins](joins/) | **No** | By join type | DataFusion batch joins or custom streaming state |
| [Window joins](window-join/) | **No** | Yes | Custom window-aware streaming join |
| [Set operations](set-operations/) | **Partial** (`UNION ALL`) | By operation | Flink-compatible union wiring; DataFusion or native keyed state for future operations |
| [Table and collection expansion](table-expansion/) | **Partial** (scalar array `UNNEST`) | Yes | DataFusion `UnnestExec` with Flink-compatible correlate semantics |
| [ORDER BY](order-by/) | **No** | Bounded inputs | DataFusion sort |
| [LIMIT](limit/) | **No** | Bounded inputs | DataFusion limit |
| [Top-N](top-n/) | **No** | Yes | Custom keyed ranking state |
| [Window Top-N](window-top-n/) | **No** | Yes | Custom per-window ranking state |
| [Deduplication](deduplication/) | **No** | Yes | Custom keyed first/last-row state |
| [Window deduplication](window-deduplication/) | **No** | Yes | Custom per-window keyed state |
| [Pattern recognition](pattern-recognition/) | **No** | Potentially | Custom streaming NFA; no DataFusion equivalent |
| [Changelog conversion](changelog-conversion/) | **Partial** (`DropUpdateBefore`) | Not generally a compute target | Preserve Flink row-kind conversion |
| [Time travel](time-travel/) | **No** | No | Catalog snapshot resolution stays in Flink |
| [Model inference](model-inference/) | **No** | Provider-dependent | Native provider integration only when parity is proven |
| [Vector search](vector-search/) | **No** | Potentially | DataFusion/custom vector kernels when connector semantics permit |

The future-target column describes architectural possibilities, not implemented support. Each linked page defines current eligibility, fallback behavior, SQL syntax, and implementation details.

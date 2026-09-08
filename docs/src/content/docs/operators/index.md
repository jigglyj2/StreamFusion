---
title: Operators
description: StreamFusion acceleration and fallback coverage for Flink SQL operators.
sidebar:
  order: 1
---

This matrix follows the query operations documented by Flink 2.3, including the specialized operations with their own reference pages. Most persistent native state and unverified native combinations are currently [gated during architecture completion](/StreamFusion/development/architecture-admission/). Support is conservative: an unsupported expression causes the containing Calc, and therefore the all-or-nothing StreamFusion plan, to remain on Flink.

| Operator | Accelerated today? | Future acceleration target | Intended implementation |
| --- | --- | --- | --- |
| [SELECT & WHERE](select-where/) | **Partial** (streaming and bounded Calc) | Yes | DataFusion projections and filters |
| [SELECT DISTINCT](select-distinct/) | **Temporarily gated** (whole-plan Flink fallback) | Yes | Native counted keyed state |
| [WITH](with/) | **No** | Not directly | Inlined by Flink; accelerate resulting operators |
| [VALUES](values/) | **Partial** (streaming and bounded scalar literals) | Yes | Source-free native Arrow batch |
| [Windowing TVFs](window-tvf/) | **Partial** (standalone aligned TVFs; `SESSION` gated) | Yes | Native aligned assignment and keyed session merging |
| [Watermark assignment](watermark-assignment/) | **Plan-compatible** | Flink-owned | Distinct StreamFusion node delegating Flink's exact timer and idleness runtime |
| [Group aggregation](group-aggregation/) | **Partial** (synchronous keyed BIGINT aggregates; memory and default RocksDB) | Yes | DataFusion accumulators with Flink keyed state and changelog adapters |
| [Window aggregation](window-aggregation/) | **Partial** (two-phase UTC HOP COUNT/MIN/MAX) | Yes | DataFusion grouped accumulators, native keyed slices and Flink control lifecycle |
| [OVER aggregation](over-aggregation/) | **Temporarily gated** (whole-plan Flink fallback) | Yes | Native ordered state, timers, absorbed batch sort, and aggregate kernels |
| [Joins](joins/) | **Partial** (binary inner MultiJoin with bounded comparisons; memory and default RocksDB) | By join type | Native keyed state, vectorized predicates, and timers |
| [Window joins](window-join/) | **Temporarily gated** (whole-plan Flink fallback) | Yes | Native two-sided window state plus Flink join conditions |
| [Set operations](set-operations/) | **Partial** (`UNION ALL`; stateful rewrites gated) | By physical rewrite | Arrow IPC at Flink multi-input gates; native aggregate/join state and row replication |
| [Exchange](exchange/) | **Partial** (hash and singleton) | Yes | Native Flink-compatible key grouping with Flink-owned network transport |
| [Table and collection expansion](table-expansion/) | **Partial** (standalone supported `UNNEST`; native combinations gated) | Yes | DataFusion `UnnestExec` with Flink-compatible correlate semantics |
| [ORDER BY](order-by/) | **Temporarily gated** (whole-plan Flink fallback) | Yes | Native counted full sort, bounded heap, Top-N, or timer/state sort |
| [LIMIT](limit/) | **Partial** (bounded Arrow-slice limits; streaming stateful paths gated) | Yes | Arrow slicing or native counter/Top-N state |
| [Top-N](top-n/) | **Temporarily gated** (whole-plan Flink fallback) | Yes | Native Arrow ranking state with memory or RocksDB backing |
| [Window Top-N](window-top-n/) | **Temporarily gated** (whole-plan Flink fallback) | Yes | Native per-window state plus Flink's exact generated comparator |
| [Deduplication](deduplication/) | **Temporarily gated** (whole-plan Flink fallback) | Yes | Native batched raw keyed state |
| [Window deduplication](window-deduplication/) | **Temporarily gated** (whole-plan Flink fallback) | Yes | Native retractable per-window keyed state and timers |
| [Pattern recognition](pattern-recognition/) | **Temporarily gated** (whole-plan Flink fallback) | Yes | Native fixed-sequence state machine; Flink CEP fallback for general NFA/timer shapes |
| [Changelog conversion](changelog-conversion/) | **Partial** (`DropUpdateBefore`; normalization gated) | By conversion | Native keyed upsert normalization and Flink-compatible row-kind handling |
| [Time travel](time-travel/) | **No** | No | Catalog snapshot resolution stays in Flink |
| [Model inference](model-inference/) | **No** | Provider-dependent | Native provider integration only when parity is proven |
| [Vector search](vector-search/) | **No** | Potentially | DataFusion/custom vector kernels when connector semantics permit |

The future-target column describes architectural possibilities, not implemented support. Each linked page defines current eligibility, fallback behavior, SQL syntax, and implementation details.

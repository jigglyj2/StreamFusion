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
| [SELECT DISTINCT](select-distinct/) | **Yes** (timer-free streaming) | Yes | Native counted keyed state |
| [WITH](with/) | **No** | Not directly | Inlined by Flink; accelerate resulting operators |
| [VALUES](values/) | **Partial** (scalar literals) | Yes | Source-free native Arrow batch |
| [Windowing TVFs](window-tvf/) | **Yes** (`TUMBLE`, `HOP`, `CUMULATE`, `SESSION`) | Yes | Native aligned assignment and keyed session merging |
| [Watermark assignment](watermark-assignment/) | **Plan-compatible** | Flink-owned | Distinct StreamFusion node delegating Flink's exact timer and idleness runtime |
| [Group aggregation](group-aggregation/) | **Partial** (timer-free keyed) | Yes | Native keyed state and Arrow aggregate kernels |
| [Window aggregation](window-aggregation/) | **Partial** (`TUMBLE`, `HOP`, `CUMULATE`, `SESSION`) | Yes | Native keyed window state, timers, and Arrow aggregate kernels |
| [OVER aggregation](over-aggregation/) | **Partial** (non-time unbounded-preceding `ROWS`/`RANGE`) | Yes | Native ordered state and aggregate kernels |
| [Joins](joins/) | **Partial** (regular and constant-bound interval streaming equi-joins) | By join type | Native two-sided keyed state and timers; future DataFusion batch joins |
| [Window joins](window-join/) | **Yes** (event-time attached windows) | Yes | Native two-sided window state plus Flink join conditions |
| [Set operations](set-operations/) | **Partial** (`UNION ALL`, `UNION DISTINCT`) | By operation | Arrow IPC at Flink multi-input gates; native distinct keyed state |
| [Exchange](exchange/) | **Partial** (hash and singleton) | Yes | Native Flink-compatible key grouping with Flink-owned network transport |
| [Table and collection expansion](table-expansion/) | **Partial** (scalar array `UNNEST`) | Yes | DataFusion `UnnestExec` with Flink-compatible correlate semantics |
| [ORDER BY](order-by/) | **Partial** (streaming finite Top-N) | Yes | Native Top-N state; DataFusion sort for future bounded full sorts |
| [LIMIT](limit/) | **Yes** (streaming constant `LIMIT`/`OFFSET`) | Yes | Native counter/Top-N state with memory or RocksDB backing |
| [Top-N](top-n/) | **Yes** (streaming `ROW_NUMBER`) | Yes | Native Arrow ranking state with memory or RocksDB backing |
| [Window Top-N](window-top-n/) | **Yes** (event-time constant `ROW_NUMBER` range) | Yes | Native per-window state plus Flink's exact generated comparator |
| [Deduplication](deduplication/) | **Partial** (row-time keep-last, including Q18) | Yes | Native batched raw keyed state |
| [Window deduplication](window-deduplication/) | **Yes** (event-time first/last) | Yes | Native retractable per-window keyed state and timers |
| [Pattern recognition](pattern-recognition/) | **No** | Potentially | Custom streaming NFA; no DataFusion equivalent |
| [Changelog conversion](changelog-conversion/) | **Partial** (`ChangelogNormalize`, `DropUpdateBefore`) | By conversion | Native keyed upsert normalization and Flink-compatible row-kind handling |
| [Time travel](time-travel/) | **No** | No | Catalog snapshot resolution stays in Flink |
| [Model inference](model-inference/) | **No** | Provider-dependent | Native provider integration only when parity is proven |
| [Vector search](vector-search/) | **No** | Potentially | DataFusion/custom vector kernels when connector semantics permit |

The future-target column describes architectural possibilities, not implemented support. Each linked page defines current eligibility, fallback behavior, SQL syntax, and implementation details.

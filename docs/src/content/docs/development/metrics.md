---
title: Metric compatibility
description: How accelerated operators preserve Flink's metric contract.
---

StreamFusion treats metrics as part of Flink runtime compatibility. An accelerated
physical operator publishes every metric that its Flink counterpart publishes with the
same name, type, unit, scope, lifecycle, and meaning. Given the same records, control
events, checkpoints, and terminal path, the values must also match. A metric that cannot
be reproduced exactly is a fallback condition rather than a reason to silently omit or
reinterpret it.

Flink's operator runtime continues to own the standard record-rate, watermark, latency,
busy-time, idle-time, and backpressure metrics. StreamFusion operators emit through the
normal Flink `Output` and retain Flink's input lifecycle, so these metrics keep their
normal registration and update paths. Internal Arrow batches do not count as records.
The native exchange similarly corrects its internal IPC-frame counts back to the logical
row counts that the corresponding Flink network edge reports.

The current operator-specific audit is:

| StreamFusion operator | Flink reference | Operator-specific metric handling |
| --- | --- | --- |
| Calc | generated Flink Calc operator | No additional reference metrics; standard Flink metrics are retained. |
| Expand | generated Flink Expand operator | No additional reference metrics; standard Flink metrics are retained. |
| Array/Map/Multiset Unnest | generated Flink Correlate operator | No additional reference metrics; standard Flink metrics are retained. |
| Union All | Flink `UnionStreamOperator` | No additional reference metrics; standard Flink metrics are retained. |
| Values | Flink `ValuesInputFormat` source | No additional reference metrics; standard Flink source metrics are retained. |
| Aligned Window TVF | Flink `AlignedWindowTableFunctionOperator` | Publishes `numNullRowTimeRecordsDropped` and increments it at the same per-record decision point. |
| Drop Update Before | Flink `StreamFilter` with `DropUpdateBeforeFunction` | No additional reference metrics; standard Flink metrics are retained. |
| Watermark Assigner | Flink `WatermarkAssignerOperatorFactory` | Uses Flink's operator directly, including its metrics and backpressure listener lifecycle. |
| Hash/Singleton Exchange | Flink `PartitionTransformation` | Network transport remains Flink-owned; operator/task record counters report logical rows rather than native IPC frames. |

StreamFusion-specific diagnostics are additive and use distinct names; they do not
replace Flink metrics. Current native managed-memory gauges expose used, peak, and
assigned bytes. As fused plans gain more stateful DataFusion operators, native plan-node
metrics will be mapped back to their corresponding Java physical nodes using stable
protobuf identities, following Comet's metric-tree model.

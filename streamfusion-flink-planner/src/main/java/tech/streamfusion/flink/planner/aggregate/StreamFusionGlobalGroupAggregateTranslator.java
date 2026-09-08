/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner.aggregate;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.metrics.NativeStateMetricSupport;

/** Builds the global consumer's protobuf fragment; routing and runtime belong to the shared region. */
public final class StreamFusionGlobalGroupAggregateTranslator {
    private StreamFusionGlobalGroupAggregateTranslator() {}

    public static byte[] createStagePlan(
            RowType originalInputType,
            RowType internalInputType,
            RowType outputType,
            int groupingCount,
            AggregateCall[] calls,
            boolean[] retractable,
            boolean generateUpdateBefore,
            boolean needRetraction,
            long stateRetentionTime,
            ReadableConfig config) {
        String reason = unsupportedStageReason(
                originalInputType,
                internalInputType,
                outputType,
                groupingCount,
                calls,
                retractable,
                needRetraction,
                stateRetentionTime,
                config);
        if (reason != null) throw new IllegalArgumentException(reason);
        return StreamFusionGroupAggregatePlan.createGlobal(
                originalInputType,
                internalInputType,
                outputType,
                groupingCount,
                calls,
                retractable,
                generateUpdateBefore,
                config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE));
    }

    public static String unsupportedStageReason(
            RowType originalInputType,
            RowType internalInputType,
            RowType outputType,
            int groupingCount,
            AggregateCall[] calls,
            boolean[] retractable,
            boolean needRetraction,
            long stateRetentionTime,
            ReadableConfig config) {
        if (groupingCount < 0 || internalInputType.getFieldCount() != groupingCount + 1) {
            return "schema: global aggregate input must contain grouping fields followed by one opaque accumulator";
        }
        var accumulator = internalInputType.getTypeAt(groupingCount);
        if (accumulator.getTypeRoot() != LogicalTypeRoot.VARBINARY || accumulator.isNullable()) {
            return "schema: global aggregate requires a non-null VARBINARY accumulator";
        }
        if (calls.length != retractable.length) {
            return "aggregate: calls and retraction requirements must be equally sized";
        }
        if (outputType.getFieldCount() != groupingCount + calls.length) {
            return "schema: global aggregate output must contain grouping fields followed by aggregate values";
        }
        String stateReason = StreamFusionGroupAggregateTranslator.unsupportedStateReason(stateRetentionTime, config);
        if (stateReason != null) return stateReason;
        if (!config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)) {
            return "mini-batch: global aggregate requires enabled mini-batching";
        }
        for (int index = 0; index < groupingCount; index++) {
            if (!internalInputType.getTypeAt(index).equals(outputType.getTypeAt(index))) {
                return "key[" + index + "]: global input and output types must match exactly";
            }
        }
        for (int index = 0; index < calls.length; index++) {
            // Calls address the original SQL row, not the key-plus-opaque-accumulator input.
            String reason = StreamFusionGroupAggregateTranslator.unsupportedCall(
                    originalInputType, outputType.getTypeAt(groupingCount + index), calls[index]);
            if (reason != null) return "aggregate[" + index + "]: " + reason;
            if (needRetraction && !retractable[index]) {
                return "aggregate[" + index + "]: Flink did not select a retractable accumulator";
            }
        }
        return NativeStateMetricSupport.unsupportedReason(config);
    }
}

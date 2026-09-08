/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner.aggregate;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.aggregate.StreamFusionArrowLocalGroupAggregateOperator;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;

/** Runtime translation for the native local half of two-phase group aggregation. */
public final class StreamFusionLocalGroupAggregateTranslator {
    private StreamFusionLocalGroupAggregateTranslator() {}

    /** A protobuf fragment only; the shared region owns data and control execution. */
    public static byte[] createStagePlan(
            RowType inputType,
            RowType internalOutputType,
            int[] grouping,
            AggregateCall[] calls,
            boolean[] retractable,
            boolean inputChangelog,
            org.apache.flink.configuration.ReadableConfig config) {
        String reason = unsupportedStageReason(
                inputType, internalOutputType, grouping, calls, retractable, inputChangelog, config);
        if (reason != null) throw new IllegalArgumentException(reason);
        return StreamFusionGroupAggregatePlan.createLocal(
                inputType,
                internalOutputType,
                grouping,
                calls,
                retractable,
                inputChangelog,
                config.get(org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE),
                2);
    }

    public static String unsupportedStageReason(
            RowType inputType,
            RowType internalOutputType,
            int[] grouping,
            AggregateCall[] calls,
            boolean[] retractable,
            boolean inputChangelog,
            org.apache.flink.configuration.ReadableConfig config) {
        if (!config.get(org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)
                || config.get(org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE) <= 0)
            return "mini-batch: local aggregate requires enabled mini-batching with a positive size";
        if (internalOutputType.getFieldCount() != grouping.length + 1)
            return "schema: local aggregate output must contain grouping fields followed by one opaque accumulator";
        var accumulator = internalOutputType.getTypeAt(grouping.length);
        if (accumulator.getTypeRoot() != org.apache.flink.table.types.logical.LogicalTypeRoot.VARBINARY
                || accumulator.isNullable()) return "schema: local aggregate requires a non-null VARBINARY accumulator";
        if (calls.length != retractable.length)
            return "aggregate: calls and retraction requirements must be equally sized";
        for (int i = 0; i < grouping.length; i++) {
            int index = grouping[i];
            if (index < 0 || index >= inputType.getFieldCount())
                return "key: index " + index + " is outside the input row";
            if (!inputType.getTypeAt(index).equals(internalOutputType.getTypeAt(i)))
                return "key[" + i + "]: local input and output types must match exactly";
        }
        for (int i = 0; i < calls.length; i++) {
            String reason = StreamFusionGroupAggregateTranslator.unsupportedCall(
                    inputType,
                    org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType(calls[i].getType()),
                    calls[i]);
            if (reason != null) return "aggregate[" + i + "]: " + reason;
            if (inputChangelog && !retractable[i])
                return "aggregate[" + i + "]: Flink did not select a retractable accumulator";
        }
        return null;
    }

    public static Transformation<RowData> translateBatch(
            Transformation<RowData> input,
            RowType inputType,
            RowType internalOutputType,
            int[] grouping,
            AggregateCall[] calls,
            RowDataKeySelector keySelector,
            boolean hashAggregateMetrics) {
        byte[] plan = StreamFusionGroupAggregatePlan.createBoundedLocal(inputType, internalOutputType, grouping, calls);
        Transformation<ArrowRowDataBatch> arrowInput = StreamFusionArrowBoundaries.toArrow(input, inputType);
        OneInputTransformation<ArrowRowDataBatch, ArrowRowDataBatch> transformation = new OneInputTransformation<>(
                arrowInput,
                "streamfusion-batch-local-group-aggregate",
                new StreamFusionArrowLocalGroupAggregateOperator(
                        plan,
                        inputType,
                        internalOutputType,
                        grouping,
                        false,
                        keySelector,
                        hashAggregateMetrics && grouping.length > 0),
                ArrowRowDataBatchTypeInfo.INSTANCE,
                input.getParallelism(),
                false);
        transformation.declareManagedMemoryUseCaseAtOperatorScope(
                ManagedMemoryUseCase.OPERATOR, AggregateManagedMemoryWeights.BATCH);
        return StreamFusionArrowBoundaries.asPlannerTransformation(transformation);
    }
}

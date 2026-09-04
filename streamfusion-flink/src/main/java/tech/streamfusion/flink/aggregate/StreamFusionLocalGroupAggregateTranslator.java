/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.aggregate;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;

/** Runtime translation for the native local half of two-phase group aggregation. */
public final class StreamFusionLocalGroupAggregateTranslator {
    private StreamFusionLocalGroupAggregateTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> input,
            RowType inputType,
            RowType internalOutputType,
            int[] grouping,
            AggregateCall[] calls,
            boolean[] retractable,
            boolean inputChangelog,
            long miniBatchSize,
            RowDataKeySelector keySelector) {
        byte[] plan = StreamFusionGroupAggregatePlan.createLocal(
                inputType, internalOutputType, grouping, calls, retractable, inputChangelog, miniBatchSize);
        Transformation<ArrowRowDataBatch> arrowInput = StreamFusionArrowBoundaries.toArrow(input, inputType);
        OneInputTransformation<ArrowRowDataBatch, ArrowRowDataBatch> transformation = new OneInputTransformation<>(
                arrowInput,
                "streamfusion-local-group-aggregate",
                new StreamFusionArrowLocalGroupAggregateOperator(
                        plan, inputType, internalOutputType, grouping, inputChangelog, keySelector),
                ArrowRowDataBatchTypeInfo.INSTANCE,
                input.getParallelism(),
                false);
        transformation.declareManagedMemoryUseCaseAtOperatorScope(
                ManagedMemoryUseCase.OPERATOR, AggregateManagedMemoryWeights.LOCAL);
        return StreamFusionArrowBoundaries.asPlannerTransformation(transformation);
    }
}

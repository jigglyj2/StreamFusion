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
import tech.streamfusion.flink.deduplicate.ArrowBatchKeySelector;

/** Runtime translation for Flink's native incremental mini-batch aggregate stage. */
public final class StreamFusionIncrementalGroupAggregateTranslator {
    private StreamFusionIncrementalGroupAggregateTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> input,
            RowType partialOriginalInputType,
            RowType internalInputType,
            RowType internalOutputType,
            int partialGroupingCount,
            int[] finalGrouping,
            AggregateCall[] partialCalls,
            boolean[] partialRetractable,
            RowType finalOriginalInputType,
            AggregateCall[] finalCalls,
            boolean[] finalRetractable,
            long miniBatchSize,
            RowDataKeySelector keySelector) {
        byte[] plan = StreamFusionGroupAggregatePlan.createIncremental(
                partialOriginalInputType,
                internalInputType,
                internalOutputType,
                partialGroupingCount,
                finalGrouping,
                partialCalls,
                partialRetractable,
                finalOriginalInputType,
                finalCalls,
                finalRetractable,
                miniBatchSize);
        Transformation<ArrowRowDataBatch> arrowInput = StreamFusionArrowBoundaries.toArrow(input, internalInputType);
        OneInputTransformation<ArrowRowDataBatch, ArrowRowDataBatch> transformation = new OneInputTransformation<>(
                arrowInput,
                "streamfusion-incremental-group-aggregate",
                new StreamFusionArrowGroupAggregateOperator(
                        internalInputType,
                        internalOutputType,
                        java.util.stream.IntStream.range(0, partialGroupingCount)
                                .toArray(),
                        plan,
                        false,
                        keySelector,
                        miniBatchSize,
                        "incremental group aggregate"),
                ArrowRowDataBatchTypeInfo.INSTANCE,
                input.getParallelism(),
                false);
        transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);
        if (input.getMaxParallelism() > 0) {
            transformation.setMaxParallelism(input.getMaxParallelism());
        }
        transformation.setStateKeySelector(new ArrowBatchKeySelector(keySelector));
        transformation.setStateKeyType(keySelector.getProducedType());
        return StreamFusionArrowBoundaries.asPlannerTransformation(transformation);
    }
}

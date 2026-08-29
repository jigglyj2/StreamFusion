/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.watermark;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedWatermarkGenerator;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;

/** Reflection entry point for the Arrow-native watermark operator. */
public final class StreamFusionWatermarkTranslator {
    private StreamFusionWatermarkTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> input,
            RowType rowType,
            int rowtimeFieldIndex,
            long idleTimeout,
            GeneratedWatermarkGenerator generator) {
        Transformation<ArrowRowDataBatch> arrowInput = StreamFusionArrowBoundaries.toArrow(input, rowType);
        OneInputTransformation<ArrowRowDataBatch, ArrowRowDataBatch> transformation = new OneInputTransformation<>(
                arrowInput,
                "streamfusion-arrow-watermark-assigner",
                new StreamFusionArrowWatermarkAssignerOperatorFactory(rowtimeFieldIndex, idleTimeout, generator),
                ArrowRowDataBatchTypeInfo.INSTANCE,
                input.getParallelism(),
                false);
        transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);
        return StreamFusionArrowBoundaries.asPlannerTransformation(transformation);
    }
}

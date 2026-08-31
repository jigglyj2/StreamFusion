/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.watermark;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedWatermarkGenerator;
import org.apache.flink.table.runtime.operators.wmassigners.WatermarkAssignerOperatorFactory;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;

/** Keeps Flink watermark control on the source side of the one RowData-to-Arrow boundary. */
public final class StreamFusionWatermarkTranslator {
    private StreamFusionWatermarkTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> input,
            RowType rowType,
            int rowtimeFieldIndex,
            long idleTimeout,
            GeneratedWatermarkGenerator generator) {
        if (((Object) input.getOutputType()) instanceof ArrowRowDataBatchTypeInfo) {
            @SuppressWarnings("unchecked")
            Transformation<ArrowRowDataBatch> arrowInput =
                    (Transformation<ArrowRowDataBatch>) (Transformation<?>) input;
            OneInputTransformation<ArrowRowDataBatch, ArrowRowDataBatch> watermark = new OneInputTransformation<>(
                    arrowInput,
                    "streamfusion-arrow-watermark-assigner",
                    new StreamFusionArrowWatermarkAssignerOperatorFactory(rowtimeFieldIndex, idleTimeout, generator),
                    ArrowRowDataBatchTypeInfo.INSTANCE,
                    input.getParallelism(),
                    false);
            return StreamFusionArrowBoundaries.asPlannerTransformation(watermark);
        }
        OneInputTransformation<RowData, RowData> watermark = new OneInputTransformation<>(
                input,
                "streamfusion-source-watermark-assigner",
                new WatermarkAssignerOperatorFactory(rowtimeFieldIndex, idleTimeout, generator),
                input.getOutputType(),
                input.getParallelism(),
                false);
        return watermark;
    }
}

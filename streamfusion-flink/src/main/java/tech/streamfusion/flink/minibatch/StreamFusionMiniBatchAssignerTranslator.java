/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.minibatch;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.trait.MiniBatchMode;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;

/** Reflection entry point for Flink's watermark-backed mini-batch control node. */
public final class StreamFusionMiniBatchAssignerTranslator {
    private StreamFusionMiniBatchAssignerTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> input, RowType rowType, long intervalMillis, MiniBatchMode mode) {
        Transformation<ArrowRowDataBatch> arrowInput = StreamFusionArrowBoundaries.toArrow(input, rowType);
        OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch> operator;
        if (mode == MiniBatchMode.ProcTime) {
            operator = new StreamFusionArrowProcTimeMiniBatchAssignerOperator(intervalMillis);
        } else if (mode == MiniBatchMode.RowTime) {
            operator = new StreamFusionArrowRowTimeMiniBatchAssignerOperator(intervalMillis);
        } else {
            throw new IllegalArgumentException("Unsupported mini-batch mode " + mode);
        }
        OneInputTransformation<ArrowRowDataBatch, ArrowRowDataBatch> transformation = new OneInputTransformation<>(
                arrowInput,
                "streamfusion-mini-batch-assigner",
                operator,
                ArrowRowDataBatchTypeInfo.INSTANCE,
                input.getParallelism(),
                false);
        return StreamFusionArrowBoundaries.asPlannerTransformation(transformation);
    }
}

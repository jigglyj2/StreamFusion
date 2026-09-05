/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.limit;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.ArrowUtils;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;

/** Reflection entry point for bounded local/global LIMIT. */
public final class StreamFusionBoundedLimitTranslator {
    private StreamFusionBoundedLimitTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> input, RowType inputType, long start, long end, boolean global) {
        if (unsupportedReason(inputType, start, end) != null) {
            return null;
        }
        Transformation<ArrowRowDataBatch> arrowInput = StreamFusionArrowBoundaries.toArrow(input, inputType);
        OneInputTransformation<ArrowRowDataBatch, ArrowRowDataBatch> result = new OneInputTransformation<>(
                arrowInput,
                "streamfusion-bounded-limit[" + (global ? "global" : "local") + "]",
                new StreamFusionArrowBoundedLimitOperator(global, start, end),
                ArrowRowDataBatchTypeInfo.INSTANCE,
                input.getParallelism(),
                false);
        return StreamFusionArrowBoundaries.asPlannerTransformation(result);
    }

    public static String unsupportedReason(RowType inputType, long start, long end) {
        if (start < 0 || end < start) {
            return "limit: invalid half-open range [" + start + ", " + end + ")";
        }
        try {
            ArrowUtils.toArrowSchema(inputType);
        } catch (RuntimeException failure) {
            return "schema: " + failure.getMessage();
        }
        return null;
    }
}

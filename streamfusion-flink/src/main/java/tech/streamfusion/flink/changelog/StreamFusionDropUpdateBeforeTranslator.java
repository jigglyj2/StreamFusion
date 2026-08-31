/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.changelog;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;

/** Runtime translation entry point for StreamFusion's changelog metadata filter. */
public final class StreamFusionDropUpdateBeforeTranslator {
    private StreamFusionDropUpdateBeforeTranslator() {}

    public static Transformation<RowData> translate(Transformation<RowData> input, RowType rowType) {
        OneInputTransformation<ArrowRowDataBatch, ArrowRowDataBatch> transformation = new OneInputTransformation<>(
                StreamFusionArrowBoundaries.toArrow(input, rowType),
                "streamfusion-drop-update-before",
                new StreamFusionDropUpdateBeforeOperator(),
                ArrowRowDataBatchTypeInfo.INSTANCE,
                input.getParallelism(),
                false);
        return StreamFusionArrowBoundaries.asPlannerTransformation(transformation);
    }
}

/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

/** Inserts source/sink Arrow boundaries and never inserts them between StreamFusion operators. */
public final class StreamFusionArrowBoundaries {
    private StreamFusionArrowBoundaries() {}

    @SuppressWarnings("unchecked")
    public static Transformation<ArrowRowDataBatch> toArrow(Transformation<RowData> input, RowType rowType) {
        if (((Object) input.getOutputType()) instanceof ArrowRowDataBatchTypeInfo) {
            return (Transformation<ArrowRowDataBatch>) (Transformation<?>) input;
        }
        OneInputTransformation<RowData, ArrowRowDataBatch> boundary = new OneInputTransformation<>(
                input,
                "streamfusion-rowdata-to-arrow",
                new RowDataToArrowBatchOperator(rowType),
                ArrowRowDataBatchTypeInfo.INSTANCE,
                input.getParallelism(),
                false);
        // The source edge owns both the reusable transposition vectors and in-flight exported
        // buffers. Give that conversion two shares so wide variable-width batches can grow while
        // remaining inside Flink's existing managed-memory budget.
        boundary.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 2);
        return boundary;
    }

    /** Inserts a source boundary that writes only fields consumed by the first native operator. */
    public static Transformation<ArrowRowDataBatch> toArrow(
            Transformation<RowData> input, RowType projectedType, int[][] inputFieldPaths, int[][] inputRowArities) {
        if (((Object) input.getOutputType()) instanceof ArrowRowDataBatchTypeInfo) {
            throw new IllegalArgumentException("Cannot project an Arrow input at an internal operator boundary");
        }
        OneInputTransformation<RowData, ArrowRowDataBatch> boundary = new OneInputTransformation<>(
                input,
                "streamfusion-rowdata-to-arrow",
                new RowDataToArrowBatchOperator(projectedType, inputFieldPaths, inputRowArities),
                ArrowRowDataBatchTypeInfo.INSTANCE,
                input.getParallelism(),
                false);
        boundary.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 2);
        return boundary;
    }

    public static boolean isArrow(Transformation<RowData> input) {
        return ((Object) input.getOutputType()) instanceof ArrowRowDataBatchTypeInfo;
    }

    @SuppressWarnings("unchecked")
    public static Transformation<RowData> toRowData(Transformation<RowData> input, RowType rowType) {
        if (!(((Object) input.getOutputType()) instanceof ArrowRowDataBatchTypeInfo)) {
            return input;
        }
        Transformation<ArrowRowDataBatch> arrowInput = (Transformation<ArrowRowDataBatch>) (Transformation<?>) input;
        return new OneInputTransformation<>(
                arrowInput,
                "streamfusion-arrow-to-rowdata",
                new ArrowBatchToRowDataOperator(),
                InternalTypeInfo.of(rowType),
                input.getParallelism(),
                false);
    }

    /** Bridges Flink planner's RowData generic while retaining Arrow runtime type information. */
    @SuppressWarnings("unchecked")
    public static Transformation<RowData> asPlannerTransformation(Transformation<ArrowRowDataBatch> transformation) {
        return (Transformation<RowData>) (Transformation<?>) transformation;
    }
}

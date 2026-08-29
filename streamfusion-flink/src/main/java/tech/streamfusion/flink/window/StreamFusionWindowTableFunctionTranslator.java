/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.window;

import java.time.Duration;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.logical.CumulativeWindowSpec;
import org.apache.flink.table.planner.plan.logical.HoppingWindowSpec;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.TumblingWindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.proto.plan.v1.WindowKind;

/** Reflection entry point used by the planner extension for native aligned Window TVFs. */
public final class StreamFusionWindowTableFunctionTranslator {
    private StreamFusionWindowTableFunctionTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> input,
            RowType inputType,
            RowType outputType,
            TimeAttributeWindowingStrategy strategy) {
        if (unsupportedReason(inputType, outputType, strategy) != null) {
            return null;
        }
        WindowParameters parameters = parameters(strategy.getWindow());
        OneInputTransformation<RowData, RowData> transformation = new OneInputTransformation<>(
                input,
                "streamfusion-window-table-function[" + parameters.kind + "]",
                new StreamFusionWindowTableFunctionOperator(
                        inputType, outputType, strategy.getTimeAttributeIndex(), parameters),
                InternalTypeInfo.of(outputType),
                input.getParallelism(),
                false);
        transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);
        return transformation;
    }

    public static String unsupportedReason(
            RowType inputType, RowType outputType, TimeAttributeWindowingStrategy strategy) {
        if (strategy.isProctime()) {
            return "time attribute: processing-time Window TVFs require per-record Flink clock parity";
        }
        if (strategy.getTimeAttributeType().getTypeRoot() != LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
            return "time attribute: only TIMESTAMP rowtime is native; TIMESTAMP_LTZ requires Flink local-time-zone shifting";
        }
        int index = strategy.getTimeAttributeIndex();
        if (index < 0 || index >= inputType.getFieldCount()) {
            return "time attribute: index is outside the input row";
        }
        if (outputType.getFieldCount() != inputType.getFieldCount() + 3) {
            return "output: aligned Window TVF must append window_start, window_end, and window_time";
        }
        try {
            parameters(strategy.getWindow());
            return null;
        } catch (IllegalArgumentException unsupported) {
            return "window: " + unsupported.getMessage();
        }
    }

    static WindowParameters parameters(WindowSpec spec) {
        if (spec instanceof TumblingWindowSpec) {
            TumblingWindowSpec tumble = (TumblingWindowSpec) spec;
            return new WindowParameters(
                    WindowKind.WINDOW_KIND_TUMBLE, tumble.getSize().toMillis(), 0, millis(tumble.getOffset()));
        }
        if (spec instanceof HoppingWindowSpec) {
            HoppingWindowSpec hop = (HoppingWindowSpec) spec;
            return new WindowParameters(
                    WindowKind.WINDOW_KIND_HOP,
                    hop.getSize().toMillis(),
                    hop.getSlide().toMillis(),
                    millis(hop.getOffset()));
        }
        if (spec instanceof CumulativeWindowSpec) {
            CumulativeWindowSpec cumulate = (CumulativeWindowSpec) spec;
            return new WindowParameters(
                    WindowKind.WINDOW_KIND_CUMULATE,
                    cumulate.getMaxSize().toMillis(),
                    cumulate.getStep().toMillis(),
                    millis(cumulate.getOffset()));
        }
        throw new IllegalArgumentException(spec.getClass().getSimpleName() + " is not an aligned Window TVF");
    }

    private static long millis(Duration duration) {
        return duration == null ? 0 : duration.toMillis();
    }

    static final class WindowParameters {
        final WindowKind kind;
        final long sizeMillis;
        final long slideOrStepMillis;
        final long offsetMillis;

        WindowParameters(WindowKind kind, long sizeMillis, long slideOrStepMillis, long offsetMillis) {
            this.kind = kind;
            this.sizeMillis = sizeMillis;
            this.slideOrStepMillis = slideOrStepMillis;
            this.offsetMillis = offsetMillis;
        }
    }
}

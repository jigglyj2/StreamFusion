/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.window;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM;

import java.time.Duration;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.planner.plan.logical.HoppingWindowSpec;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.plan.logical.SessionGroupWindow;
import org.apache.flink.table.planner.plan.logical.SessionWindowSpec;
import org.apache.flink.table.planner.plan.logical.SlidingGroupWindow;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.TumblingGroupWindow;
import org.apache.flink.table.planner.plan.logical.TumblingWindowSpec;
import org.apache.flink.table.planner.plan.utils.WindowEmitStrategy;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;
import tech.streamfusion.flink.deduplicate.ArrowBatchKeySelector;
import tech.streamfusion.flink.exchange.StreamFusionExchangeTranslator;
import tech.streamfusion.flink.state.StreamFusionStateBackendFactory;
import tech.streamfusion.proto.plan.v1.WindowKind;

/** Lowers legacy group-window syntax onto the canonical native window state machine. */
public final class StreamFusionGroupWindowAggregateTranslator {
    private static final int STATEFUL_MANAGED_MEMORY_WEIGHT = 8;

    private StreamFusionGroupWindowAggregateTranslator() {}

    public static Transformation<RowData> translateBatchLocal(
            Transformation<RowData> input,
            RowType inputType,
            RowType internalOutputType,
            int[] grouping,
            AggregateCall[] calls,
            LogicalWindow window,
            ReadableConfig config) {
        LegacyWindow planned = plan(window, grouping, config);
        if (planned.reason != null || planned.timeStrategy == null) {
            return null;
        }
        return StreamFusionWindowAggregateTranslator.translateBatchLocal(
                input, inputType, internalOutputType, grouping, calls, planned.timeStrategy, config);
    }

    public static Transformation<RowData> translateBatchGlobal(
            Transformation<RowData> input,
            RowType originalInputType,
            RowType internalInputType,
            RowType outputType,
            int groupingCount,
            AggregateCall[] calls,
            LogicalWindow window,
            NamedWindowProperty[] properties,
            ReadableConfig config,
            StreamExecutionEnvironment environment,
            RowDataKeySelector keySelector) {
        int[] grouping = java.util.stream.IntStream.range(0, groupingCount).toArray();
        LegacyWindow planned = plan(window, grouping, config);
        if (planned.reason != null || planned.timeStrategy == null) {
            return null;
        }
        return StreamFusionWindowAggregateTranslator.translateBatchGlobal(
                input,
                originalInputType,
                internalInputType,
                outputType,
                groupingCount,
                calls,
                planned.timeStrategy,
                properties,
                config,
                environment,
                keySelector);
    }

    public static String unsupportedBatchReason(
            RowType inputType,
            RowType outputType,
            int[] grouping,
            AggregateCall[] calls,
            LogicalWindow window,
            NamedWindowProperty[] properties,
            ReadableConfig config) {
        LegacyWindow planned = plan(window, grouping, config);
        if (planned.reason != null) {
            return planned.reason;
        }
        if (planned.timeStrategy == null) {
            return "bounded two-phase window aggregate: row-count windows are not yet native";
        }
        if (planned.timeStrategy.getWindow() instanceof SessionWindowSpec) {
            return "bounded two-phase window aggregate: SESSION is not a slicing local/global window";
        }
        return unsupportedReason(inputType, outputType, grouping, calls, window, properties, false, config);
    }

    public static Transformation<RowData> translate(
            Transformation<RowData> input,
            RowType inputType,
            RowType outputType,
            int[] grouping,
            AggregateCall[] calls,
            LogicalWindow window,
            NamedWindowProperty[] properties,
            boolean needRetraction,
            ReadableConfig config,
            StreamExecutionEnvironment environment,
            RowDataKeySelector keySelector) {
        LegacyWindow planned = plan(window, grouping, config);
        String reason =
                unsupportedReason(inputType, outputType, grouping, calls, window, properties, needRetraction, config);
        if (reason != null) {
            return null;
        }
        if (!planned.countWindow) {
            return StreamFusionWindowAggregateTranslator.translate(
                    input,
                    inputType,
                    outputType,
                    grouping,
                    calls,
                    planned.timeStrategy,
                    properties,
                    needRetraction,
                    config,
                    environment,
                    keySelector);
        }

        StreamFusionStateBackendFactory.install(environment);
        byte[] nativePlan = StreamFusionWindowAggregatePlan.create(
                inputType,
                outputType,
                grouping,
                calls,
                needRetraction,
                needRetraction,
                planned.parameters,
                Math.max(0, window.timeAttribute().getFieldIndex()),
                -1,
                -1,
                true,
                "UTC",
                properties);
        Transformation<RowData> partitionedInput = input;
        if (!"StreamFusionExchangeReader".equals(input.getName())) {
            partitionedInput = grouping.length == 0
                    ? StreamFusionExchangeTranslator.singleton(input, inputType)
                    : StreamFusionExchangeTranslator.hash(
                            input,
                            inputType,
                            grouping,
                            DEFAULT_LOWER_BOUND_MAX_PARALLELISM,
                            environment.getParallelism(),
                            config.get(CheckpointingOptions.ENABLE_UNALIGNED)
                                    || config.get(CheckpointingOptions.FORCE_UNALIGNED));
        }
        Transformation<ArrowRowDataBatch> arrowInput = StreamFusionArrowBoundaries.toArrow(partitionedInput, inputType);
        OneInputTransformation<ArrowRowDataBatch, ArrowRowDataBatch> transformation = new OneInputTransformation<>(
                arrowInput,
                "streamfusion-group-window-aggregate[" + planned.parameters.kind + "]",
                new StreamFusionArrowWindowAggregateOperator(
                        inputType, outputType, grouping, nativePlan, needRetraction, true, keySelector),
                ArrowRowDataBatchTypeInfo.INSTANCE,
                partitionedInput.getParallelism(),
                false);
        if (partitionedInput.getMaxParallelism() > 0) {
            transformation.setMaxParallelism(partitionedInput.getMaxParallelism());
        }
        transformation.declareManagedMemoryUseCaseAtOperatorScope(
                ManagedMemoryUseCase.OPERATOR, STATEFUL_MANAGED_MEMORY_WEIGHT);
        transformation.setStateKeySelector(new ArrowBatchKeySelector(keySelector));
        transformation.setStateKeyType(keySelector.getProducedType());
        return StreamFusionArrowBoundaries.asPlannerTransformation(transformation);
    }

    public static String unsupportedReason(
            RowType inputType,
            RowType outputType,
            int[] grouping,
            AggregateCall[] calls,
            LogicalWindow window,
            NamedWindowProperty[] properties,
            boolean needRetraction,
            ReadableConfig config) {
        LegacyWindow planned = plan(window, grouping, config);
        if (planned.reason != null) {
            return planned.reason;
        }
        if (planned.countWindow && properties.length != 0) {
            return "legacy group window: count windows do not define time window properties";
        }
        TimeAttributeWindowingStrategy validationStrategy = planned.timeStrategy;
        if (validationStrategy == null) {
            validationStrategy = new TimeAttributeWindowingStrategy(
                    new TumblingWindowSpec(Duration.ofMillis(1), null),
                    window.timeAttribute().getOutputDataType().getLogicalType(),
                    Math.max(0, window.timeAttribute().getFieldIndex()));
        }
        return StreamFusionWindowAggregateTranslator.unsupportedReason(
                inputType, outputType, grouping, calls, validationStrategy, properties, needRetraction, config);
    }

    private static LegacyWindow plan(LogicalWindow window, int[] grouping, ReadableConfig config) {
        if (WindowEmitStrategy.apply(config, window).produceUpdates()) {
            return LegacyWindow.unsupported(
                    "legacy group window: early/late firing is not implemented by native window aggregation");
        }
        boolean processingTime = LogicalTypeChecks.isProctimeAttribute(
                window.timeAttribute().getOutputDataType().getLogicalType());
        if (window instanceof TumblingGroupWindow) {
            ValueLiteralExpression size = ((TumblingGroupWindow) window).size();
            boolean countInterval = size.getOutputDataType().getLogicalType().is(LogicalTypeRoot.BIGINT);
            if (!countInterval) {
                Duration duration = size.getValueAs(Duration.class).orElse(null);
                if (duration == null) {
                    return LegacyWindow.unsupported(
                            "legacy group window: TUMBLE size must be a day-time or row-count interval");
                }
                return LegacyWindow.time(new TimeAttributeWindowingStrategy(
                        new TumblingWindowSpec(duration, null),
                        window.timeAttribute().getOutputDataType().getLogicalType(),
                        window.timeAttribute().getFieldIndex()));
            }
            Long count = size.getValueAs(Long.class).orElse(null);
            if (!processingTime || count == null || count <= 0) {
                return LegacyWindow.unsupported(
                        "legacy group window: row-count TUMBLE requires a positive processing-time count");
            }
            return LegacyWindow.count(new StreamFusionWindowTableFunctionTranslator.WindowParameters(
                    WindowKind.WINDOW_KIND_COUNT_TUMBLE, count, 0, 0));
        }
        if (window instanceof SlidingGroupWindow) {
            SlidingGroupWindow sliding = (SlidingGroupWindow) window;
            boolean countInterval = sliding.size()
                            .getOutputDataType()
                            .getLogicalType()
                            .is(LogicalTypeRoot.BIGINT)
                    && sliding.slide().getOutputDataType().getLogicalType().is(LogicalTypeRoot.BIGINT);
            if (!countInterval) {
                Duration size = sliding.size().getValueAs(Duration.class).orElse(null);
                Duration slide = sliding.slide().getValueAs(Duration.class).orElse(null);
                if (size == null || slide == null) {
                    return LegacyWindow.unsupported(
                            "legacy group window: HOP size and slide must use the same day-time or row-count interval family");
                }
                return LegacyWindow.time(new TimeAttributeWindowingStrategy(
                        new HoppingWindowSpec(size, slide, null),
                        window.timeAttribute().getOutputDataType().getLogicalType(),
                        window.timeAttribute().getFieldIndex()));
            }
            Long countSize = sliding.size().getValueAs(Long.class).orElse(null);
            Long countSlide = sliding.slide().getValueAs(Long.class).orElse(null);
            if (!processingTime || countSize == null || countSlide == null || countSize <= 0 || countSlide <= 0) {
                return LegacyWindow.unsupported(
                        "legacy group window: row-count HOP requires positive processing-time size and slide");
            }
            return LegacyWindow.count(new StreamFusionWindowTableFunctionTranslator.WindowParameters(
                    WindowKind.WINDOW_KIND_COUNT_HOP, countSize, countSlide, 0));
        }
        if (window instanceof SessionGroupWindow) {
            Duration gap = ((SessionGroupWindow) window)
                    .gap()
                    .getValueAs(Duration.class)
                    .orElse(null);
            if (gap == null) {
                return LegacyWindow.unsupported("legacy group window: SESSION gap must be a day-time interval");
            }
            return LegacyWindow.time(new TimeAttributeWindowingStrategy(
                    new SessionWindowSpec(gap, grouping),
                    window.timeAttribute().getOutputDataType().getLogicalType(),
                    window.timeAttribute().getFieldIndex()));
        }
        return LegacyWindow.unsupported("legacy group window: unsupported logical window "
                + window.getClass().getSimpleName());
    }

    private static final class LegacyWindow {
        private final boolean countWindow;
        private final TimeAttributeWindowingStrategy timeStrategy;
        private final StreamFusionWindowTableFunctionTranslator.WindowParameters parameters;
        private final String reason;

        private LegacyWindow(
                boolean countWindow,
                TimeAttributeWindowingStrategy timeStrategy,
                StreamFusionWindowTableFunctionTranslator.WindowParameters parameters,
                String reason) {
            this.countWindow = countWindow;
            this.timeStrategy = timeStrategy;
            this.parameters = parameters;
            this.reason = reason;
        }

        private static LegacyWindow time(TimeAttributeWindowingStrategy strategy) {
            return new LegacyWindow(false, strategy, null, null);
        }

        private static LegacyWindow count(StreamFusionWindowTableFunctionTranslator.WindowParameters parameters) {
            return new LegacyWindow(true, null, parameters, null);
        }

        private static LegacyWindow unsupported(String reason) {
            return new LegacyWindow(false, null, null, reason);
        }
    }
}

/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.window;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM;

import java.time.Duration;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.logical.CumulativeWindowSpec;
import org.apache.flink.table.planner.plan.logical.HoppingWindowSpec;
import org.apache.flink.table.planner.plan.logical.SessionWindowSpec;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.TumblingWindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowSpec;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;
import tech.streamfusion.flink.deduplicate.ArrowBatchKeySelector;
import tech.streamfusion.flink.exchange.StreamFusionExchangeTranslator;
import tech.streamfusion.flink.operator.StreamFusionArrowNativeOperator;
import tech.streamfusion.flink.state.StreamFusionStateBackendFactory;
import tech.streamfusion.proto.plan.v1.WindowKind;

/** Reflection entry point used by the planner extension for native aligned Window TVFs. */
public final class StreamFusionWindowTableFunctionTranslator {
    private StreamFusionWindowTableFunctionTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> input,
            RowType inputType,
            RowType outputType,
            TimeAttributeWindowingStrategy strategy,
            ReadableConfig config,
            StreamExecutionEnvironment environment,
            RowDataKeySelector keySelector) {
        if (unsupportedReason(inputType, outputType, strategy, config) != null) {
            return null;
        }
        WindowParameters parameters = parameters(strategy.getWindow());
        int[] partitionKeys = strategy.getWindow() instanceof SessionWindowSpec
                ? ((SessionWindowSpec) strategy.getWindow()).getPartitionKeyIndices()
                : new int[0];
        String shiftTimeZone = TimeWindowUtil.getShiftTimeZone(
                        strategy.getTimeAttributeType(), TableConfigUtils.getLocalTimeZone(config))
                .getId();
        byte[] plan = StreamFusionWindowTableFunctionPlan.create(
                inputType,
                strategy.getTimeAttributeIndex(),
                partitionKeys,
                strategy.isProctime(),
                shiftTimeZone,
                parameters);
        if (strategy.getWindow() instanceof SessionWindowSpec) {
            StreamFusionStateBackendFactory.install(environment);
            Transformation<RowData> partitionedInput = input;
            if (!"StreamFusionExchangeReader".equals(input.getName())) {
                partitionedInput = partitionKeys.length == 0
                        ? StreamFusionExchangeTranslator.singleton(input, inputType)
                        : StreamFusionExchangeTranslator.hash(
                                input,
                                inputType,
                                partitionKeys,
                                DEFAULT_LOWER_BOUND_MAX_PARALLELISM,
                                environment.getParallelism(),
                                config.get(CheckpointingOptions.ENABLE_UNALIGNED)
                                        || config.get(CheckpointingOptions.FORCE_UNALIGNED));
            }
            Transformation<ArrowRowDataBatch> arrowInput =
                    StreamFusionArrowBoundaries.toArrow(partitionedInput, inputType);
            OneInputTransformation<ArrowRowDataBatch, ArrowRowDataBatch> transformation = new OneInputTransformation<>(
                    arrowInput,
                    "streamfusion-window-table-function[SESSION]",
                    new StreamFusionArrowSessionWindowTableFunctionOperator(
                            inputType, outputType, partitionKeys, plan, strategy.isProctime(), keySelector),
                    ArrowRowDataBatchTypeInfo.INSTANCE,
                    partitionedInput.getParallelism(),
                    false);
            if (partitionedInput.getMaxParallelism() > 0) {
                transformation.setMaxParallelism(partitionedInput.getMaxParallelism());
            }
            transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);
            transformation.setStateKeySelector(new ArrowBatchKeySelector(keySelector));
            transformation.setStateKeyType(keySelector.getProducedType());
            return StreamFusionArrowBoundaries.asPlannerTransformation(transformation);
        }
        Transformation<ArrowRowDataBatch> arrowInput = StreamFusionArrowBoundaries.toArrow(input, inputType);
        OneInputTransformation<ArrowRowDataBatch, ArrowRowDataBatch> transformation = new OneInputTransformation<>(
                arrowInput,
                "streamfusion-window-table-function[" + parameters.kind + "]",
                new StreamFusionArrowNativeOperator(
                        outputType,
                        plan,
                        "streamfusion-window-table-function",
                        strategy.getTimeAttributeIndex(),
                        "numNullRowTimeRecordsDropped",
                        false),
                ArrowRowDataBatchTypeInfo.INSTANCE,
                input.getParallelism(),
                false);
        transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);
        return StreamFusionArrowBoundaries.asPlannerTransformation(transformation);
    }

    public static String unsupportedReason(
            RowType inputType, RowType outputType, TimeAttributeWindowingStrategy strategy, ReadableConfig config) {
        boolean session = strategy.getWindow() instanceof SessionWindowSpec;
        if (strategy.isProctime() && !session) {
            return "time attribute: processing-time Window TVFs require per-record Flink clock parity";
        }
        if (!session && strategy.getTimeAttributeType().getTypeRoot() != LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
            return "time attribute: only TIMESTAMP rowtime is native; TIMESTAMP_LTZ requires Flink local-time-zone shifting";
        }
        int index = strategy.getTimeAttributeIndex();
        if (index < 0 || index >= inputType.getFieldCount()) {
            return "time attribute: index is outside the input row";
        }
        if (outputType.getFieldCount() != inputType.getFieldCount() + 3) {
            return "output: aligned Window TVF must append window_start, window_end, and window_time";
        }
        if (session) {
            SessionWindowSpec spec = (SessionWindowSpec) strategy.getWindow();
            for (int key : spec.getPartitionKeyIndices()) {
                if (key < 0 || key >= inputType.getFieldCount()) {
                    return "partition key: index " + key + " is outside the input row";
                }
            }
            if (config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED)) {
                return "state: Flink async-state mode is not implemented by native session Window TVF";
            }
            if (config.get(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG)) {
                return "state: Flink changelog-state wrapping is not implemented by native session Window TVF";
            }
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
        if (spec instanceof SessionWindowSpec) {
            SessionWindowSpec session = (SessionWindowSpec) spec;
            return new WindowParameters(
                    WindowKind.WINDOW_KIND_SESSION, session.getGap().toMillis(), 0, 0);
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

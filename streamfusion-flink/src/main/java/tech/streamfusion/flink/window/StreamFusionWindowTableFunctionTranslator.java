/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.window;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
import tech.streamfusion.flink.calc.StreamFusionCalcPlan;
import tech.streamfusion.flink.calc.StreamFusionCalcTranslator;
import tech.streamfusion.flink.calc.StreamFusionInputProjection;
import tech.streamfusion.flink.deduplicate.ArrowBatchKeySelector;
import tech.streamfusion.flink.exchange.StreamFusionExchangeTranslator;
import tech.streamfusion.flink.memory.StreamFusionTaskMemory;
import tech.streamfusion.flink.operator.StreamFusionArrowNativeOperator;
import tech.streamfusion.flink.state.StreamFusionStateBackendFactory;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.Operator;
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
        transformation.declareManagedMemoryUseCaseAtOperatorScope(
                ManagedMemoryUseCase.OPERATOR, StreamFusionTaskMemory.MANAGED_MEMORY_WEIGHT);
        return StreamFusionArrowBoundaries.asPlannerTransformation(transformation);
    }

    /** Fuses a bounded input Calc chain directly below an aligned Window TVF. */
    public static Transformation<RowData> translateInputCalcChain(
            Transformation<RowData> input,
            List<RowType> calcInputTypes,
            List<RowType> calcOutputTypes,
            List<List<?>> calcProjectionStages,
            List<?> calcConditions,
            RowType outputType,
            TimeAttributeWindowingStrategy strategy,
            ReadableConfig config) {
        if (calcInputTypes.isEmpty()
                || calcInputTypes.size() != calcOutputTypes.size()
                || calcInputTypes.size() != calcProjectionStages.size()
                || calcInputTypes.size() != calcConditions.size()) {
            throw new IllegalArgumentException("A fused Window TVF input Calc chain must be non-empty and aligned");
        }
        RowType windowInputType = calcOutputTypes.get(calcOutputTypes.size() - 1);
        if (strategy.getWindow() instanceof SessionWindowSpec
                || unsupportedReason(windowInputType, outputType, strategy, config) != null) {
            return null;
        }
        List<List<Expression>> nativeProjections = new ArrayList<>(calcProjectionStages.size());
        List<Expression> nativeConditions = new ArrayList<>(calcConditions.size());
        for (int stage = 0; stage < calcInputTypes.size(); stage++) {
            RowType stageInput = calcInputTypes.get(stage);
            RowType stageOutput = calcOutputTypes.get(stage);
            List<?> projections = calcProjectionStages.get(stage);
            List<Expression> expressions = new ArrayList<>(projections.size());
            for (int index = 0; index < projections.size(); index++) {
                expressions.add(StreamFusionCalcTranslator.operatorExpression(
                        projections.get(index), stageInput, stageOutput.getTypeAt(index)));
            }
            nativeProjections.add(expressions);
            nativeConditions.add(StreamFusionCalcTranslator.operatorCondition(calcConditions.get(stage), stageInput));
        }

        RowType planInputType = calcInputTypes.get(0);
        Transformation<ArrowRowDataBatch> arrowInput;
        if (StreamFusionArrowBoundaries.isArrow(input)) {
            arrowInput = StreamFusionArrowBoundaries.toArrow(input, planInputType);
        } else {
            StreamFusionInputProjection.Projection projection = StreamFusionInputProjection.create(
                    planInputType, nativeProjections.get(0), nativeConditions.get(0));
            planInputType = projection.inputType();
            nativeProjections.set(0, projection.projections());
            nativeConditions.set(0, projection.condition());
            arrowInput = StreamFusionArrowBoundaries.toArrow(
                    input, planInputType, projection.fieldPaths(), projection.rowArities());
        }

        Operator calcs = StreamFusionCalcPlan.appendCalcOperators(
                Operator.newBuilder().setInput(Input.newBuilder()).build(),
                planInputType.getFieldCount(),
                nativeProjections,
                nativeConditions);
        WindowParameters parameters = parameters(strategy.getWindow());
        String shiftTimeZone = TimeWindowUtil.getShiftTimeZone(
                        strategy.getTimeAttributeType(), TableConfigUtils.getLocalTimeZone(config))
                .getId();
        byte[] plan = StreamFusionWindowTableFunctionPlan.create(
                calcs, windowInputType, strategy.getTimeAttributeIndex(), new int[0], false, shiftTimeZone, parameters);
        OneInputTransformation<ArrowRowDataBatch, ArrowRowDataBatch> transformation = new OneInputTransformation<>(
                arrowInput,
                "streamfusion-calc-window-table-function[" + parameters.kind + "]",
                new StreamFusionArrowNativeOperator(
                        outputType,
                        plan,
                        "streamfusion-window-table-function",
                        -1,
                        "numNullRowTimeRecordsDropped",
                        false),
                ArrowRowDataBatchTypeInfo.INSTANCE,
                input.getParallelism(),
                false);
        transformation.declareManagedMemoryUseCaseAtOperatorScope(
                ManagedMemoryUseCase.OPERATOR, StreamFusionTaskMemory.MANAGED_MEMORY_WEIGHT);
        return StreamFusionArrowBoundaries.asPlannerTransformation(transformation);
    }

    public static String unsupportedReason(
            RowType inputType, RowType outputType, TimeAttributeWindowingStrategy strategy, ReadableConfig config) {
        boolean session = strategy.getWindow() instanceof SessionWindowSpec;
        if (strategy.isProctime() && !session) {
            return "time attribute: processing-time Window TVFs require per-record Flink clock parity";
        }
        if (!session
                && strategy.getTimeAttributeType().getTypeRoot() != LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                && strategy.getTimeAttributeType().getTypeRoot() != LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            return "time attribute: aligned Window TVFs require TIMESTAMP or TIMESTAMP_LTZ";
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

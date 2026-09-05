/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.window;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.runtime.groupwindow.ProctimeAttribute;
import org.apache.flink.table.runtime.groupwindow.RowtimeAttribute;
import org.apache.flink.table.runtime.groupwindow.WindowEnd;
import org.apache.flink.table.runtime.groupwindow.WindowStart;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.aggregate.StreamFusionGroupAggregateTranslator;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;
import tech.streamfusion.flink.deduplicate.ArrowBatchKeySelector;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.exchange.NativeExchangeFrameKeySelector;
import tech.streamfusion.flink.exchange.NativeExchangeFrameTypeInfo;
import tech.streamfusion.flink.exchange.NativeExchangeReaderOperator;
import tech.streamfusion.flink.exchange.StreamFusionExchangeTranslator;
import tech.streamfusion.flink.state.StreamFusionStateBackendFactory;

/** Reflection entry point for native TUMBLE, HOP, and CUMULATE SQL window aggregation. */
public final class StreamFusionWindowAggregateTranslator {
    private static final int STATEFUL_MANAGED_MEMORY_WEIGHT = 8;
    private static final int BATCH_MANAGED_MEMORY_WEIGHT = 128;

    private StreamFusionWindowAggregateTranslator() {}

    public static Transformation<RowData> translateLocal(
            Transformation<RowData> input,
            RowType inputType,
            RowType internalOutputType,
            int[] grouping,
            AggregateCall[] calls,
            WindowingStrategy strategy,
            boolean needRetraction,
            ReadableConfig config) {
        return translateLocal(
                input,
                inputType,
                internalOutputType,
                grouping,
                calls,
                strategy,
                needRetraction,
                config,
                STATEFUL_MANAGED_MEMORY_WEIGHT / 2);
    }

    static Transformation<RowData> translateBatchLocal(
            Transformation<RowData> input,
            RowType inputType,
            RowType internalOutputType,
            int[] grouping,
            AggregateCall[] calls,
            WindowingStrategy strategy,
            ReadableConfig config) {
        return translateLocal(
                input,
                inputType,
                internalOutputType,
                grouping,
                calls,
                strategy,
                false,
                config,
                BATCH_MANAGED_MEMORY_WEIGHT);
    }

    private static Transformation<RowData> translateLocal(
            Transformation<RowData> input,
            RowType inputType,
            RowType internalOutputType,
            int[] grouping,
            AggregateCall[] calls,
            WindowingStrategy strategy,
            boolean needRetraction,
            ReadableConfig config,
            int managedMemoryWeight) {
        if (strategy.isProctime()) {
            return null;
        }
        int timeAttributeIndex = 0;
        int attachedWindowStartIndex = -1;
        int attachedWindowEndIndex = -1;
        if (strategy instanceof TimeAttributeWindowingStrategy) {
            timeAttributeIndex = ((TimeAttributeWindowingStrategy) strategy).getTimeAttributeIndex();
        } else if (strategy instanceof WindowAttachedWindowingStrategy) {
            WindowAttachedWindowingStrategy attached = (WindowAttachedWindowingStrategy) strategy;
            attachedWindowStartIndex = attached.getWindowStart();
            attachedWindowEndIndex = attached.getWindowEnd();
        } else {
            return null;
        }
        String shiftTimeZone = TimeWindowUtil.getShiftTimeZone(
                        strategy.getTimeAttributeType(), TableConfigUtils.getLocalTimeZone(config))
                .getId();
        byte[] plan = StreamFusionWindowAggregatePlan.createLocal(
                inputType,
                internalOutputType,
                grouping,
                calls,
                needRetraction,
                needRetraction,
                StreamFusionWindowTableFunctionTranslator.parameters(strategy.getWindow()),
                timeAttributeIndex,
                attachedWindowStartIndex,
                attachedWindowEndIndex,
                shiftTimeZone);
        Transformation<ArrowRowDataBatch> arrowInput = StreamFusionArrowBoundaries.toArrow(input, inputType);
        OneInputTransformation<ArrowRowDataBatch, ArrowRowDataBatch> transformation = new OneInputTransformation<>(
                arrowInput,
                "streamfusion-local-window-aggregate["
                        + strategy.getWindow().getClass().getSimpleName() + "]",
                new StreamFusionArrowLocalWindowAggregateOperator(plan, internalOutputType, needRetraction),
                ArrowRowDataBatchTypeInfo.INSTANCE,
                input.getParallelism(),
                false);
        transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, managedMemoryWeight);
        return StreamFusionArrowBoundaries.asPlannerTransformation(transformation);
    }

    public static Transformation<RowData> translateGlobal(
            Transformation<RowData> input,
            RowType originalInputType,
            RowType internalInputType,
            RowType outputType,
            int groupingCount,
            AggregateCall[] calls,
            WindowingStrategy strategy,
            NamedWindowProperty[] properties,
            boolean needRetraction,
            ReadableConfig config,
            StreamExecutionEnvironment environment,
            RowDataKeySelector keySelector) {
        return translateGlobal(
                input,
                originalInputType,
                internalInputType,
                outputType,
                groupingCount,
                calls,
                strategy,
                properties,
                needRetraction,
                config,
                environment,
                keySelector,
                STATEFUL_MANAGED_MEMORY_WEIGHT);
    }

    static Transformation<RowData> translateBatchGlobal(
            Transformation<RowData> input,
            RowType originalInputType,
            RowType internalInputType,
            RowType outputType,
            int groupingCount,
            AggregateCall[] calls,
            WindowingStrategy strategy,
            NamedWindowProperty[] properties,
            ReadableConfig config,
            StreamExecutionEnvironment environment,
            RowDataKeySelector keySelector) {
        StreamFusionStateBackendFactory.install(environment);
        String shiftTimeZone = TimeWindowUtil.getShiftTimeZone(
                        strategy.getTimeAttributeType(), TableConfigUtils.getLocalTimeZone(config))
                .getId();
        byte[] aggregatePlan = StreamFusionWindowAggregatePlan.createGlobal(
                originalInputType,
                internalInputType,
                outputType,
                groupingCount,
                calls,
                false,
                StreamFusionWindowTableFunctionTranslator.parameters(strategy.getWindow()),
                strategy instanceof TimeAttributeWindowingStrategy,
                shiftTimeZone,
                properties);
        int[] grouping = java.util.stream.IntStream.range(0, groupingCount).toArray();
        FramedInput framed = framed(input);
        OneInputTransformation<NativeExchangeFrame, ArrowRowDataBatch> transformation = new OneInputTransformation<>(
                framed.transformation,
                "streamfusion-batch-global-window-aggregate["
                        + strategy.getWindow().getClass().getSimpleName() + "]",
                new StreamFusionArrowFramedWindowAggregateOperator(
                        internalInputType, outputType, grouping, aggregatePlan, keySelector, framed.plan),
                ArrowRowDataBatchTypeInfo.INSTANCE,
                input.getParallelism(),
                false);
        transformation.setMaxParallelism(DEFAULT_LOWER_BOUND_MAX_PARALLELISM);
        transformation.declareManagedMemoryUseCaseAtOperatorScope(
                ManagedMemoryUseCase.OPERATOR, BATCH_MANAGED_MEMORY_WEIGHT);
        transformation.setStateKeySelector(new NativeExchangeFrameKeySelector(DEFAULT_LOWER_BOUND_MAX_PARALLELISM));
        transformation.setStateKeyType(Types.INT);
        return StreamFusionArrowBoundaries.asPlannerTransformation(transformation);
    }

    @SuppressWarnings("unchecked")
    private static FramedInput framed(Transformation<RowData> input) {
        if (!(input instanceof OneInputTransformation) || !"StreamFusionExchangeReader".equals(input.getName())) {
            throw new IllegalStateException("Native bounded window aggregate requires a framed exchange");
        }
        OneInputTransformation<?, ?> reader = (OneInputTransformation<?, ?>) input;
        if (!(reader.getOperatorFactory() instanceof SimpleOperatorFactory)) {
            throw new IllegalStateException(
                    "Native bounded window aggregate cannot inspect its exchange reader factory");
        }
        Object operator = ((SimpleOperatorFactory<?>) reader.getOperatorFactory()).getOperator();
        if (!(operator instanceof NativeExchangeReaderOperator)) {
            throw new IllegalStateException("Native bounded window aggregate received an incompatible exchange reader");
        }
        Transformation<?> frames = reader.getInputs().get(0);
        if (!(frames.getOutputType() instanceof NativeExchangeFrameTypeInfo)) {
            throw new IllegalStateException("Native bounded window aggregate exchange input is not frame encoded");
        }
        return new FramedInput(
                (Transformation<NativeExchangeFrame>) frames,
                ((NativeExchangeReaderOperator) operator).serializedPlan());
    }

    private static final class FramedInput {
        private final Transformation<NativeExchangeFrame> transformation;
        private final byte[] plan;

        private FramedInput(Transformation<NativeExchangeFrame> transformation, byte[] plan) {
            this.transformation = transformation;
            this.plan = plan;
        }
    }

    private static Transformation<RowData> translateGlobal(
            Transformation<RowData> input,
            RowType originalInputType,
            RowType internalInputType,
            RowType outputType,
            int groupingCount,
            AggregateCall[] calls,
            WindowingStrategy strategy,
            NamedWindowProperty[] properties,
            boolean needRetraction,
            ReadableConfig config,
            StreamExecutionEnvironment environment,
            RowDataKeySelector keySelector,
            int managedMemoryWeight) {
        StreamFusionStateBackendFactory.install(environment);
        String shiftTimeZone = TimeWindowUtil.getShiftTimeZone(
                        strategy.getTimeAttributeType(), TableConfigUtils.getLocalTimeZone(config))
                .getId();
        byte[] plan = StreamFusionWindowAggregatePlan.createGlobal(
                originalInputType,
                internalInputType,
                outputType,
                groupingCount,
                calls,
                needRetraction,
                StreamFusionWindowTableFunctionTranslator.parameters(strategy.getWindow()),
                strategy instanceof TimeAttributeWindowingStrategy,
                shiftTimeZone,
                properties);
        int[] grouping = java.util.stream.IntStream.range(0, groupingCount).toArray();
        Transformation<ArrowRowDataBatch> arrowInput = StreamFusionArrowBoundaries.toArrow(input, internalInputType);
        OneInputTransformation<ArrowRowDataBatch, ArrowRowDataBatch> transformation = new OneInputTransformation<>(
                arrowInput,
                "streamfusion-global-window-aggregate["
                        + strategy.getWindow().getClass().getSimpleName() + "]",
                new StreamFusionArrowWindowAggregateOperator(
                        internalInputType, outputType, grouping, plan, false, false, keySelector),
                ArrowRowDataBatchTypeInfo.INSTANCE,
                input.getParallelism(),
                false);
        if (input.getMaxParallelism() > 0) {
            transformation.setMaxParallelism(input.getMaxParallelism());
        }
        transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, managedMemoryWeight);
        transformation.setStateKeySelector(new ArrowBatchKeySelector(keySelector));
        transformation.setStateKeyType(keySelector.getProducedType());
        return StreamFusionArrowBoundaries.asPlannerTransformation(transformation);
    }

    public static Transformation<RowData> translate(
            Transformation<RowData> input,
            RowType inputType,
            RowType outputType,
            int[] grouping,
            AggregateCall[] calls,
            WindowingStrategy strategy,
            NamedWindowProperty[] properties,
            boolean needRetraction,
            ReadableConfig config,
            StreamExecutionEnvironment environment,
            RowDataKeySelector keySelector) {
        String reason =
                unsupportedReason(inputType, outputType, grouping, calls, strategy, properties, needRetraction, config);
        if (reason != null) {
            return null;
        }
        int timeAttributeIndex = 0;
        int attachedWindowStartIndex = -1;
        int attachedWindowEndIndex = -1;
        if (strategy instanceof TimeAttributeWindowingStrategy) {
            timeAttributeIndex = ((TimeAttributeWindowingStrategy) strategy).getTimeAttributeIndex();
        } else {
            WindowAttachedWindowingStrategy attached = (WindowAttachedWindowingStrategy) strategy;
            attachedWindowStartIndex = attached.getWindowStart();
            attachedWindowEndIndex = attached.getWindowEnd();
        }
        StreamFusionStateBackendFactory.install(environment);
        byte[] plan = StreamFusionWindowAggregatePlan.create(
                inputType,
                outputType,
                grouping,
                calls,
                needRetraction,
                needRetraction,
                StreamFusionWindowTableFunctionTranslator.parameters(strategy.getWindow()),
                timeAttributeIndex,
                attachedWindowStartIndex,
                attachedWindowEndIndex,
                strategy.isProctime(),
                TimeWindowUtil.getShiftTimeZone(
                                strategy.getTimeAttributeType(), TableConfigUtils.getLocalTimeZone(config))
                        .getId(),
                properties);
        Transformation<RowData> partitionedInput = input;
        if (!"StreamFusionExchangeReader".equals(input.getName())) {
            if (grouping.length == 0) {
                partitionedInput = StreamFusionExchangeTranslator.singleton(input, inputType);
            } else {
                partitionedInput = StreamFusionExchangeTranslator.hash(
                        input,
                        inputType,
                        grouping,
                        DEFAULT_LOWER_BOUND_MAX_PARALLELISM,
                        environment.getParallelism(),
                        config.get(CheckpointingOptions.ENABLE_UNALIGNED)
                                || config.get(CheckpointingOptions.FORCE_UNALIGNED));
            }
        }
        Transformation<ArrowRowDataBatch> arrowInput = StreamFusionArrowBoundaries.toArrow(partitionedInput, inputType);
        OneInputTransformation<ArrowRowDataBatch, ArrowRowDataBatch> transformation = new OneInputTransformation<>(
                arrowInput,
                "streamfusion-window-aggregate["
                        + strategy.getWindow().getClass().getSimpleName() + "]",
                new StreamFusionArrowWindowAggregateOperator(
                        inputType, outputType, grouping, plan, needRetraction, strategy.isProctime(), keySelector),
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
            WindowingStrategy strategy,
            NamedWindowProperty[] properties,
            boolean needRetraction,
            ReadableConfig config) {
        if (!(strategy instanceof TimeAttributeWindowingStrategy)
                && !(strategy instanceof WindowAttachedWindowingStrategy)) {
            return "window strategy: only direct or attached time windows are native";
        }
        if (strategy instanceof WindowAttachedWindowingStrategy) {
            WindowAttachedWindowingStrategy attached = (WindowAttachedWindowingStrategy) strategy;
            if (attached.getWindowStart() < 0) {
                return "window strategy: attached aggregation requires both window-start and window-end columns";
            }
            if (attached.getWindowStart() >= inputType.getFieldCount()
                    || attached.getWindowEnd() >= inputType.getFieldCount()) {
                return "window strategy: attached window columns are outside the input row";
            }
            if (inputType.getTypeAt(attached.getWindowStart()).getTypeRoot()
                            != org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                    || inputType.getTypeAt(attached.getWindowEnd()).getTypeRoot()
                            != org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
                return "window strategy: attached window columns must both be TIMESTAMP";
            }
        }
        try {
            StreamFusionWindowTableFunctionTranslator.parameters(strategy.getWindow());
        } catch (IllegalArgumentException unsupported) {
            return "window: " + unsupported.getMessage();
        }
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED)) {
            return "state: Flink async-state mode is not implemented by native window aggregation";
        }
        if (config.get(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG)) {
            return "state: Flink changelog-state wrapping is not implemented by native window aggregation";
        }
        int expectedFields = grouping.length + calls.length + properties.length;
        if (outputType.getFieldCount() != expectedFields) {
            return "schema: window output must contain keys, aggregates, then named properties";
        }
        for (int index = 0; index < grouping.length; index++) {
            int inputIndex = grouping[index];
            if (inputIndex < 0 || inputIndex >= inputType.getFieldCount()) {
                return "key: index " + inputIndex + " is outside the input row";
            }
            if (!inputType.getTypeAt(inputIndex).equals(outputType.getTypeAt(index))) {
                return "key[" + index + "]: input and output types must match exactly";
            }
        }
        for (int index = 0; index < calls.length; index++) {
            LogicalType output = outputType.getTypeAt(grouping.length + index);
            if (calls[index].isDistinct()) {
                return "aggregate[" + index + "]: DISTINCT window aggregation is not implemented";
            }
            String reason = StreamFusionGroupAggregateTranslator.unsupportedCall(inputType, output, calls[index]);
            if (reason != null) {
                return "aggregate[" + index + "]: " + reason;
            }
            if (!FlinkTypeFactory.toLogicalType(calls[index].getType()).equals(output)) {
                return "aggregate[" + index + "]: planned output type mismatch";
            }
        }
        for (int index = 0; index < properties.length; index++) {
            Object property = properties[index].getProperty();
            if (!(property instanceof WindowStart)
                    && !(property instanceof WindowEnd)
                    && !(property instanceof RowtimeAttribute)
                    && !(property instanceof ProctimeAttribute)) {
                return "window property[" + index + "]: " + property.getClass().getSimpleName() + " is not native";
            }
        }
        return null;
    }
}

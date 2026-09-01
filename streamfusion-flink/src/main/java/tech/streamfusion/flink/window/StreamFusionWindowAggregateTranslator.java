/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.window;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
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
import tech.streamfusion.flink.exchange.StreamFusionExchangeTranslator;
import tech.streamfusion.flink.state.StreamFusionStateBackendFactory;

/** Reflection entry point for native TUMBLE, HOP, and CUMULATE SQL window aggregation. */
public final class StreamFusionWindowAggregateTranslator {
    private StreamFusionWindowAggregateTranslator() {}

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
        TimeAttributeWindowingStrategy timeStrategy = (TimeAttributeWindowingStrategy) strategy;
        StreamFusionStateBackendFactory.install(environment);
        byte[] plan = StreamFusionWindowAggregatePlan.create(
                inputType,
                outputType,
                grouping,
                calls,
                needRetraction,
                needRetraction,
                StreamFusionWindowTableFunctionTranslator.parameters(strategy.getWindow()),
                timeStrategy.getTimeAttributeIndex(),
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
        transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);
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
        if (!(strategy instanceof TimeAttributeWindowingStrategy)) {
            return "window strategy: only direct time-attribute window aggregation is native";
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

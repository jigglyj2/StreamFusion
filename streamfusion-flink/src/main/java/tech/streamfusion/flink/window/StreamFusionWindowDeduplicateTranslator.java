/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.window;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;
import tech.streamfusion.flink.deduplicate.ArrowBatchKeySelector;
import tech.streamfusion.flink.exchange.StreamFusionExchangeTranslator;
import tech.streamfusion.flink.state.StreamFusionStateBackendFactory;

/** Reflection entry point for native window first/last-row deduplication. */
public final class StreamFusionWindowDeduplicateTranslator {
    private StreamFusionWindowDeduplicateTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> input,
            RowType inputType,
            RowType outputType,
            int[] partitionKeys,
            int orderKey,
            boolean keepLast,
            WindowingStrategy windowing,
            ReadableConfig config,
            StreamExecutionEnvironment environment,
            RowDataKeySelector keySelector) {
        String reason = unsupportedReason(inputType, outputType, partitionKeys, orderKey, windowing, config);
        if (reason != null) {
            return null;
        }
        WindowAttachedWindowingStrategy attached = (WindowAttachedWindowingStrategy) windowing;
        String shiftTimeZone = TimeWindowUtil.getShiftTimeZone(
                        windowing.getTimeAttributeType(), TableConfigUtils.getLocalTimeZone(config))
                .getId();
        byte[] plan = StreamFusionWindowDeduplicatePlan.create(
                inputType, partitionKeys, orderKey, attached.getWindowEnd(), keepLast, shiftTimeZone);
        StreamFusionStateBackendFactory.install(environment);
        Transformation<RowData> partitionedInput = input;
        if (!"StreamFusionExchangeReader".equals(input.getName())) {
            if (partitionKeys.length == 0) {
                partitionedInput = StreamFusionExchangeTranslator.singleton(input, inputType);
            } else {
                partitionedInput = StreamFusionExchangeTranslator.hash(
                        input,
                        inputType,
                        partitionKeys,
                        DEFAULT_LOWER_BOUND_MAX_PARALLELISM,
                        environment.getParallelism(),
                        config.get(CheckpointingOptions.ENABLE_UNALIGNED)
                                || config.get(CheckpointingOptions.FORCE_UNALIGNED));
            }
        }
        Transformation<ArrowRowDataBatch> arrowInput = StreamFusionArrowBoundaries.toArrow(partitionedInput, inputType);
        OneInputTransformation<ArrowRowDataBatch, ArrowRowDataBatch> transformation = new OneInputTransformation<>(
                arrowInput,
                "streamfusion-window-deduplicate",
                new StreamFusionArrowWindowDeduplicateOperator(inputType, partitionKeys, plan, keySelector),
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
            int[] partitionKeys,
            int orderKey,
            WindowingStrategy windowing,
            ReadableConfig config) {
        if (!(windowing instanceof WindowAttachedWindowingStrategy)) {
            return "window strategy: window deduplication requires attached window columns";
        }
        if (!windowing.isRowtime()) {
            return "window time: Flink does not plan processing-time Window Deduplicate";
        }
        if (!inputType.equals(outputType)) {
            return "schema: window deduplicate input and output rows must match";
        }
        if (orderKey < 0 || orderKey >= inputType.getFieldCount()) {
            return "order key: index is outside the input row";
        }
        int windowEnd = ((WindowAttachedWindowingStrategy) windowing).getWindowEnd();
        if (windowEnd < 0 || windowEnd >= inputType.getFieldCount()) {
            return "window end: index is outside the input row";
        }
        for (int key : partitionKeys) {
            if (key < 0 || key >= inputType.getFieldCount()) {
                return "partition key: index " + key + " is outside the input row";
            }
        }
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED)) {
            return "state: Flink async-state mode is not implemented by native window deduplication";
        }
        if (config.get(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG)) {
            return "state: Flink changelog-state wrapping is not implemented by native window deduplication";
        }
        return null;
    }
}

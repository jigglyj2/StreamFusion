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
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;
import tech.streamfusion.flink.deduplicate.ArrowBatchKeySelector;
import tech.streamfusion.flink.exchange.StreamFusionExchangeTranslator;
import tech.streamfusion.flink.state.StreamFusionStateBackendFactory;

/** Reflection entry point for native event-time Window Top-N. */
public final class StreamFusionWindowRankTranslator {
    private StreamFusionWindowRankTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> input,
            RowType inputType,
            RowType outputType,
            int[] partitionKeys,
            SortSpec sortSpec,
            long rankStart,
            long rankEnd,
            boolean outputRankNumber,
            WindowingStrategy windowing,
            ReadableConfig config,
            StreamExecutionEnvironment environment,
            RowDataKeySelector partitionSelector,
            RowDataKeySelector sortSelector,
            GeneratedRecordComparator comparator) {
        String reason = unsupportedReason(
                inputType,
                outputType,
                partitionKeys,
                sortSpec,
                rankStart,
                rankEnd,
                outputRankNumber,
                windowing,
                config);
        if (reason != null) {
            return null;
        }
        WindowAttachedWindowingStrategy attached = (WindowAttachedWindowingStrategy) windowing;
        String shiftTimeZone = TimeWindowUtil.getShiftTimeZone(
                        windowing.getTimeAttributeType(), TableConfigUtils.getLocalTimeZone(config))
                .getId();
        byte[] plan = StreamFusionWindowRankPlan.create(
                inputType,
                partitionKeys,
                sortSpec,
                attached.getWindowEnd(),
                rankStart,
                rankEnd,
                outputRankNumber,
                shiftTimeZone);
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
                "streamfusion-window-rank",
                new StreamFusionArrowWindowRankOperator(
                        inputType,
                        outputType,
                        partitionKeys,
                        sortSpec.getFieldSize(),
                        rankStart,
                        rankEnd,
                        outputRankNumber,
                        plan,
                        partitionSelector,
                        sortSelector,
                        comparator),
                ArrowRowDataBatchTypeInfo.INSTANCE,
                partitionedInput.getParallelism(),
                false);
        if (partitionedInput.getMaxParallelism() > 0) {
            transformation.setMaxParallelism(partitionedInput.getMaxParallelism());
        }
        transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);
        transformation.setStateKeySelector(new ArrowBatchKeySelector(partitionSelector));
        transformation.setStateKeyType(partitionSelector.getProducedType());
        return StreamFusionArrowBoundaries.asPlannerTransformation(transformation);
    }

    public static String unsupportedReason(
            RowType inputType,
            RowType outputType,
            int[] partitionKeys,
            SortSpec sortSpec,
            long rankStart,
            long rankEnd,
            boolean outputRankNumber,
            WindowingStrategy windowing,
            ReadableConfig config) {
        if (!(windowing instanceof WindowAttachedWindowingStrategy)) {
            return "window strategy: Window Top-N requires attached window columns";
        }
        if (!windowing.isRowtime()) {
            return "window time: Flink does not plan processing-time Window Top-N";
        }
        if (rankStart <= 0 || rankEnd < rankStart) {
            return "rank range: Window Top-N requires a positive constant range";
        }
        if (sortSpec.getFieldSize() == 0) {
            return "sort: Window Top-N requires at least one ordering field";
        }
        int expected = inputType.getFieldCount() + (outputRankNumber ? 1 : 0);
        if (outputType.getFieldCount() != expected) {
            return "schema: Window Top-N output does not match input plus optional rank number";
        }
        for (int key : partitionKeys) {
            if (key < 0 || key >= inputType.getFieldCount()) {
                return "partition key: index " + key + " is outside the input row";
            }
        }
        for (int key : sortSpec.getFieldIndices()) {
            if (key < 0 || key >= inputType.getFieldCount()) {
                return "sort key: index " + key + " is outside the input row";
            }
        }
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED)) {
            return "state: Flink async-state mode is not implemented by native Window Top-N";
        }
        if (config.get(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG)) {
            return "state: Flink changelog-state wrapping is not implemented by native Window Top-N";
        }
        return null;
    }
}

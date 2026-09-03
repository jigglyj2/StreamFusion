/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.topn;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RANK_TOPN_CACHE_SIZE;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;
import tech.streamfusion.flink.deduplicate.ArrowBatchKeySelector;
import tech.streamfusion.flink.exchange.StreamFusionExchangeTranslator;
import tech.streamfusion.flink.state.StreamFusionStateBackendFactory;

/** Reflection entry point for native streaming non-window Top-N/ROW_NUMBER. */
public final class StreamFusionTopNTranslator {
    private StreamFusionTopNTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> input,
            RowType inputType,
            RowType outputType,
            int[] partitionKeys,
            SortSpec sortSpec,
            int[] primaryKeys,
            long rankStart,
            Long rankEnd,
            Integer variableRankEndIndex,
            boolean outputRankNumber,
            boolean generateUpdateBefore,
            String strategyName,
            long stateTtlMillis,
            ReadableConfig config,
            StreamExecutionEnvironment environment,
            RowDataKeySelector partitionSelector) {
        String reason = unsupportedReason(
                inputType,
                outputType,
                partitionKeys,
                sortSpec,
                primaryKeys,
                rankStart,
                rankEnd,
                variableRankEndIndex,
                outputRankNumber,
                strategyName,
                stateTtlMillis,
                config);
        if (reason != null) {
            return null;
        }
        StreamFusionTopNStrategy strategy = StreamFusionTopNStrategy.valueOf(strategyName);
        byte[] plan = StreamFusionTopNPlan.create(
                inputType,
                outputType,
                partitionKeys,
                sortSpec,
                primaryKeys,
                rankStart,
                rankEnd,
                variableRankEndIndex,
                outputRankNumber,
                generateUpdateBefore,
                strategy,
                stateTtlMillis);
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
                "streamfusion-top-n",
                new StreamFusionArrowTopNOperator(
                        inputType,
                        outputType,
                        partitionKeys,
                        plan,
                        partitionSelector,
                        strategy,
                        config.get(TABLE_EXEC_RANK_TOPN_CACHE_SIZE)),
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
            int[] primaryKeys,
            long rankStart,
            Long rankEnd,
            Integer variableRankEndIndex,
            boolean outputRankNumber,
            String strategyName,
            long stateTtlMillis,
            ReadableConfig config) {
        if (rankStart <= 0) {
            return "rank range: Top-N requires a positive one-based rank start";
        }
        if ((rankEnd == null) == (variableRankEndIndex == null)) {
            return "rank range: Top-N requires exactly one constant or variable rank end";
        }
        if (rankEnd != null && rankEnd < rankStart) {
            return "rank range: Top-N rank end precedes rank start";
        }
        if (variableRankEndIndex != null) {
            if (variableRankEndIndex < 0 || variableRankEndIndex >= inputType.getFieldCount()) {
                return "rank range: variable rank end index is outside the input row";
            }
            LogicalTypeRoot root = inputType.getTypeAt(variableRankEndIndex).getTypeRoot();
            if (root != LogicalTypeRoot.BIGINT && root != LogicalTypeRoot.INTEGER && root != LogicalTypeRoot.SMALLINT) {
                return "rank range: variable rank end must be BIGINT, INTEGER, or SMALLINT";
            }
        }
        if (sortSpec.getFieldSize() == 0
                && (partitionKeys.length != 0 || outputRankNumber || variableRankEndIndex != null || rankEnd == null)) {
            return "sort: unordered rank is supported only for Flink's global constant LIMIT/OFFSET shape";
        }
        int expectedFields = inputType.getFieldCount() + (outputRankNumber ? 1 : 0);
        if (outputType.getFieldCount() != expectedFields) {
            return "schema: Top-N output does not match input plus optional rank number";
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
        for (int key : primaryKeys) {
            if (key < 0 || key >= inputType.getFieldCount()) {
                return "primary key: index " + key + " is outside the input row";
            }
        }
        if (strategyName == null) {
            return "rank strategy: unsupported Flink RankProcessStrategy";
        }
        try {
            StreamFusionTopNStrategy.valueOf(strategyName);
        } catch (IllegalArgumentException failure) {
            return "rank strategy: unsupported Flink RankProcessStrategy " + strategyName;
        }
        if (stateTtlMillis < 0) {
            return "state: Top-N TTL may not be negative";
        }
        return null;
    }
}

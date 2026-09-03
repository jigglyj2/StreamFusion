/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.over;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isProctimeAttribute;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isRowtimeAttribute;

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
import org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.aggregate.StreamFusionGroupAggregateTranslator;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;
import tech.streamfusion.flink.deduplicate.ArrowBatchKeySelector;
import tech.streamfusion.flink.exchange.StreamFusionExchangeTranslator;
import tech.streamfusion.flink.state.StreamFusionStateBackendFactory;

/** Reflection entry point for native non-time unbounded streaming OVER aggregation. */
public final class StreamFusionOverAggregateTranslator {
    private StreamFusionOverAggregateTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> input,
            RowType inputType,
            RowType outputType,
            OverSpec overSpec,
            long stateTtl,
            ReadableConfig config,
            StreamExecutionEnvironment environment,
            RowDataKeySelector keySelector) {
        if (unsupportedReason(inputType, outputType, overSpec, stateTtl, config) != null) {
            return null;
        }
        int[] partitionKeys = overSpec.getPartition().getFieldIndices();
        StreamFusionStateBackendFactory.install(environment);
        Transformation<RowData> partitioned = input;
        if (!"StreamFusionExchangeReader".equals(input.getName())) {
            partitioned = partitionKeys.length == 0
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
        byte[] plan = StreamFusionOverAggregatePlan.create(inputType, outputType, overSpec, stateTtl);
        Transformation<ArrowRowDataBatch> arrowInput = StreamFusionArrowBoundaries.toArrow(partitioned, inputType);
        OneInputTransformation<ArrowRowDataBatch, ArrowRowDataBatch> result = new OneInputTransformation<>(
                arrowInput,
                "streamfusion-over-aggregate",
                new StreamFusionArrowOverAggregateOperator(
                        inputType, outputType, partitionKeys, plan, true, keySelector),
                ArrowRowDataBatchTypeInfo.INSTANCE,
                partitioned.getParallelism(),
                false);
        if (partitioned.getMaxParallelism() > 0) {
            result.setMaxParallelism(partitioned.getMaxParallelism());
        }
        result.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);
        result.setStateKeySelector(new ArrowBatchKeySelector(keySelector));
        result.setStateKeyType(keySelector.getProducedType());
        return StreamFusionArrowBoundaries.asPlannerTransformation(result);
    }

    public static String unsupportedReason(
            RowType inputType, RowType outputType, OverSpec overSpec, long stateTtl, ReadableConfig config) {
        if (overSpec.getGroups().size() != 1) {
            return "window groups: native OVER requires one Flink-compatible group";
        }
        OverSpec.GroupSpec group = overSpec.getGroups().get(0);
        int[] orderKeys = group.getSort().getFieldIndices();
        if (orderKeys.length != 1 || !group.getSort().getAscendingOrders()[0]) {
            return "ordering: native OVER currently requires one ascending order key";
        }
        LogicalType orderType = inputType.getTypeAt(orderKeys[0]);
        if (isRowtimeAttribute(orderType) || isProctimeAttribute(orderType)) {
            return "time frame: native row-time and processing-time OVER support is being implemented";
        }
        if (!group.getLowerBound().isPreceding()
                || !group.getLowerBound().isUnbounded()
                || !group.getUpperBound().isCurrentRow()) {
            return "frame: native non-time OVER currently requires UNBOUNDED PRECEDING to CURRENT ROW";
        }
        if (stateTtl != 0) {
            return "state: native OVER TTL is not implemented yet";
        }
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)) {
            return "mini-batch: native OVER bundle semantics are not implemented";
        }
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED)) {
            return "state: native OVER does not implement Flink async-state mode";
        }
        if (config.get(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG)) {
            return "state: native OVER does not implement Flink changelog-state wrapping";
        }
        if (outputType.getFieldCount()
                != inputType.getFieldCount() + group.getAggCalls().size()) {
            return "schema: OVER output must append one field per aggregate call";
        }
        for (int index = 0; index < group.getAggCalls().size(); index++) {
            AggregateCall call = group.getAggCalls().get(index);
            String reason = StreamFusionGroupAggregateTranslator.unsupportedCall(
                    inputType, outputType.getTypeAt(inputType.getFieldCount() + index), call);
            if (reason != null) {
                return "aggregate[" + index + "]: " + reason;
            }
        }
        return null;
    }
}

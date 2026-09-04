/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.aggregate;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
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
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;
import tech.streamfusion.flink.deduplicate.ArrowBatchKeySelector;
import tech.streamfusion.flink.exchange.StreamFusionExchangeTranslator;
import tech.streamfusion.flink.state.StreamFusionStateBackendFactory;

/** Reflection entry point for timer-free unbounded keyed group aggregation. */
public final class StreamFusionGroupAggregateTranslator {
    private StreamFusionGroupAggregateTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> input,
            RowType inputType,
            RowType outputType,
            int[] grouping,
            AggregateCall[] calls,
            boolean[] retractable,
            boolean generateUpdateBefore,
            boolean needRetraction,
            long stateRetentionTime,
            ReadableConfig config,
            StreamExecutionEnvironment environment,
            RowDataKeySelector keySelector) {
        if (unsupportedReason(
                        inputType,
                        outputType,
                        grouping,
                        calls,
                        retractable,
                        generateUpdateBefore,
                        needRetraction,
                        stateRetentionTime,
                        config)
                != null) {
            return null;
        }
        StreamFusionStateBackendFactory.install(environment);
        byte[] plan = StreamFusionGroupAggregatePlan.create(
                inputType, grouping, calls, retractable, generateUpdateBefore, needRetraction);
        Transformation<RowData> partitionedInput = input;
        if (!"StreamFusionExchangeReader".equals(input.getName())) {
            // A keyed transformation may become a separate task even when the planner elides an
            // Exchange (for example VALUES has max parallelism one). Frame that edge explicitly
            // and use the same Flink-compatible hash/key-group partitioning as a planned exchange.
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
                calls.length == 0 ? "streamfusion-select-distinct" : "streamfusion-group-aggregate",
                new StreamFusionArrowGroupAggregateOperator(
                        inputType, outputType, grouping, plan, needRetraction, keySelector),
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
            boolean[] retractable,
            boolean generateUpdateBefore,
            boolean needRetraction,
            long stateRetentionTime,
            ReadableConfig config) {
        if (calls.length != retractable.length) {
            return "aggregate: calls and retraction requirements must be equally sized";
        }
        if (outputType.getFieldCount() != grouping.length + calls.length) {
            return "schema: group aggregate output must contain grouping fields followed by aggregate values";
        }
        if (stateRetentionTime != 0) {
            return "state: native group aggregate TTL is not implemented";
        }
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)) {
            return "mini-batch: native group aggregation is not implemented";
        }
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED)) {
            return "state: Flink async-state mode is not implemented by native group aggregate";
        }
        if (config.get(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG)) {
            return "state: Flink changelog-state wrapping is not implemented by native group aggregate";
        }
        for (int outputIndex = 0; outputIndex < grouping.length; outputIndex++) {
            int inputIndex = grouping[outputIndex];
            if (inputIndex < 0 || inputIndex >= inputType.getFieldCount()) {
                return "key: index " + inputIndex + " is outside the input row";
            }
            if (!inputType.getTypeAt(inputIndex).equals(outputType.getTypeAt(outputIndex))) {
                return "key[" + outputIndex + "]: input and output types must match exactly";
            }
        }
        for (int index = 0; index < calls.length; index++) {
            String reason = unsupportedCall(inputType, outputType.getTypeAt(grouping.length + index), calls[index]);
            if (reason != null) {
                return "aggregate[" + index + "]: " + reason;
            }
            if (needRetraction && !retractable[index]) {
                return "aggregate[" + index + "]: Flink did not select a retractable accumulator";
            }
        }
        return null;
    }

    public static String unsupportedCall(RowType inputType, LogicalType outputType, AggregateCall call) {
        if (call.isDistinct()) {
            return "DISTINCT is not implemented";
        }
        if (call.isApproximate()) {
            return "approximate aggregation is not implemented";
        }
        if (call.ignoreNulls()) {
            return "IGNORE NULLS is not implemented";
        }
        if (call.filterArg >= 0) {
            return "FILTER is not implemented";
        }
        if (!call.getCollation().getFieldCollations().isEmpty()) {
            return "ordered aggregation is not implemented";
        }
        SqlKind kind = call.getAggregation().getKind();
        int arguments = call.getArgList().size();
        if (kind == SqlKind.COUNT && arguments > 1) {
            return "COUNT accepts zero or one input in the initial native slice";
        }
        if (kind != SqlKind.COUNT
                && kind != SqlKind.SUM
                && kind != SqlKind.SUM0
                && kind != SqlKind.MIN
                && kind != SqlKind.MAX) {
            return call.getAggregation().getName() + " is not implemented";
        }
        if (kind != SqlKind.COUNT && arguments != 1) {
            return call.getAggregation().getName() + " requires exactly one input";
        }
        if (arguments == 1) {
            int inputIndex = call.getArgList().get(0);
            if (inputIndex < 0 || inputIndex >= inputType.getFieldCount()) {
                return "input index " + inputIndex + " is outside the input row";
            }
            LogicalTypeRoot inputRoot = inputType.getTypeAt(inputIndex).getTypeRoot();
            if ((kind == SqlKind.SUM || kind == SqlKind.SUM0) && !supportedSum(inputRoot)) {
                return inputType.getTypeAt(inputIndex) + " is not supported by native SUM";
            }
            if ((kind == SqlKind.MIN || kind == SqlKind.MAX) && !supportedExtremum(inputRoot)) {
                return inputType.getTypeAt(inputIndex) + " is not supported by native " + kind;
            }
        }
        LogicalType plannedOutput = FlinkTypeFactory.toLogicalType(call.getType());
        if (!plannedOutput.equals(outputType)) {
            return "Flink call type " + plannedOutput + " does not match output " + outputType;
        }
        if (kind == SqlKind.COUNT && outputType.getTypeRoot() != LogicalTypeRoot.BIGINT) {
            return "COUNT output must be BIGINT, got " + outputType;
        }
        if ((kind == SqlKind.SUM || kind == SqlKind.SUM0) && !supportedSum(outputType.getTypeRoot())) {
            return "output type " + outputType + " is not supported by native SUM";
        }
        if ((kind == SqlKind.MIN || kind == SqlKind.MAX) && !supportedExtremum(outputType.getTypeRoot())) {
            return "output type " + outputType + " is not supported by native " + kind;
        }
        return null;
    }

    private static boolean supportedSum(LogicalTypeRoot root) {
        switch (root) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return true;
            default:
                return false;
        }
    }

    private static boolean supportedExtremum(LogicalTypeRoot root) {
        switch (root) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case BOOLEAN:
            case CHAR:
            case VARCHAR:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return true;
            default:
                return false;
        }
    }
}

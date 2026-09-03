/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.join;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM;

import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.exchange.NativeExchangeFrameKeySelector;
import tech.streamfusion.flink.exchange.NativeExchangeFrameTypeInfo;
import tech.streamfusion.flink.exchange.NativeExchangeReaderOperator;
import tech.streamfusion.flink.exchange.StreamFusionExchangeTranslator;
import tech.streamfusion.flink.state.StreamFusionStateBackendFactory;

/** Reflection entry point for native synchronous regular streaming joins. */
public final class StreamFusionRegularJoinTranslator {
    private StreamFusionRegularJoinTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> left,
            Transformation<RowData> right,
            RowType leftType,
            RowType rightType,
            RowType outputType,
            JoinSpec joinSpec,
            List<int[]> leftUpsertKeys,
            List<int[]> rightUpsertKeys,
            long leftStateTtlMillis,
            long rightStateTtlMillis,
            ReadableConfig config,
            StreamExecutionEnvironment environment,
            RowDataKeySelector leftSelector,
            RowDataKeySelector rightSelector) {
        String reason = unsupportedReason(
                leftType,
                rightType,
                outputType,
                joinSpec,
                leftUpsertKeys,
                rightUpsertKeys,
                leftStateTtlMillis,
                rightStateTtlMillis,
                config);
        if (reason != null) {
            return null;
        }
        byte[] plan = StreamFusionRegularJoinPlan.create(
                leftType,
                rightType,
                joinSpec.getLeftKeys(),
                joinSpec.getRightKeys(),
                joinSpec.getFilterNulls(),
                joinSpec.getJoinType());
        StreamFusionStateBackendFactory.install(environment);
        Transformation<RowData> keyedLeft = keyed(left, leftType, joinSpec.getLeftKeys(), config, environment);
        Transformation<RowData> keyedRight = keyed(right, rightType, joinSpec.getRightKeys(), config, environment);
        FramedInput framedLeft = framed(keyedLeft);
        FramedInput framedRight = framed(keyedRight);
        StreamFusionArrowRegularJoinOperator operator = new StreamFusionArrowRegularJoinOperator(
                leftType,
                rightType,
                outputType,
                joinSpec.getLeftKeys(),
                joinSpec.getRightKeys(),
                plan,
                leftSelector,
                rightSelector,
                framedLeft.plan,
                framedRight.plan);
        TwoInputTransformation<
                        NativeExchangeFrame, NativeExchangeFrame, tech.streamfusion.flink.arrow.ArrowRowDataBatch>
                transformation = new TwoInputTransformation<>(
                        framedLeft.transformation,
                        framedRight.transformation,
                        "streamfusion-regular-join[" + joinSpec.getJoinType() + "]",
                        operator,
                        ArrowRowDataBatchTypeInfo.INSTANCE,
                        keyedLeft.getParallelism(),
                        false);
        int maxParallelism =
                keyedLeft.getMaxParallelism() > 0 ? keyedLeft.getMaxParallelism() : DEFAULT_LOWER_BOUND_MAX_PARALLELISM;
        transformation.setMaxParallelism(maxParallelism);
        transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);
        NativeExchangeFrameKeySelector selector = new NativeExchangeFrameKeySelector(maxParallelism);
        transformation.setStateKeySelectors(selector, selector);
        transformation.setStateKeyType(Types.INT);
        return tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries.asPlannerTransformation(transformation);
    }

    public static String unsupportedReason(
            RowType leftType,
            RowType rightType,
            RowType outputType,
            JoinSpec joinSpec,
            List<int[]> leftUpsertKeys,
            List<int[]> rightUpsertKeys,
            long leftStateTtlMillis,
            long rightStateTtlMillis,
            ReadableConfig config) {
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)) {
            return "mini-batch: native regular join bundle and suppression semantics are not implemented yet";
        }
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED)) {
            return "state: Flink async-state mode is not implemented by native regular join";
        }
        if (config.get(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG)) {
            return "state: Flink changelog-state wrapping is not implemented by native regular join";
        }
        if (joinSpec.getNonEquiCondition().isPresent()) {
            return "join condition: native regular join condition feedback is not implemented yet";
        }
        if ((leftUpsertKeys != null && !leftUpsertKeys.isEmpty())
                || (rightUpsertKeys != null && !rightUpsertKeys.isEmpty())) {
            return "upsert state: native regular join unique-key compaction is not implemented yet";
        }
        if (leftStateTtlMillis != 0 || rightStateTtlMillis != 0) {
            return "state TTL: native regular join expiration semantics are not implemented yet";
        }
        if (joinSpec.getLeftKeys().length != joinSpec.getRightKeys().length
                || joinSpec.getLeftKeys().length != joinSpec.getFilterNulls().length) {
            return "join keys: left, right, and null-filter key counts differ";
        }
        for (int key : joinSpec.getLeftKeys()) {
            if (key < 0 || key >= leftType.getFieldCount()) {
                return "left join key: index " + key + " is outside the input row";
            }
        }
        for (int key : joinSpec.getRightKeys()) {
            if (key < 0 || key >= rightType.getFieldCount()) {
                return "right join key: index " + key + " is outside the input row";
            }
        }
        int expectedOutput = leftType.getFieldCount();
        switch (joinSpec.getJoinType()) {
            case INNER:
            case LEFT:
            case RIGHT:
            case FULL:
                expectedOutput += rightType.getFieldCount();
                break;
            case SEMI:
            case ANTI:
                break;
            default:
                return "join type: " + joinSpec.getJoinType() + " is not a regular streaming join";
        }
        if (outputType.getFieldCount() != expectedOutput) {
            return "schema: regular join output arity does not match its join type";
        }
        for (int side = 0; side < 2; side++) {
            RowType type = side == 0 ? leftType : rightType;
            for (int index = 0; index < type.getFieldCount(); index++) {
                if (!supportedType(type.getTypeAt(index))) {
                    return (side == 0 ? "left" : "right") + " state row[" + index + "]: " + type.getTypeAt(index)
                            + " has no native Arrow-row representation";
                }
            }
        }
        return null;
    }

    private static boolean supportedType(LogicalType type) {
        if (type instanceof DistinctType) {
            return supportedType(((DistinctType) type).getSourceType());
        }
        if (type instanceof StructuredType) {
            return type.getChildren().stream().allMatch(StreamFusionRegularJoinTranslator::supportedType);
        }
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
            case DECIMAL:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
            case ARRAY:
            case MAP:
            case MULTISET:
            case ROW:
                return type.getChildren().stream().allMatch(StreamFusionRegularJoinTranslator::supportedType);
            default:
                return false;
        }
    }

    private static Transformation<RowData> keyed(
            Transformation<RowData> input,
            RowType type,
            int[] keys,
            ReadableConfig config,
            StreamExecutionEnvironment environment) {
        if ("StreamFusionExchangeReader".equals(input.getName())) {
            return input;
        }
        if (keys.length == 0) {
            return StreamFusionExchangeTranslator.singleton(input, type);
        }
        return StreamFusionExchangeTranslator.hash(
                input,
                type,
                keys,
                DEFAULT_LOWER_BOUND_MAX_PARALLELISM,
                environment.getParallelism(),
                config.get(CheckpointingOptions.ENABLE_UNALIGNED) || config.get(CheckpointingOptions.FORCE_UNALIGNED));
    }

    @SuppressWarnings("unchecked")
    private static FramedInput framed(Transformation<RowData> input) {
        if (!(input instanceof OneInputTransformation) || !"StreamFusionExchangeReader".equals(input.getName())) {
            throw new IllegalStateException("Native regular join requires a framed exchange on both inputs");
        }
        OneInputTransformation<?, ?> reader = (OneInputTransformation<?, ?>) input;
        if (!(reader.getOperatorFactory() instanceof SimpleOperatorFactory)) {
            throw new IllegalStateException("Native regular join cannot inspect its exchange reader factory");
        }
        Object operator = ((SimpleOperatorFactory<?>) reader.getOperatorFactory()).getOperator();
        if (!(operator instanceof NativeExchangeReaderOperator)) {
            throw new IllegalStateException("Native regular join received an incompatible exchange reader");
        }
        Transformation<?> framed = reader.getInputs().get(0);
        if (!(framed.getOutputType() instanceof NativeExchangeFrameTypeInfo)) {
            throw new IllegalStateException("Native regular join exchange input is not frame encoded");
        }
        return new FramedInput(
                (Transformation<NativeExchangeFrame>) framed,
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
}

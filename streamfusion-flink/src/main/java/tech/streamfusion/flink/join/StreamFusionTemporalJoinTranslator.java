/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.join;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM;

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
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
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

/** Reflection entry point for native event- and processing-time temporal table joins. */
public final class StreamFusionTemporalJoinTranslator {
    private StreamFusionTemporalJoinTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> left,
            Transformation<RowData> right,
            RowType leftType,
            RowType rightType,
            RowType outputType,
            JoinSpec joinSpec,
            boolean temporalFunctionJoin,
            int leftTimeIndex,
            int rightTimeIndex,
            ReadableConfig config,
            StreamExecutionEnvironment environment,
            RowDataKeySelector leftSelector,
            RowDataKeySelector rightSelector,
            GeneratedJoinCondition condition) {
        String reason = unsupportedReason(
                leftType, rightType, outputType, joinSpec, temporalFunctionJoin, leftTimeIndex, rightTimeIndex, config);
        if (reason != null) {
            return null;
        }
        boolean processingTime = rightTimeIndex < 0;
        long minRetention =
                config.get(ExecutionConfigOptions.IDLE_STATE_RETENTION).toMillis();
        long maxRetention = TableConfigUtils.getMaxIdleStateRetentionTime(config);
        byte[] plan = StreamFusionTemporalJoinPlan.create(
                leftType,
                rightType,
                joinSpec.getLeftKeys(),
                joinSpec.getRightKeys(),
                joinSpec.getFilterNulls(),
                joinSpec.getJoinType(),
                processingTime,
                leftTimeIndex,
                rightTimeIndex,
                minRetention,
                maxRetention);
        StreamFusionStateBackendFactory.install(environment);
        Transformation<RowData> keyedLeft = keyed(left, leftType, joinSpec.getLeftKeys(), config, environment);
        Transformation<RowData> keyedRight = keyed(right, rightType, joinSpec.getRightKeys(), config, environment);
        FramedInput framedLeft = framed(keyedLeft);
        FramedInput framedRight = framed(keyedRight);
        StreamFusionArrowTemporalJoinOperator operator = new StreamFusionArrowTemporalJoinOperator(
                leftType,
                rightType,
                outputType,
                joinSpec.getLeftKeys(),
                joinSpec.getRightKeys(),
                plan,
                leftSelector,
                rightSelector,
                framedLeft.plan,
                framedRight.plan,
                processingTime,
                joinSpec.getJoinType(),
                condition);
        TwoInputTransformation<
                        NativeExchangeFrame, NativeExchangeFrame, tech.streamfusion.flink.arrow.ArrowRowDataBatch>
                transformation = new TwoInputTransformation<>(
                        framedLeft.transformation,
                        framedRight.transformation,
                        "streamfusion-temporal-join[" + joinSpec.getJoinType() + "]",
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
            boolean temporalFunctionJoin,
            int leftTimeIndex,
            int rightTimeIndex,
            ReadableConfig config) {
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED)) {
            return "state: Flink async-state mode is not implemented by native temporal join";
        }
        if (config.get(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG)) {
            return "state: Flink changelog-state wrapping is not implemented by native temporal join";
        }
        if (joinSpec.getJoinType() != FlinkJoinType.INNER && joinSpec.getJoinType() != FlinkJoinType.LEFT) {
            return "join type: temporal table joins support only INNER and LEFT";
        }
        if (temporalFunctionJoin && joinSpec.getJoinType() != FlinkJoinType.INNER) {
            return "join type: temporal table-function joins support only INNER";
        }
        if (rightTimeIndex < 0 && !temporalFunctionJoin) {
            return "processing-time temporal table join is rejected by Flink (FLINK-19830)";
        }
        if (rightTimeIndex >= 0
                && (leftTimeIndex < 0
                        || leftTimeIndex >= leftType.getFieldCount()
                        || rightTimeIndex >= rightType.getFieldCount())) {
            return "time attributes: event-time temporal join indices are outside their input rows";
        }
        if (joinSpec.getLeftKeys().length != joinSpec.getRightKeys().length
                || joinSpec.getLeftKeys().length != joinSpec.getFilterNulls().length) {
            return "join keys: left, right, and null-filter key counts differ";
        }
        for (int side = 0; side < 2; side++) {
            RowType type = side == 0 ? leftType : rightType;
            int[] keys = side == 0 ? joinSpec.getLeftKeys() : joinSpec.getRightKeys();
            for (int key : keys) {
                if (key < 0 || key >= type.getFieldCount()) {
                    return (side == 0 ? "left" : "right") + " join key index is outside the input row";
                }
            }
            for (int index = 0; index < type.getFieldCount(); index++) {
                if (!supportedType(type.getTypeAt(index))) {
                    return (side == 0 ? "left" : "right") + " state row[" + index + "]: " + type.getTypeAt(index)
                            + " has no native Arrow-row representation";
                }
            }
        }
        if (outputType.getFieldCount() != leftType.getFieldCount() + rightType.getFieldCount()) {
            return "schema: temporal join output arity must contain both input rows";
        }
        return null;
    }

    private static boolean supportedType(LogicalType type) {
        if (type instanceof DistinctType) {
            return supportedType(((DistinctType) type).getSourceType());
        }
        if (type instanceof StructuredType) {
            return type.getChildren().stream().allMatch(StreamFusionTemporalJoinTranslator::supportedType);
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
                return type.getChildren().stream().allMatch(StreamFusionTemporalJoinTranslator::supportedType);
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
            throw new IllegalStateException("Native temporal join requires a framed exchange on both inputs");
        }
        OneInputTransformation<?, ?> reader = (OneInputTransformation<?, ?>) input;
        if (!(reader.getOperatorFactory() instanceof SimpleOperatorFactory)) {
            throw new IllegalStateException("Native temporal join cannot inspect its exchange reader factory");
        }
        Object operator = ((SimpleOperatorFactory<?>) reader.getOperatorFactory()).getOperator();
        if (!(operator instanceof NativeExchangeReaderOperator)) {
            throw new IllegalStateException("Native temporal join received an incompatible exchange reader");
        }
        Transformation<?> framed = reader.getInputs().get(0);
        if (!(framed.getOutputType() instanceof NativeExchangeFrameTypeInfo)) {
            throw new IllegalStateException("Native temporal join exchange input is not frame encoded");
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

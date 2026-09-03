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
import org.apache.flink.table.planner.plan.nodes.exec.spec.IntervalJoinSpec;
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

/** Reflection entry point for native event-time and processing-time interval joins. */
public final class StreamFusionIntervalJoinTranslator {
    private StreamFusionIntervalJoinTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> left,
            Transformation<RowData> right,
            RowType leftType,
            RowType rightType,
            RowType outputType,
            IntervalJoinSpec intervalJoinSpec,
            ReadableConfig config,
            StreamExecutionEnvironment environment,
            RowDataKeySelector leftSelector,
            RowDataKeySelector rightSelector) {
        String reason = unsupportedReason(leftType, rightType, outputType, intervalJoinSpec, config);
        if (reason != null) {
            return null;
        }
        JoinSpec joinSpec = intervalJoinSpec.getJoinSpec();
        IntervalJoinSpec.WindowBounds bounds = intervalJoinSpec.getWindowBounds();
        long minCleanup = config.get(ExecutionConfigOptions.TABLE_EXEC_INTERVAL_JOIN_MIN_CLEAN_UP_INTERVAL)
                .toMillis();
        byte[] plan = StreamFusionIntervalJoinPlan.create(
                leftType,
                rightType,
                joinSpec.getLeftKeys(),
                joinSpec.getRightKeys(),
                joinSpec.getFilterNulls(),
                joinSpec.getJoinType(),
                bounds,
                minCleanup);
        StreamFusionStateBackendFactory.install(environment);
        Transformation<RowData> keyedLeft = keyed(left, leftType, joinSpec.getLeftKeys(), config, environment);
        Transformation<RowData> keyedRight = keyed(right, rightType, joinSpec.getRightKeys(), config, environment);
        FramedInput framedLeft = framed(keyedLeft);
        FramedInput framedRight = framed(keyedRight);
        long maxOutputDelay =
                bounds.isEventTime() ? Math.max(-bounds.getLeftLowerBound(), bounds.getLeftUpperBound()) : 0;
        StreamFusionArrowIntervalJoinOperator operator = new StreamFusionArrowIntervalJoinOperator(
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
                bounds.isEventTime(),
                maxOutputDelay);
        TwoInputTransformation<
                        NativeExchangeFrame, NativeExchangeFrame, tech.streamfusion.flink.arrow.ArrowRowDataBatch>
                transformation = new TwoInputTransformation<>(
                        framedLeft.transformation,
                        framedRight.transformation,
                        "streamfusion-interval-join[" + joinSpec.getJoinType() + "]",
                        operator,
                        ArrowRowDataBatchTypeInfo.INSTANCE,
                        keyedLeft.getParallelism(),
                        false);
        int maxParallelism =
                keyedLeft.getMaxParallelism() > 0 ? keyedLeft.getMaxParallelism() : DEFAULT_LOWER_BOUND_MAX_PARALLELISM;
        transformation.setMaxParallelism(maxParallelism);
        // Two timestamp-indexed input tables, timers, and the output probe need materially more
        // headroom than a stateless Arrow stage. This remains Flink OPERATOR managed memory.
        transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 8);
        NativeExchangeFrameKeySelector selector = new NativeExchangeFrameKeySelector(maxParallelism);
        transformation.setStateKeySelectors(selector, selector);
        transformation.setStateKeyType(Types.INT);
        return tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries.asPlannerTransformation(transformation);
    }

    public static String unsupportedReason(
            RowType leftType,
            RowType rightType,
            RowType outputType,
            IntervalJoinSpec intervalJoinSpec,
            ReadableConfig config) {
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)) {
            return "mini-batch: native interval join bundle semantics are not implemented yet";
        }
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED)) {
            return "state: Flink async-state mode is not implemented by native interval join";
        }
        if (config.get(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG)) {
            return "state: Flink changelog-state wrapping is not implemented by native interval join";
        }
        JoinSpec join = intervalJoinSpec.getJoinSpec();
        IntervalJoinSpec.WindowBounds bounds = intervalJoinSpec.getWindowBounds();
        if (join.getNonEquiCondition().isPresent()) {
            return "join condition: native interval join currently supports the extracted time bounds and equi keys only";
        }
        switch (join.getJoinType()) {
            case INNER:
            case LEFT:
            case RIGHT:
            case FULL:
                break;
            default:
                return "join type: " + join.getJoinType() + " is not supported by Flink interval join";
        }
        if (bounds.getLeftLowerBound() > bounds.getLeftUpperBound()) {
            return "interval bounds: negative relative windows use Flink's padding/filter topology";
        }
        if (join.getLeftKeys().length != join.getRightKeys().length
                || join.getLeftKeys().length != join.getFilterNulls().length) {
            return "join keys: left, right, and null-filter key counts differ";
        }
        if (outputType.getFieldCount() != leftType.getFieldCount() + rightType.getFieldCount()) {
            return "schema: interval join output arity does not equal both inputs";
        }
        if (bounds.isEventTime()
                && (bounds.getLeftTimeIdx() < 0
                        || bounds.getLeftTimeIdx() >= leftType.getFieldCount()
                        || bounds.getRightTimeIdx() < 0
                        || bounds.getRightTimeIdx() >= rightType.getFieldCount())) {
            return "time attribute: interval join time index is outside its input row";
        }
        for (int key : join.getLeftKeys()) {
            if (key < 0 || key >= leftType.getFieldCount()) {
                return "left join key: index " + key + " is outside the input row";
            }
        }
        for (int key : join.getRightKeys()) {
            if (key < 0 || key >= rightType.getFieldCount()) {
                return "right join key: index " + key + " is outside the input row";
            }
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
            return type.getChildren().stream().allMatch(StreamFusionIntervalJoinTranslator::supportedType);
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
                return type.getChildren().stream().allMatch(StreamFusionIntervalJoinTranslator::supportedType);
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
            throw new IllegalStateException("Native interval join requires a framed exchange on both inputs");
        }
        OneInputTransformation<?, ?> reader = (OneInputTransformation<?, ?>) input;
        if (!(reader.getOperatorFactory() instanceof SimpleOperatorFactory)) {
            throw new IllegalStateException("Native interval join cannot inspect its exchange reader factory");
        }
        Object operator = ((SimpleOperatorFactory<?>) reader.getOperatorFactory()).getOperator();
        if (!(operator instanceof NativeExchangeReaderOperator)) {
            throw new IllegalStateException("Native interval join received an incompatible exchange reader");
        }
        Transformation<?> framed = reader.getInputs().get(0);
        if (!(framed.getOutputType() instanceof NativeExchangeFrameTypeInfo)) {
            throw new IllegalStateException("Native interval join exchange input is not frame encoded");
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

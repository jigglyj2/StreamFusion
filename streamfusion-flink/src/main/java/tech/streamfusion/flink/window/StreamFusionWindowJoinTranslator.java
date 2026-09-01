/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.window;

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
import org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.util.TimeWindowUtil;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.exchange.NativeExchangeFrameKeySelector;
import tech.streamfusion.flink.exchange.NativeExchangeFrameTypeInfo;
import tech.streamfusion.flink.exchange.NativeExchangeReaderOperator;
import tech.streamfusion.flink.exchange.StreamFusionExchangeTranslator;
import tech.streamfusion.flink.state.StreamFusionStateBackendFactory;

/** Reflection entry point for native event-time Window Join. */
public final class StreamFusionWindowJoinTranslator {
    private StreamFusionWindowJoinTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> left,
            Transformation<RowData> right,
            RowType leftType,
            RowType rightType,
            RowType outputType,
            JoinSpec joinSpec,
            WindowingStrategy leftWindowing,
            WindowingStrategy rightWindowing,
            GeneratedJoinCondition condition,
            ReadableConfig config,
            StreamExecutionEnvironment environment,
            RowDataKeySelector leftSelector,
            RowDataKeySelector rightSelector) {
        String reason =
                unsupportedReason(leftType, rightType, outputType, joinSpec, leftWindowing, rightWindowing, config);
        if (reason != null) {
            return null;
        }
        int leftWindowEnd = ((WindowAttachedWindowingStrategy) leftWindowing).getWindowEnd();
        int rightWindowEnd = ((WindowAttachedWindowingStrategy) rightWindowing).getWindowEnd();
        String shiftTimeZone = TimeWindowUtil.getShiftTimeZone(
                        leftWindowing.getTimeAttributeType(), TableConfigUtils.getLocalTimeZone(config))
                .getId();
        byte[] plan = StreamFusionWindowJoinPlan.create(
                leftType,
                rightType,
                joinSpec.getLeftKeys(),
                joinSpec.getRightKeys(),
                leftWindowEnd,
                rightWindowEnd,
                shiftTimeZone);
        StreamFusionStateBackendFactory.install(environment);
        Transformation<RowData> keyedLeft = keyed(left, leftType, joinSpec.getLeftKeys(), config, environment);
        Transformation<RowData> keyedRight = keyed(right, rightType, joinSpec.getRightKeys(), config, environment);
        FramedInput framedLeft = framed(keyedLeft);
        FramedInput framedRight = framed(keyedRight);
        StreamFusionArrowWindowJoinOperator operator = new StreamFusionArrowWindowJoinOperator(
                leftType,
                rightType,
                outputType,
                joinSpec.getLeftKeys(),
                joinSpec.getRightKeys(),
                plan,
                leftSelector,
                rightSelector,
                joinSpec.getJoinType(),
                condition,
                joinSpec.getFilterNulls(),
                framedLeft.plan,
                framedRight.plan);
        TwoInputTransformation<
                        NativeExchangeFrame, NativeExchangeFrame, tech.streamfusion.flink.arrow.ArrowRowDataBatch>
                transformation = new TwoInputTransformation<>(
                        framedLeft.transformation,
                        framedRight.transformation,
                        "streamfusion-window-join[" + joinSpec.getJoinType() + "]",
                        operator,
                        ArrowRowDataBatchTypeInfo.INSTANCE,
                        keyedLeft.getParallelism(),
                        false);
        int maxParallelism =
                keyedLeft.getMaxParallelism() > 0 ? keyedLeft.getMaxParallelism() : DEFAULT_LOWER_BOUND_MAX_PARALLELISM;
        transformation.setMaxParallelism(maxParallelism);
        transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);
        NativeExchangeFrameKeySelector frameSelector = new NativeExchangeFrameKeySelector(maxParallelism);
        transformation.setStateKeySelectors(frameSelector, frameSelector);
        transformation.setStateKeyType(Types.INT);
        return tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries.asPlannerTransformation(transformation);
    }

    public static String unsupportedReason(
            RowType leftType,
            RowType rightType,
            RowType outputType,
            JoinSpec joinSpec,
            WindowingStrategy leftWindowing,
            WindowingStrategy rightWindowing,
            ReadableConfig config) {
        if (!(leftWindowing instanceof WindowAttachedWindowingStrategy)
                || !(rightWindowing instanceof WindowAttachedWindowingStrategy)) {
            return "window strategy: Window Join requires attached window columns on both inputs";
        }
        if (!leftWindowing.isRowtime() || !rightWindowing.isRowtime()) {
            return "window time: Flink does not support processing-time Window Join";
        }
        if (joinSpec.getLeftKeys().length != joinSpec.getRightKeys().length) {
            return "join keys: left and right key counts differ";
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
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED)) {
            return "state: Flink async-state mode is not implemented by native Window Join";
        }
        if (config.get(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG)) {
            return "state: Flink changelog-state wrapping is not implemented by native Window Join";
        }
        return null;
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
            throw new IllegalStateException("Native Window Join requires a framed exchange on both inputs");
        }
        OneInputTransformation<?, ?> reader = (OneInputTransformation<?, ?>) input;
        if (!(reader.getOperatorFactory() instanceof SimpleOperatorFactory)) {
            throw new IllegalStateException("Native Window Join cannot inspect its exchange reader factory");
        }
        Object operator = ((SimpleOperatorFactory<?>) reader.getOperatorFactory()).getOperator();
        if (!(operator instanceof NativeExchangeReaderOperator)) {
            throw new IllegalStateException("Native Window Join received an incompatible exchange reader");
        }
        Transformation<?> framed = reader.getInputs().get(0);
        if (!(framed.getOutputType() instanceof NativeExchangeFrameTypeInfo)) {
            throw new IllegalStateException("Native Window Join exchange input is not frame encoded");
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

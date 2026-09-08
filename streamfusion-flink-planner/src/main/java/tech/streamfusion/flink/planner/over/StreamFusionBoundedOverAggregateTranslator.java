/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner.over;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.spec.OverSpec;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.exchange.NativeExchangeFrameKeySelector;
import tech.streamfusion.flink.exchange.NativeExchangeFrameTypeInfo;
import tech.streamfusion.flink.exchange.NativeExchangeReaderOperator;
import tech.streamfusion.flink.over.StreamFusionArrowBoundedOverAggregateOperator;
import tech.streamfusion.flink.state.StreamFusionStateBackendFactory;
import tech.streamfusion.proto.plan.v1.ExchangeDistribution;
import tech.streamfusion.proto.plan.v1.NativeExchangePlan;

/** Reflection entry point for a fused bounded sort and BatchExecOverAggregate. */
public final class StreamFusionBoundedOverAggregateTranslator {
    private StreamFusionBoundedOverAggregateTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> input,
            RowType inputType,
            RowType outputType,
            OverSpec overSpec,
            ReadableConfig config,
            StreamExecutionEnvironment environment) {
        if (unsupportedReason(inputType, outputType, overSpec, config) != null) {
            return null;
        }
        StreamFusionStateBackendFactory.install(environment);
        FramedInput framed = framed(input);
        int[] partitionKeys = overSpec.getPartition().getFieldIndices();
        if (framed.distribution != ExchangeDistribution.EXCHANGE_DISTRIBUTION_HASH
                && !(partitionKeys.length == 0
                        && framed.distribution == ExchangeDistribution.EXCHANGE_DISTRIBUTION_SINGLETON)) {
            throw new IllegalStateException("Bounded OVER requires its Flink hash or singleton exchange");
        }
        byte[] plan = StreamFusionOverAggregatePlan.createBounded(inputType, outputType, overSpec);
        OneInputTransformation<NativeExchangeFrame, ArrowRowDataBatch> result = new OneInputTransformation<>(
                framed.transformation,
                "streamfusion-bounded-over-aggregate",
                new StreamFusionArrowBoundedOverAggregateOperator(inputType, outputType, plan, framed.plan),
                ArrowRowDataBatchTypeInfo.INSTANCE,
                framed.parallelism,
                false);
        result.setParallelism(framed.parallelism);
        result.setMaxParallelism(framed.maxParallelism);
        // This single native stage owns both Flink's external sort and OVER frame state.
        result.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 128);
        result.setStateKeySelector(new NativeExchangeFrameKeySelector(framed.maxParallelism));
        result.setStateKeyType(Types.INT);
        return StreamFusionArrowBoundaries.asPlannerTransformation(result);
    }

    public static String unsupportedReason(
            RowType inputType, RowType outputType, OverSpec overSpec, ReadableConfig config) {
        return StreamFusionOverAggregateTranslator.unsupportedBoundedReason(inputType, outputType, overSpec, config);
    }

    @SuppressWarnings("unchecked")
    private static FramedInput framed(Transformation<RowData> input) {
        if (!(input instanceof OneInputTransformation) || !"StreamFusionExchangeReader".equals(input.getName())) {
            throw new IllegalStateException("Native bounded OVER requires a framed exchange");
        }
        OneInputTransformation<?, ?> reader = (OneInputTransformation<?, ?>) input;
        if (!(reader.getOperatorFactory() instanceof SimpleOperatorFactory)) {
            throw new IllegalStateException("Native bounded OVER cannot inspect its exchange reader factory");
        }
        Object operator = ((SimpleOperatorFactory<?>) reader.getOperatorFactory()).getOperator();
        if (!(operator instanceof NativeExchangeReaderOperator)) {
            throw new IllegalStateException("Native bounded OVER received an incompatible exchange reader");
        }
        Transformation<?> frames = reader.getInputs().get(0);
        if (!(frames.getOutputType() instanceof NativeExchangeFrameTypeInfo)) {
            throw new IllegalStateException("Native bounded OVER exchange input is not frame encoded");
        }
        byte[] plan = ((NativeExchangeReaderOperator) operator).serializedPlan();
        NativeExchangePlan exchange;
        try {
            exchange = NativeExchangePlan.parseFrom(plan);
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalStateException("Native bounded OVER received a corrupt exchange contract", failure);
        }
        int maxParallelism =
                exchange.getMaxParallelism() > 0 ? exchange.getMaxParallelism() : DEFAULT_LOWER_BOUND_MAX_PARALLELISM;
        return new FramedInput(
                (Transformation<NativeExchangeFrame>) frames,
                plan,
                exchange.getDistribution(),
                maxParallelism,
                exchange.getParallelism());
    }

    private static final class FramedInput {
        private final Transformation<NativeExchangeFrame> transformation;
        private final byte[] plan;
        private final ExchangeDistribution distribution;
        private final int maxParallelism;
        private final int parallelism;

        private FramedInput(
                Transformation<NativeExchangeFrame> transformation,
                byte[] plan,
                ExchangeDistribution distribution,
                int maxParallelism,
                int parallelism) {
            this.transformation = transformation;
            this.plan = plan;
            this.distribution = distribution;
            this.maxParallelism = maxParallelism;
            this.parallelism = parallelism;
        }
    }
}

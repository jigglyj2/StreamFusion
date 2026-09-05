/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.rank;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.ArrowUtils;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.exchange.NativeExchangeFrameKeySelector;
import tech.streamfusion.flink.exchange.NativeExchangeFrameTypeInfo;
import tech.streamfusion.flink.exchange.NativeExchangeReaderOperator;
import tech.streamfusion.flink.state.StreamFusionStateBackendFactory;
import tech.streamfusion.proto.plan.v1.ExchangeDistribution;
import tech.streamfusion.proto.plan.v1.NativeExchangePlan;

/** Reflection entry point for bounded RANK over a Flink-sorted input. */
public final class StreamFusionBoundedRankTranslator {
    private StreamFusionBoundedRankTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> input,
            RowType inputType,
            RowType outputType,
            int[] partitionFields,
            int[] sortFields,
            long rankStart,
            long rankEnd,
            boolean outputRankNumber,
            GeneratedRecordComparator partitionComparator,
            GeneratedRecordComparator orderComparator) {
        if (unsupportedReason(inputType, outputType, partitionFields, sortFields, rankStart, rankEnd, outputRankNumber)
                != null) {
            return null;
        }
        Transformation<ArrowRowDataBatch> arrowInput = StreamFusionArrowBoundaries.toArrow(input, inputType);
        OneInputTransformation<ArrowRowDataBatch, ArrowRowDataBatch> result = new OneInputTransformation<>(
                arrowInput,
                "streamfusion-bounded-rank",
                new StreamFusionArrowBoundedRankOperator(
                        StreamFusionBoundedRankPlan.create(
                                inputType,
                                outputType,
                                partitionFields,
                                sortFields,
                                rankStart,
                                rankEnd,
                                outputRankNumber),
                        outputType),
                ArrowRowDataBatchTypeInfo.INSTANCE,
                input.getParallelism(),
                false);
        result.setMaxParallelism(input.getParallelism() == 1 ? 1 : inheritedMaxParallelism(input));
        // The upstream sort emits managed 16K-row terminal batches. Give the native Arrow gather
        // enough of the shared OPERATOR pool to hold one such batch plus selection/rank vectors.
        result.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 4);
        return StreamFusionArrowBoundaries.asPlannerTransformation(result);
    }

    /** Fuses the required BatchExecSort into a bounded, tie-aware keyed selection. */
    public static Transformation<RowData> translateSelection(
            Transformation<RowData> input,
            RowType inputType,
            RowType outputType,
            int[] partitionFields,
            int[] sortFields,
            SortSpec inputSortSpec,
            long rankStart,
            long rankEnd,
            boolean outputRankNumber,
            ReadableConfig config,
            StreamExecutionEnvironment environment) {
        StreamFusionStateBackendFactory.install(environment);
        FramedInput framed = framed(input);
        if (framed.distribution != ExchangeDistribution.EXCHANGE_DISTRIBUTION_HASH
                && !(partitionFields.length == 0
                        && framed.distribution == ExchangeDistribution.EXCHANGE_DISTRIBUTION_SINGLETON)) {
            throw new IllegalStateException("Bounded RANK requires its Flink hash or singleton exchange");
        }
        byte[] plan = StreamFusionBoundedRankSelectPlan.create(
                inputType,
                outputType,
                partitionFields,
                sortFields,
                inputSortSpec,
                rankStart,
                rankEnd,
                outputRankNumber);
        OneInputTransformation<NativeExchangeFrame, ArrowRowDataBatch> result = new OneInputTransformation<>(
                framed.transformation,
                "streamfusion-bounded-rank-selection",
                new StreamFusionArrowBoundedRankSelectOperator(inputType, outputType, plan, framed.plan),
                ArrowRowDataBatchTypeInfo.INSTANCE,
                framed.parallelism,
                false);
        result.setParallelism(framed.parallelism);
        result.setMaxParallelism(framed.maxParallelism);
        result.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 16);
        result.setStateKeySelector(new NativeExchangeFrameKeySelector(framed.maxParallelism));
        result.setStateKeyType(Types.INT);
        return StreamFusionArrowBoundaries.asPlannerTransformation(result);
    }

    public static String unsupportedReason(
            RowType inputType,
            RowType outputType,
            int[] partitionFields,
            int[] sortFields,
            long rankStart,
            long rankEnd,
            boolean outputRankNumber) {
        if (rankStart <= 0 || rankEnd < rankStart) {
            return "bounded rank: invalid inclusive range [" + rankStart + ", " + rankEnd + "]";
        }
        int expectedFields = inputType.getFieldCount() + (outputRankNumber ? 1 : 0);
        if (outputType.getFieldCount() != expectedFields) {
            return "bounded rank: output does not match the input plus its optional rank column";
        }
        if (outputRankNumber && !(outputType.getTypeAt(outputType.getFieldCount() - 1) instanceof BigIntType)) {
            return "bounded rank: rank output column is not BIGINT";
        }
        for (int field : partitionFields) {
            if (field < 0 || field >= inputType.getFieldCount()) {
                return "bounded rank: partition field " + field + " is outside the input row";
            }
        }
        if (sortFields.length == 0) {
            return "bounded rank: RANK requires at least one ordering field";
        }
        for (int field : sortFields) {
            if (field < 0 || field >= inputType.getFieldCount()) {
                return "bounded rank: order field " + field + " is outside the input row";
            }
        }
        try {
            ArrowUtils.toArrowSchema(inputType);
            ArrowUtils.toArrowSchema(outputType);
        } catch (RuntimeException failure) {
            return "schema: " + failure.getMessage();
        }
        return null;
    }

    private static int inheritedMaxParallelism(Transformation<?> input) {
        if (input.getMaxParallelism() > 0) {
            return input.getMaxParallelism();
        }
        for (Transformation<?> parent : input.getInputs()) {
            int candidate = inheritedMaxParallelism(parent);
            if (candidate > 0) {
                return candidate;
            }
        }
        return DEFAULT_LOWER_BOUND_MAX_PARALLELISM;
    }

    @SuppressWarnings("unchecked")
    private static FramedInput framed(Transformation<RowData> input) {
        if (!(input instanceof OneInputTransformation) || !"StreamFusionExchangeReader".equals(input.getName())) {
            throw new IllegalStateException("Native bounded RANK selection requires a framed exchange");
        }
        OneInputTransformation<?, ?> reader = (OneInputTransformation<?, ?>) input;
        if (!(reader.getOperatorFactory() instanceof SimpleOperatorFactory)) {
            throw new IllegalStateException("Native bounded RANK cannot inspect its exchange reader factory");
        }
        Object operator = ((SimpleOperatorFactory<?>) reader.getOperatorFactory()).getOperator();
        if (!(operator instanceof NativeExchangeReaderOperator)) {
            throw new IllegalStateException("Native bounded RANK received an incompatible exchange reader");
        }
        Transformation<?> frames = reader.getInputs().get(0);
        if (!(frames.getOutputType() instanceof NativeExchangeFrameTypeInfo)) {
            throw new IllegalStateException("Native bounded RANK exchange input is not frame encoded");
        }
        byte[] plan = ((NativeExchangeReaderOperator) operator).serializedPlan();
        NativeExchangePlan exchange;
        try {
            exchange = NativeExchangePlan.parseFrom(plan);
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalStateException("Native bounded RANK received a corrupt exchange contract", failure);
        }
        return new FramedInput(
                (Transformation<NativeExchangeFrame>) frames,
                plan,
                exchange.getDistribution(),
                exchange.getMaxParallelism(),
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

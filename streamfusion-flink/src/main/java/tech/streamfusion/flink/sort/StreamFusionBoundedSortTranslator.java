/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sort;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.ArrowUtils;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.exchange.NativeExchangeFrameKeySelector;
import tech.streamfusion.flink.exchange.NativeExchangeFrameTypeInfo;
import tech.streamfusion.flink.exchange.NativeExchangeReaderOperator;
import tech.streamfusion.flink.exchange.StreamFusionExchangeTranslator;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.flink.state.StreamFusionStateBackendFactory;
import tech.streamfusion.proto.plan.v1.ExchangeDistribution;
import tech.streamfusion.proto.plan.v1.NativeExchangePlan;

/** Reflection entry point for native bounded global or hash-partitioned full sort. */
public final class StreamFusionBoundedSortTranslator {
    private StreamFusionBoundedSortTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> input,
            RowType inputType,
            SortSpec sortSpec,
            ReadableConfig config,
            StreamExecutionEnvironment environment) {
        if (unsupportedReason(inputType, sortSpec, config) != null) {
            return null;
        }
        StreamFusionStateBackendFactory.install(environment);
        Transformation<RowData> sortedInput = input;
        if (!"StreamFusionExchangeReader".equals(input.getName())) {
            sortedInput = StreamFusionExchangeTranslator.singleton(input, inputType);
        }
        FramedInput framed = framed(sortedInput);
        boolean partitioned = framed.distribution == ExchangeDistribution.EXCHANGE_DISTRIBUTION_HASH;
        byte[] plan = partitioned
                ? StreamFusionBoundedSortPlan.createPartitioned(inputType, sortSpec)
                : StreamFusionBoundedSortPlan.create(inputType, sortSpec);
        OneInputTransformation<NativeExchangeFrame, tech.streamfusion.flink.arrow.ArrowRowDataBatch> transformation =
                new OneInputTransformation<>(
                        framed.transformation,
                        "streamfusion-bounded-sort",
                        new StreamFusionArrowBoundedSortOperator(inputType, plan, framed.plan),
                        ArrowRowDataBatchTypeInfo.INSTANCE,
                        framed.parallelism,
                        false);
        transformation.setParallelism(framed.parallelism);
        transformation.setMaxParallelism(framed.maxParallelism);
        // Flink's bounded StreamExecSort reserves its external-sort memory with weight 128. Keep
        // that physical-node weight so graph translation and the replacement expose the same
        // managed-memory contract.
        transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 128);
        transformation.setStateKeySelector(new NativeExchangeFrameKeySelector(framed.maxParallelism));
        transformation.setStateKeyType(Types.INT);
        return StreamFusionArrowBoundaries.asPlannerTransformation(transformation);
    }

    public static String unsupportedReason(RowType inputType, SortSpec sortSpec, ReadableConfig config) {
        if (sortSpec.getFieldSize() == 0) {
            return "sort: bounded full sort requires at least one ordering field";
        }
        for (int index = 0; index < sortSpec.getFieldSize(); index++) {
            int fieldIndex = sortSpec.getFieldSpec(index).getFieldIndex();
            if (fieldIndex < 0 || fieldIndex >= inputType.getFieldCount()) {
                return "sort: field index " + fieldIndex + " is outside the input row";
            }
            if (!orderable(inputType.getTypeAt(fieldIndex))) {
                return "sort: field " + fieldIndex + " type " + inputType.getTypeAt(fieldIndex)
                        + " has no exact native Flink comparator";
            }
        }
        try {
            ArrowUtils.toArrowSchema(inputType);
            for (LogicalType type : inputType.getChildren()) {
                FlinkLogicalTypeProto.serialize(type);
            }
        } catch (RuntimeException failure) {
            return "schema: " + failure.getMessage();
        }
        return null;
    }

    private static boolean orderable(LogicalType type) {
        switch (type.getTypeRoot()) {
            case ARRAY:
                return orderable(((ArrayType) type).getElementType());
            case ROW:
                return type.getChildren().stream().allMatch(StreamFusionBoundedSortTranslator::orderable);
            case MAP:
            case MULTISET:
            case RAW:
            case SYMBOL:
            case DESCRIPTOR:
                return false;
            default:
                return true;
        }
    }

    @SuppressWarnings("unchecked")
    private static FramedInput framed(Transformation<RowData> input) {
        if (!(input instanceof OneInputTransformation) || !"StreamFusionExchangeReader".equals(input.getName())) {
            throw new IllegalStateException("Native bounded sort requires a framed exchange");
        }
        OneInputTransformation<?, ?> reader = (OneInputTransformation<?, ?>) input;
        if (!(reader.getOperatorFactory() instanceof SimpleOperatorFactory)) {
            throw new IllegalStateException("Native bounded sort cannot inspect its exchange reader factory");
        }
        Object operator = ((SimpleOperatorFactory<?>) reader.getOperatorFactory()).getOperator();
        if (!(operator instanceof NativeExchangeReaderOperator)) {
            throw new IllegalStateException("Native bounded sort received an incompatible exchange reader");
        }
        Transformation<?> frames = reader.getInputs().get(0);
        if (!(frames.getOutputType() instanceof NativeExchangeFrameTypeInfo)) {
            throw new IllegalStateException("Native bounded sort exchange input is not frame encoded");
        }
        byte[] plan = ((NativeExchangeReaderOperator) operator).serializedPlan();
        NativeExchangePlan exchange;
        try {
            exchange = NativeExchangePlan.parseFrom(plan);
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalStateException("Native bounded sort received a corrupt exchange contract", failure);
        }
        if (exchange.getDistribution() != ExchangeDistribution.EXCHANGE_DISTRIBUTION_HASH
                && exchange.getDistribution() != ExchangeDistribution.EXCHANGE_DISTRIBUTION_SINGLETON) {
            throw new IllegalStateException(
                    "Native bounded sort requires hash or singleton distribution, got " + exchange.getDistribution());
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

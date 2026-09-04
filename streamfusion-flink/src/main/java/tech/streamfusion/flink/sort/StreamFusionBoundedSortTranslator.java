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

/** Reflection entry point for native bounded global full sort. */
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
        Transformation<RowData> singleton = input;
        if (!"StreamFusionExchangeReader".equals(input.getName())) {
            singleton = StreamFusionExchangeTranslator.singleton(input, inputType);
        }
        FramedInput framed = framed(singleton);
        byte[] plan = StreamFusionBoundedSortPlan.create(inputType, sortSpec);
        OneInputTransformation<NativeExchangeFrame, tech.streamfusion.flink.arrow.ArrowRowDataBatch> transformation =
                new OneInputTransformation<>(
                        framed.transformation,
                        "streamfusion-bounded-sort",
                        new StreamFusionArrowBoundedSortOperator(inputType, plan, framed.plan),
                        ArrowRowDataBatchTypeInfo.INSTANCE,
                        1,
                        false);
        transformation.setParallelism(1);
        transformation.setMaxParallelism(1);
        // Flink's bounded StreamExecSort reserves its external-sort memory with weight 128. Keep
        // that physical-node weight so graph translation and the replacement expose the same
        // managed-memory contract.
        transformation.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 128);
        transformation.setStateKeySelector(new NativeExchangeFrameKeySelector(1));
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
            throw new IllegalStateException("Native bounded sort requires a framed singleton exchange");
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
        return new FramedInput(
                (Transformation<NativeExchangeFrame>) frames,
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

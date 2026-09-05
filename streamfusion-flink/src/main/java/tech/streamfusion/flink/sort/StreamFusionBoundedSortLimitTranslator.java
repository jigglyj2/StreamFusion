/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sort;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.runtime.keyselector.EmptyRowDataKeySelector;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchTypeInfo;
import tech.streamfusion.flink.arrow.StreamFusionArrowBoundaries;
import tech.streamfusion.flink.deduplicate.ArrowBatchKeySelector;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.exchange.NativeExchangeFrameKeySelector;
import tech.streamfusion.flink.exchange.NativeExchangeFrameTypeInfo;
import tech.streamfusion.flink.exchange.NativeExchangeReaderOperator;
import tech.streamfusion.flink.exchange.StreamFusionExchangeTranslator;
import tech.streamfusion.flink.state.StreamFusionStateBackendFactory;

/** Reflection entry point for two-stage native bounded SortLimit. */
public final class StreamFusionBoundedSortLimitTranslator {
    private StreamFusionBoundedSortLimitTranslator() {}

    public static Transformation<RowData> translate(
            Transformation<RowData> input,
            RowType inputType,
            SortSpec sortSpec,
            long limitStart,
            long limitEnd,
            boolean global,
            ReadableConfig config,
            StreamExecutionEnvironment environment) {
        if (unsupportedReason(inputType, sortSpec, limitStart, limitEnd, config) != null) {
            return null;
        }
        StreamFusionStateBackendFactory.install(environment);
        if (global) {
            Transformation<RowData> singleton = input;
            if (!"StreamFusionExchangeReader".equals(input.getName())) {
                singleton = StreamFusionExchangeTranslator.singleton(input, inputType);
            }
            FramedInput framed = framed(singleton);
            byte[] plan = StreamFusionBoundedSortPlan.create(inputType, sortSpec, limitStart, limitEnd, false);
            OneInputTransformation<NativeExchangeFrame, ArrowRowDataBatch> result = new OneInputTransformation<>(
                    framed.transformation,
                    "streamfusion-bounded-sort-limit[global]",
                    new StreamFusionArrowBoundedSortOperator(inputType, plan, framed.plan, false),
                    ArrowRowDataBatchTypeInfo.INSTANCE,
                    1,
                    false);
            result.setParallelism(1);
            result.setMaxParallelism(1);
            result.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);
            result.setStateKeySelector(new NativeExchangeFrameKeySelector(1));
            result.setStateKeyType(org.apache.flink.api.common.typeinfo.Types.INT);
            return StreamFusionArrowBoundaries.asPlannerTransformation(result);
        }

        Transformation<ArrowRowDataBatch> arrowInput = StreamFusionArrowBoundaries.toArrow(input, inputType);
        int parallelism = input.getParallelism();
        byte[] plan = StreamFusionBoundedSortPlan.create(inputType, sortSpec, 0L, limitEnd, true);
        OneInputTransformation<ArrowRowDataBatch, ArrowRowDataBatch> result = new OneInputTransformation<>(
                arrowInput,
                "streamfusion-bounded-sort-limit[local]",
                new StreamFusionArrowBoundedSortLimitOperator(inputType, plan),
                ArrowRowDataBatchTypeInfo.INSTANCE,
                parallelism,
                false);
        result.setParallelism(parallelism);
        result.setMaxParallelism(inheritedMaxParallelism(input));
        result.declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, 1);
        result.setStateKeySelector(new ArrowBatchKeySelector(EmptyRowDataKeySelector.INSTANCE));
        result.setStateKeyType(EmptyRowDataKeySelector.INSTANCE.getProducedType());
        return StreamFusionArrowBoundaries.asPlannerTransformation(result);
    }

    public static String unsupportedReason(
            RowType inputType, SortSpec sortSpec, long limitStart, long limitEnd, ReadableConfig config) {
        String reason = StreamFusionBoundedSortTranslator.unsupportedReason(inputType, sortSpec, config);
        if (reason != null) {
            return reason.replaceFirst("^sort: ", "sort-limit: ");
        }
        if (limitStart < 0 || limitEnd < limitStart || limitEnd == Long.MAX_VALUE) {
            return "sort-limit: invalid or unbounded half-open limit range [" + limitStart + ", " + limitEnd + ")";
        }
        if (limitEnd > Integer.MAX_VALUE) {
            return "sort-limit: Flink's heap requires limitEnd to fit a signed integer";
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
            throw new IllegalStateException("Native global SortLimit requires a framed singleton exchange");
        }
        OneInputTransformation<?, ?> reader = (OneInputTransformation<?, ?>) input;
        if (!(reader.getOperatorFactory() instanceof SimpleOperatorFactory)) {
            throw new IllegalStateException("Native SortLimit cannot inspect its exchange reader factory");
        }
        Object operator = ((SimpleOperatorFactory<?>) reader.getOperatorFactory()).getOperator();
        if (!(operator instanceof NativeExchangeReaderOperator)) {
            throw new IllegalStateException("Native SortLimit received an incompatible exchange reader");
        }
        Transformation<?> frames = reader.getInputs().get(0);
        if (!(frames.getOutputType() instanceof NativeExchangeFrameTypeInfo)) {
            throw new IllegalStateException("Native SortLimit exchange input is not frame encoded");
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

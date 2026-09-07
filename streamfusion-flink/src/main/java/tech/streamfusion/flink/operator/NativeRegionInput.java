/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.operator;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.exchange.NativeExchangeFrameTypeInfo;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.exchange.NativeExchangeReaderOperator;
import tech.streamfusion.flink.exchange.StreamFusionExchangeTranslator;
import tech.streamfusion.proto.plan.v1.NativeExchangePlan;

/** External transport binding; an existing exchange is decoded once at the native-region edge. */
final class NativeRegionInput {
    final Transformation<NativeExchangeFrame> frames;
    final byte[] exchangePlan;

    private NativeRegionInput(Transformation<NativeExchangeFrame> frames, byte[] exchangePlan) {
        this.frames = frames;
        this.exchangePlan = exchangePlan;
    }

    @SuppressWarnings("unchecked")
    static NativeRegionInput bind(Transformation<RowData> input, RowType type, boolean keyed) {
        if (input instanceof OneInputTransformation) {
            var single = (OneInputTransformation<?, ?>) input;
            if (single.getOperatorFactory() instanceof org.apache.flink.streaming.api.operators.SimpleOperatorFactory
                    && single.getOperator() instanceof NativeExchangeReaderOperator) {
                if (!(single.getInputs().get(0).getOutputType() instanceof NativeExchangeFrameTypeInfo)) {
                    throw new IllegalArgumentException(
                            "Native region exchange boundary does not carry Arrow IPC frames");
                }
                return new NativeRegionInput(
                        (Transformation<NativeExchangeFrame>) single.getInputs().get(0),
                        ((NativeExchangeReaderOperator) single.getOperator()).serializedPlan());
            }
        }
        if (keyed) {
            throw new IllegalArgumentException(
                    "A keyed native region requires a planned exchange on every external input; routing cannot be inferred");
        }
        byte[] plan = NativeExchangePlanSerializer.singleton(type);
        return new NativeRegionInput(StreamFusionExchangeTranslator.frameForMultiInput(input, type, plan), plan);
    }

    NativeExchangePlan contract() {
        try {
            NativeExchangePlan result = NativeExchangePlan.parseFrom(exchangePlan);
            if (result.getProtocolVersion() != 1) {
                throw new IllegalArgumentException("Unsupported native region exchange protocol");
            }
            return result;
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid native region exchange contract", failure);
        }
    }
}

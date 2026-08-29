/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.window;

import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.WindowTableFunction;

/** Builds an Arrow-native aligned Window TVF protobuf plan. */
final class StreamFusionWindowTableFunctionPlan {
    private StreamFusionWindowTableFunctionPlan() {}

    static byte[] create(
            int timeAttributeIndex, StreamFusionWindowTableFunctionTranslator.WindowParameters parameters) {
        WindowTableFunction window = WindowTableFunction.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .setTimeAttributeIndex(timeAttributeIndex)
                .setKind(parameters.kind)
                .setSizeMillis(parameters.sizeMillis)
                .setSlideOrStepMillis(parameters.slideOrStepMillis)
                .setOffsetMillis(parameters.offsetMillis)
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setWindowTableFunction(window))
                .build()
                .toByteArray();
    }
}

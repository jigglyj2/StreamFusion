/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.union;

import tech.streamfusion.proto.plan.v1.Input.Builder;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.Union;

/** Builds the native UNION plan used by C Data parity tests and future native block fusion. */
final class StreamFusionUnionPlan {
    private StreamFusionUnionPlan() {}

    static byte[] create(int inputCount) {
        Union.Builder union = Union.newBuilder();
        for (int inputIndex = 0; inputIndex < inputCount; inputIndex++) {
            Builder input = tech.streamfusion.proto.plan.v1.Input.newBuilder().setInputIndex(inputIndex);
            union.addInputs(Operator.newBuilder().setInput(input));
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setUnion(union))
                .build()
                .toByteArray();
    }
}

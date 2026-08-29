/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.expand;

import java.util.List;
import tech.streamfusion.proto.plan.v1.Expand;
import tech.streamfusion.proto.plan.v1.ExpandProjection;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

/** Builds an Arrow-native Expand protobuf plan. */
final class StreamFusionExpandPlan {
    private StreamFusionExpandPlan() {}

    static byte[] create(List<List<Expression>> projections) {
        Expand.Builder expand =
                Expand.newBuilder().setInput(Operator.newBuilder().setInput(Input.newBuilder()));
        for (List<Expression> projection : projections) {
            expand.addProjections(ExpandProjection.newBuilder().addAllExpressions(projection));
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setExpand(expand))
                .build()
                .toByteArray();
    }
}

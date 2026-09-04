/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.replicate;

import java.util.List;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.ReplicateRows;

/** Builds the native contract for Flink's internal set-operation row replicator. */
final class StreamFusionReplicateRowsPlan {
    private StreamFusionReplicateRowsPlan() {}

    static byte[] create(Expression repetition, List<Expression> values) {
        ReplicateRows replicate = ReplicateRows.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .setRepetition(repetition)
                .addAllValues(values)
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setReplicateRows(replicate))
                .build()
                .toByteArray();
    }
}

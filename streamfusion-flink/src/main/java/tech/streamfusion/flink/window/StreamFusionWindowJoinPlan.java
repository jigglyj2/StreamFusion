/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.window;

import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.proto.plan.v1.Field;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.Schema;
import tech.streamfusion.proto.plan.v1.WindowJoin;

/** Builds the versioned native Window Join state contract. */
final class StreamFusionWindowJoinPlan {
    private StreamFusionWindowJoinPlan() {}

    static byte[] create(
            RowType leftType,
            RowType rightType,
            int[] leftKeys,
            int[] rightKeys,
            int leftWindowEnd,
            int rightWindowEnd,
            String shiftTimeZone) {
        WindowJoin.Builder join = WindowJoin.newBuilder()
                .setLeftWindowEndIndex(leftWindowEnd)
                .setRightWindowEndIndex(rightWindowEnd)
                .setLeftSchema(schema(leftType))
                .setRightSchema(schema(rightType))
                .setShiftTimeZone(shiftTimeZone);
        for (int key : leftKeys) {
            join.addLeftKeyIndices(key);
        }
        for (int key : rightKeys) {
            join.addRightKeyIndices(key);
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setWindowJoin(join))
                .build()
                .toByteArray();
    }

    private static Schema schema(RowType type) {
        Schema.Builder schema = Schema.newBuilder();
        for (RowType.RowField field : type.getFields()) {
            schema.addFields(Field.newBuilder()
                    .setName(field.getName())
                    .setType(FlinkLogicalTypeProto.serialize(field.getType())));
        }
        return schema.build();
    }
}

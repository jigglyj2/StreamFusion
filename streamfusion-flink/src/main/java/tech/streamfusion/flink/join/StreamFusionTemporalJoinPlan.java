/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.join;

import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.proto.plan.v1.Field;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.RegularJoinType;
import tech.streamfusion.proto.plan.v1.Schema;
import tech.streamfusion.proto.plan.v1.TemporalJoin;
import tech.streamfusion.proto.plan.v1.TemporalJoinTimeMode;

/** Builds the versioned native temporal-table join state contract. */
final class StreamFusionTemporalJoinPlan {
    private StreamFusionTemporalJoinPlan() {}

    static byte[] create(
            RowType leftType,
            RowType rightType,
            int[] leftKeys,
            int[] rightKeys,
            boolean[] filterNulls,
            FlinkJoinType joinType,
            boolean processingTime,
            int leftTimeIndex,
            int rightTimeIndex,
            long minRetentionMillis,
            long maxRetentionMillis) {
        TemporalJoin.Builder join = TemporalJoin.newBuilder()
                .setLeftSchema(schema(leftType))
                .setRightSchema(schema(rightType))
                .setJoinType(
                        joinType == FlinkJoinType.LEFT
                                ? RegularJoinType.REGULAR_JOIN_TYPE_LEFT
                                : RegularJoinType.REGULAR_JOIN_TYPE_INNER)
                .setTimeMode(
                        processingTime
                                ? TemporalJoinTimeMode.TEMPORAL_JOIN_TIME_MODE_PROCESSING_TIME
                                : TemporalJoinTimeMode.TEMPORAL_JOIN_TIME_MODE_EVENT_TIME)
                .setLeftTimeIndex(leftTimeIndex < 0 ? 0 : leftTimeIndex)
                .setRightTimeIndex(rightTimeIndex < 0 ? 0 : rightTimeIndex)
                .setMinStateRetentionMillis(minRetentionMillis)
                .setMaxStateRetentionMillis(maxRetentionMillis);
        for (int key : leftKeys) {
            join.addLeftKeyIndices(key);
        }
        for (int key : rightKeys) {
            join.addRightKeyIndices(key);
        }
        for (boolean filter : filterNulls) {
            join.addFilterNulls(filter);
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setTemporalJoin(join))
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

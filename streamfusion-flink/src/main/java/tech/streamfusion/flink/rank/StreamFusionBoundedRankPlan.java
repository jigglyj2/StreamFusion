/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.rank;

import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.proto.plan.v1.BoundedRank;
import tech.streamfusion.proto.plan.v1.Field;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.Schema;

/** Builds the versioned protobuf contract for native bounded RANK. */
final class StreamFusionBoundedRankPlan {
    private StreamFusionBoundedRankPlan() {}

    static byte[] create(
            RowType inputType,
            RowType outputType,
            int[] partitionFields,
            int[] sortFields,
            long rankStart,
            long rankEnd,
            boolean outputRankNumber) {
        BoundedRank.Builder rank = BoundedRank.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .setInputSchema(schema(inputType))
                .setOutputSchema(schema(outputType))
                .setRankStart(rankStart)
                .setRankEnd(rankEnd)
                .setOutputRankNumber(outputRankNumber);
        for (int field : partitionFields) {
            rank.addPartitionKeyIndices(field);
        }
        for (int field : sortFields) {
            rank.addSortKeyIndices(field);
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setBoundedRank(rank))
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

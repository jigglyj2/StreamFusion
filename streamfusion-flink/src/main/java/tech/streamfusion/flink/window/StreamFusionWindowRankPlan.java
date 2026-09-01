/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.window;

import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.proto.plan.v1.Field;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.Schema;
import tech.streamfusion.proto.plan.v1.WindowRank;

/** Builds the versioned protobuf control contract for native Window Top-N. */
final class StreamFusionWindowRankPlan {
    private StreamFusionWindowRankPlan() {}

    static byte[] create(
            RowType inputType,
            int[] partitionKeys,
            SortSpec sortSpec,
            int windowEndIndex,
            long rankStart,
            long rankEnd,
            boolean outputRankNumber,
            String shiftTimeZone) {
        WindowRank.Builder rank = WindowRank.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .setWindowEndIndex(windowEndIndex)
                .setRankStart(rankStart)
                .setRankEnd(rankEnd)
                .setOutputRankNumber(outputRankNumber)
                .setInputChangelog(true)
                .setInputSchema(schema(inputType))
                .setShiftTimeZone(shiftTimeZone);
        for (int key : partitionKeys) {
            rank.addPartitionKeyIndices(key);
        }
        for (SortSpec.SortFieldSpec field : sortSpec.getFieldSpecs()) {
            rank.addSortKeyIndices(field.getFieldIndex());
            rank.addSortAscending(field.getIsAscendingOrder());
            rank.addSortNullsLast(field.getNullIsLast());
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setWindowRank(rank))
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

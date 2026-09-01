/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.window;

import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.proto.plan.v1.Field;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.Schema;
import tech.streamfusion.proto.plan.v1.WindowDeduplicate;

/** Builds the versioned protobuf contract for native window deduplication. */
final class StreamFusionWindowDeduplicatePlan {
    private StreamFusionWindowDeduplicatePlan() {}

    static byte[] create(
            RowType inputType,
            int[] partitionKeys,
            int orderIndex,
            int windowEndIndex,
            boolean keepLast,
            String shiftTimeZone) {
        WindowDeduplicate.Builder deduplicate = WindowDeduplicate.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .setOrderIndex(orderIndex)
                .setWindowEndIndex(windowEndIndex)
                .setKeepLast(keepLast)
                .setInputChangelog(true)
                .setInputSchema(schema(inputType))
                .setShiftTimeZone(shiftTimeZone);
        for (int key : partitionKeys) {
            deduplicate.addPartitionKeyIndices(key);
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setWindowDeduplicate(deduplicate))
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

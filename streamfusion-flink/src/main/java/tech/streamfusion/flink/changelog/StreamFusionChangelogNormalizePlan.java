/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.changelog;

import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.proto.plan.v1.ChangelogNormalize;
import tech.streamfusion.proto.plan.v1.Field;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.Schema;

/** Builds the versioned protobuf contract for native changelog normalization. */
final class StreamFusionChangelogNormalizePlan {
    private StreamFusionChangelogNormalizePlan() {}

    static byte[] create(
            RowType inputType, int[] uniqueKeys, boolean generateUpdateBefore, long stateTtlMillis, boolean hasFilter) {
        ChangelogNormalize.Builder normalize = ChangelogNormalize.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .setGenerateUpdateBefore(generateUpdateBefore)
                .setInputSchema(schema(inputType))
                .setStateTtlMillis(stateTtlMillis)
                .setHasFilter(hasFilter);
        for (int key : uniqueKeys) {
            normalize.addKeyIndices(key);
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setChangelogNormalize(normalize))
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

/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sort;

import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.proto.plan.v1.Field;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.Schema;
import tech.streamfusion.proto.plan.v1.TemporalSort;

/** Builds the versioned protobuf contract for native temporal sort. */
final class StreamFusionTemporalSortPlan {
    private StreamFusionTemporalSortPlan() {}

    static byte[] create(RowType inputType, SortSpec sortSpec, boolean processingTime) {
        SortSpec.SortFieldSpec time = sortSpec.getFieldSpec(0);
        TemporalSort.Builder sort = TemporalSort.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .setInputSchema(schema(inputType))
                .setTimeIndex(time.getFieldIndex())
                .setProcessingTime(processingTime);
        for (int index = 1; index < sortSpec.getFieldSize(); index++) {
            SortSpec.SortFieldSpec field = sortSpec.getFieldSpec(index);
            sort.addSecondaryKeyIndices(field.getFieldIndex());
            sort.addSecondaryAscending(field.getIsAscendingOrder());
            sort.addSecondaryNullsLast(field.getNullIsLast());
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setTemporalSort(sort))
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

/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sort;

import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.proto.plan.v1.BoundedSort;
import tech.streamfusion.proto.plan.v1.Field;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.Schema;

/** Builds the versioned protobuf contract for native bounded full sort. */
final class StreamFusionBoundedSortPlan {
    private StreamFusionBoundedSortPlan() {}

    static byte[] create(RowType inputType, SortSpec sortSpec) {
        BoundedSort.Builder sort = BoundedSort.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .setInputSchema(schema(inputType));
        for (int index = 0; index < sortSpec.getFieldSize(); index++) {
            SortSpec.SortFieldSpec field = sortSpec.getFieldSpec(index);
            sort.addSortKeyIndices(field.getFieldIndex());
            sort.addSortAscending(field.getIsAscendingOrder());
            sort.addSortNullsLast(field.getNullIsLast());
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setBoundedSort(sort))
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

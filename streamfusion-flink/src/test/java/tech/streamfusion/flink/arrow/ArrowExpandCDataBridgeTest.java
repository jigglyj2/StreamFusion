/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.EmptyType;
import tech.streamfusion.proto.plan.v1.Expand;
import tech.streamfusion.proto.plan.v1.ExpandProjection;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.InputReference;
import tech.streamfusion.proto.plan.v1.IntegerLiteral;
import tech.streamfusion.proto.plan.v1.LogicalType;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

class ArrowExpandCDataBridgeTest {
    @Test
    void expandsEveryInputAndPreservesItsSelectionOrdinal() {
        RowType inputType = RowType.of(new IntType(false));
        RowType outputType = RowType.of(new IntType(false), new IntType(false));
        List<RowData> rows = List.of(GenericRowData.of(4), GenericRowData.of(9));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, inputType, allocator);
                NativeCalcResult result =
                        ArrowCDataBridge.executeWithSelection(expandPlan(), input, outputType, allocator)) {
            assertThat(result.batch().size()).isEqualTo(4);
            assertRow(result.batch().rowView(0), 4, 10);
            assertRow(result.batch().rowView(1), 4, 20);
            assertRow(result.batch().rowView(2), 9, 10);
            assertRow(result.batch().rowView(3), 9, 20);
            assertThat(List.of(result.inputRow(0), result.inputRow(1), result.inputRow(2), result.inputRow(3)))
                    .containsExactly(0, 0, 1, 1);
        }
    }

    private static void assertRow(RowData row, int value, int groupingId) {
        assertThat(row.getInt(0)).isEqualTo(value);
        assertThat(row.getInt(1)).isEqualTo(groupingId);
    }

    private static byte[] expandPlan() {
        LogicalType integer = LogicalType.newBuilder()
                .setNullable(false)
                .setInteger(EmptyType.getDefaultInstance())
                .build();
        Expression value = Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(0).setType(integer))
                .build();
        Expand.Builder expand =
                Expand.newBuilder().setInput(Operator.newBuilder().setInput(Input.newBuilder()));
        expand.addProjections(projection(value, 10));
        expand.addProjections(projection(value, 20));
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setExpand(expand))
                .build()
                .toByteArray();
    }

    private static ExpandProjection projection(Expression value, int groupingId) {
        return ExpandProjection.newBuilder()
                .addExpressions(value)
                .addExpressions(Expression.newBuilder()
                        .setIntegerLiteral(IntegerLiteral.newBuilder().setValue(groupingId)))
                .build();
    }
}

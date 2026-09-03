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

import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.EmptyType;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.Field;
import tech.streamfusion.proto.plan.v1.IntegerLiteral;
import tech.streamfusion.proto.plan.v1.LogicalType;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.NullLiteral;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.Schema;
import tech.streamfusion.proto.plan.v1.StringLiteral;
import tech.streamfusion.proto.plan.v1.Values;
import tech.streamfusion.proto.plan.v1.ValuesRow;

class ArrowValuesCDataBridgeTest {
    @Test
    void importsNullableValuesWithoutAnInputArrowBatch() {
        RowType outputType = RowType.of(new IntType(), new VarCharType());

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch batch = ArrowValuesCDataBridge.execute(plan(false), outputType, allocator)) {
            assertThat(batch.size()).isEqualTo(2);
            assertThat(batch.rowView(0).getInt(0)).isEqualTo(7);
            assertThat(batch.rowView(0).getString(1).toString()).isEqualTo("seven");
            assertThat(batch.rowView(1).isNullAt(0)).isTrue();
            assertThat(batch.rowView(1).getString(1).toString()).isEqualTo("missing");
        }
    }

    @Test
    void importsAnEmptyValuesBatchWithItsDeclaredSchema() {
        RowType outputType = RowType.of(new IntType(), new VarCharType());

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch batch = ArrowValuesCDataBridge.execute(plan(true), outputType, allocator)) {
            assertThat(batch.size()).isZero();
            assertThat(batch.root().getFieldVectors()).hasSize(2);
        }
    }

    @Test
    void retainsAllocatorForZeroColumnValuesBatch() {
        RowType outputType = RowType.of();
        Values values = Values.newBuilder()
                .setSchema(Schema.getDefaultInstance())
                .addRows(ValuesRow.getDefaultInstance())
                .build();
        byte[] plan = NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setValues(values))
                .build()
                .toByteArray();

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch batch = ArrowValuesCDataBridge.execute(plan, outputType, allocator)) {
            assertThat(batch.size()).isEqualTo(1);
            assertThat(batch.allocator()).isSameAs(allocator);
        }
    }

    private static byte[] plan(boolean empty) {
        LogicalType integer = LogicalType.newBuilder()
                .setNullable(true)
                .setInteger(EmptyType.getDefaultInstance())
                .build();
        LogicalType string = LogicalType.newBuilder()
                .setNullable(true)
                .setVarchar(EmptyType.getDefaultInstance())
                .build();
        Values.Builder values = Values.newBuilder()
                .setSchema(Schema.newBuilder()
                        .addFields(Field.newBuilder().setName("id").setType(integer))
                        .addFields(Field.newBuilder().setName("name").setType(string)));
        if (!empty) {
            values.addRows(ValuesRow.newBuilder()
                    .addValues(Expression.newBuilder()
                            .setIntegerLiteral(IntegerLiteral.newBuilder().setValue(7)))
                    .addValues(Expression.newBuilder()
                            .setStringLiteral(StringLiteral.newBuilder().setValue("seven"))));
            values.addRows(ValuesRow.newBuilder()
                    .addValues(Expression.newBuilder()
                            .setNullLiteral(NullLiteral.newBuilder().setType(integer)))
                    .addValues(Expression.newBuilder()
                            .setStringLiteral(StringLiteral.newBuilder().setValue("missing"))));
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setValues(values))
                .build()
                .toByteArray();
    }
}

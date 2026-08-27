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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.Calc;
import tech.streamfusion.proto.plan.v1.Comparison;
import tech.streamfusion.proto.plan.v1.ComparisonOperator;
import tech.streamfusion.proto.plan.v1.EmptyType;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.GreaterThanOrEqual;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.InputReference;
import tech.streamfusion.proto.plan.v1.IntegerLiteral;
import tech.streamfusion.proto.plan.v1.LogicalType;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

class ArrowCDataBridgeTest {
    @Test
    void transfersRebasedNestedBatchThroughDataFusionAndBackToRowData() {
        RowType inputType = RowType.of(
                new IntType(false), new VarCharType(), new DecimalType(10, 2), new ArrayType(new VarCharType()));
        RowType outputType = RowType.of(new IntType(false));
        List<RowData> rows = List.of(row(1, "skip", 100, "x"), row(2, "two", 200, "a"), row(3, "three", 300, "b"));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch original = ArrowRowDataBatch.transpose(rows, inputType, allocator);
                VectorSchemaRoot rebasedRoot = ArrowBatchRebaser.rebase(original.root(), 1, 2, allocator);
                ArrowRowDataBatch rebased = ArrowRowDataBatch.wrap(rebasedRoot, inputType);
                ArrowRowDataBatch output = ArrowCDataBridge.execute(plan(0, 2), rebased, outputType, allocator)) {
            assertThat(output.size()).isEqualTo(2);
            assertThat(output.rowView(0).getInt(0)).isEqualTo(2);
            assertThat(output.rowView(1).getInt(0)).isEqualTo(3);
        }
    }

    @Test
    void importsAnEmptyNativeResultWithoutLeakingItsSchemaOrBuffers() {
        RowType rowType = RowType.of(new IntType(false));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input =
                        ArrowRowDataBatch.transpose(List.of(GenericRowData.of(1)), rowType, allocator);
                ArrowRowDataBatch output = ArrowCDataBridge.execute(plan(0, 2), input, rowType, allocator)) {
            assertThat(output.size()).isZero();
        }
    }

    @Test
    void executesGenericEqualityThroughTheCDataBoundary() {
        RowType rowType = RowType.of(new IntType(false));
        List<RowData> rows = List.of(GenericRowData.of(1), GenericRowData.of(2), GenericRowData.of(3));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, rowType, allocator);
                ArrowRowDataBatch output = ArrowCDataBridge.execute(
                        comparisonPlan(ComparisonOperator.COMPARISON_OPERATOR_EQUAL), input, rowType, allocator)) {
            assertThat(output.size()).isOne();
            assertThat(output.rowView(0).getInt(0)).isEqualTo(2);
        }
    }

    @Test
    void projectsAndReordersCompatibleColumnsThroughDataFusion() {
        RowType inputType = RowType.of(
                new IntType(false), new VarCharType(), new DecimalType(10, 2), new ArrayType(new VarCharType()));
        RowType outputType = RowType.of(
                new ArrayType(new VarCharType()), new DecimalType(10, 2), new VarCharType(), new IntType(false));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input =
                        ArrowRowDataBatch.transpose(List.of(row(7, "seven", 725, "nested")), inputType, allocator);
                ArrowRowDataBatch output =
                        ArrowCDataBridge.execute(projectionPlan(3, 2, 1, 0), input, outputType, allocator)) {
            assertThat(output.size()).isOne();
            RowData row = output.rowView(0);
            assertThat(row.getArray(0).getString(0).toString()).isEqualTo("nested");
            assertThat(row.getDecimal(1, 10, 2).toBigDecimal()).isEqualByComparingTo("7.25");
            assertThat(row.getString(2).toString()).isEqualTo("seven");
            assertThat(row.getInt(3)).isEqualTo(7);
        }
    }

    private static GenericRowData row(int id, String text, long decimal, String arrayValue) {
        return GenericRowData.of(
                id,
                StringData.fromString(text),
                DecimalData.fromUnscaledLong(decimal, 10, 2),
                new GenericArrayData(new StringData[] {StringData.fromString(arrayValue), null}));
    }

    private static byte[] plan(int inputIndex, int minimum) {
        LogicalType integer = LogicalType.newBuilder()
                .setNullable(false)
                .setInteger(EmptyType.getDefaultInstance())
                .build();
        Expression reference = Expression.newBuilder()
                .setInputReference(
                        InputReference.newBuilder().setIndex(inputIndex).setType(integer))
                .build();
        Calc calc = Calc.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addProjections(reference)
                .setCondition(Expression.newBuilder()
                        .setGreaterThanOrEqual(GreaterThanOrEqual.newBuilder()
                                .setLeft(reference)
                                .setRight(Expression.newBuilder()
                                        .setIntegerLiteral(
                                                IntegerLiteral.newBuilder().setValue(minimum)))))
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    private static byte[] comparisonPlan(ComparisonOperator operator) {
        LogicalType integer = LogicalType.newBuilder()
                .setNullable(false)
                .setInteger(EmptyType.getDefaultInstance())
                .build();
        Expression reference = Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(0).setType(integer))
                .build();
        Expression literal = Expression.newBuilder()
                .setIntegerLiteral(IntegerLiteral.newBuilder().setValue(2))
                .build();
        Calc calc = Calc.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addProjections(reference)
                .setCondition(Expression.newBuilder()
                        .setComparison(Comparison.newBuilder()
                                .setLeft(reference)
                                .setRight(literal)
                                .setOperator(operator)))
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    private static byte[] projectionPlan(int... inputIndexes) {
        Calc.Builder calc = Calc.newBuilder().setInput(Operator.newBuilder().setInput(Input.newBuilder()));
        for (int inputIndex : inputIndexes) {
            calc.addProjections(Expression.newBuilder()
                    .setInputReference(InputReference.newBuilder().setIndex(inputIndex)));
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }
}

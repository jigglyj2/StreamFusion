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

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.proto.plan.v1.Arithmetic;
import tech.streamfusion.proto.plan.v1.ArithmeticOperator;
import tech.streamfusion.proto.plan.v1.Calc;
import tech.streamfusion.proto.plan.v1.Cast;
import tech.streamfusion.proto.plan.v1.CastKind;
import tech.streamfusion.proto.plan.v1.Comparison;
import tech.streamfusion.proto.plan.v1.ComparisonOperator;
import tech.streamfusion.proto.plan.v1.DecimalLiteral;
import tech.streamfusion.proto.plan.v1.DoubleLiteral;
import tech.streamfusion.proto.plan.v1.EmptyType;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.FloatLiteral;
import tech.streamfusion.proto.plan.v1.GreaterThanOrEqual;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.InputReference;
import tech.streamfusion.proto.plan.v1.IntegerLiteral;
import tech.streamfusion.proto.plan.v1.LogicalType;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.NullCheck;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.TimeLiteral;
import tech.streamfusion.proto.plan.v1.TimestampLiteral;
import tech.streamfusion.proto.plan.v1.TruthTest;
import tech.streamfusion.proto.plan.v1.TruthTestOperator;

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
    void truthTestsReturnNonNullResultsForNullableBooleans() {
        RowType inputType = RowType.of(new BooleanType(true));
        RowType outputType = RowType.of(
                new BooleanType(false), new BooleanType(false), new BooleanType(false), new BooleanType(false));
        List<RowData> rows =
                List.of(GenericRowData.of(true), GenericRowData.of(false), GenericRowData.of((Object) null));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, inputType, allocator);
                ArrowRowDataBatch output = ArrowCDataBridge.execute(truthTestPlan(), input, outputType, allocator)) {
            assertThat(output.rowView(0).getBoolean(0)).isTrue();
            assertThat(output.rowView(0).getBoolean(1)).isFalse();
            assertThat(output.rowView(1).getBoolean(0)).isFalse();
            assertThat(output.rowView(1).getBoolean(1)).isTrue();
            assertThat(output.rowView(2).getBoolean(0)).isFalse();
            assertThat(output.rowView(2).getBoolean(1)).isFalse();
            assertThat(output.rowView(2).getBoolean(2)).isTrue();
            assertThat(output.rowView(2).getBoolean(3)).isTrue();
        }
    }

    @Test
    void nullSafeComparisonsReturnNonNullResultsForNullableIntegers() {
        RowType inputType = RowType.of(new IntType(true));
        RowType outputType = RowType.of(new BooleanType(false), new BooleanType(false));
        List<RowData> rows = List.of(GenericRowData.of((Object) null), GenericRowData.of(2), GenericRowData.of(3));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, inputType, allocator);
                ArrowRowDataBatch output =
                        ArrowCDataBridge.execute(nullSafeComparisonPlan(), input, outputType, allocator)) {
            assertThat(output.rowView(0).getBoolean(0)).isTrue();
            assertThat(output.rowView(0).getBoolean(1)).isFalse();
            assertThat(output.rowView(1).getBoolean(0)).isFalse();
            assertThat(output.rowView(1).getBoolean(1)).isTrue();
            assertThat(output.rowView(2).getBoolean(0)).isTrue();
            assertThat(output.rowView(2).getBoolean(1)).isFalse();
        }
    }

    @Test
    void widensEverySignedIntegerTypeWithoutChangingValues() {
        RowType inputType = RowType.of(new TinyIntType(false), new SmallIntType(false), new IntType(false));
        RowType outputType = RowType.of(
                new SmallIntType(false),
                new IntType(false),
                new BigIntType(false),
                new IntType(false),
                new BigIntType(false),
                new BigIntType(false));
        List<RowData> rows = List.of(
                GenericRowData.of((byte) -128, (short) -32768, Integer.MIN_VALUE),
                GenericRowData.of((byte) 0, (short) 0, 0),
                GenericRowData.of((byte) 127, (short) 32767, Integer.MAX_VALUE));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, inputType, allocator);
                ArrowRowDataBatch output =
                        ArrowCDataBridge.execute(integerWideningPlan(), input, outputType, allocator)) {
            assertThat(output.rowView(0).getShort(0)).isEqualTo((short) -128);
            assertThat(output.rowView(0).getInt(1)).isEqualTo(-128);
            assertThat(output.rowView(0).getLong(2)).isEqualTo(-128L);
            assertThat(output.rowView(0).getInt(3)).isEqualTo(-32768);
            assertThat(output.rowView(0).getLong(4)).isEqualTo(-32768L);
            assertThat(output.rowView(0).getLong(5)).isEqualTo(Integer.MIN_VALUE);
            assertThat(output.rowView(2).getLong(2)).isEqualTo(127L);
            assertThat(output.rowView(2).getLong(4)).isEqualTo(32767L);
            assertThat(output.rowView(2).getLong(5)).isEqualTo(Integer.MAX_VALUE);
        }
    }

    @Test
    void widensExactlyRepresentableIntegersToFloatingPointWithoutRounding() {
        RowType inputType = RowType.of(new TinyIntType(false), new SmallIntType(false), new IntType(false));
        RowType outputType = RowType.of(
                new FloatType(false),
                new DoubleType(false),
                new FloatType(false),
                new DoubleType(false),
                new DoubleType(false));
        List<RowData> rows = List.of(
                GenericRowData.of((byte) -128, (short) -32768, Integer.MIN_VALUE),
                GenericRowData.of((byte) 0, (short) 0, 0),
                GenericRowData.of((byte) 127, (short) 32767, Integer.MAX_VALUE));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, inputType, allocator);
                ArrowRowDataBatch output =
                        ArrowCDataBridge.execute(integerToFloatingPlan(), input, outputType, allocator)) {
            assertThat(output.rowView(0).getFloat(0)).isEqualTo(-128.0f);
            assertThat(output.rowView(0).getDouble(1)).isEqualTo(-128.0d);
            assertThat(output.rowView(0).getFloat(2)).isEqualTo(-32768.0f);
            assertThat(output.rowView(0).getDouble(3)).isEqualTo(-32768.0d);
            assertThat(output.rowView(0).getDouble(4)).isEqualTo((double) Integer.MIN_VALUE);
            assertThat(output.rowView(2).getFloat(0)).isEqualTo(127.0f);
            assertThat(output.rowView(2).getDouble(1)).isEqualTo(127.0d);
            assertThat(output.rowView(2).getFloat(2)).isEqualTo(32767.0f);
            assertThat(output.rowView(2).getDouble(3)).isEqualTo(32767.0d);
            assertThat(output.rowView(2).getDouble(4)).isEqualTo((double) Integer.MAX_VALUE);
        }
    }

    @Test
    void widensFloatToDoubleWithJavaCastParity() {
        RowType inputType = RowType.of(new FloatType(false));
        RowType outputType = RowType.of(new DoubleType(false));
        List<Float> values = List.of(
                Float.NEGATIVE_INFINITY,
                -Float.MAX_VALUE,
                -0.0f,
                0.0f,
                Float.MIN_VALUE,
                Float.MAX_VALUE,
                Float.POSITIVE_INFINITY,
                Float.NaN);
        List<RowData> rows = values.stream()
                .map(value -> (RowData) GenericRowData.of(value))
                .collect(java.util.stream.Collectors.toList());

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, inputType, allocator);
                ArrowRowDataBatch output =
                        ArrowCDataBridge.execute(floatToDoublePlan(), input, outputType, allocator)) {
            for (int index = 0; index < values.size(); index++) {
                assertThat(Double.doubleToLongBits(output.rowView(index).getDouble(0)))
                        .isEqualTo(Double.doubleToLongBits((double) values.get(index)));
            }
        }
    }

    @Test
    void narrowsIntegerWithFlinkOverflowParity() {
        RowType inputType = RowType.of(new IntType(true));
        RowType outputType = RowType.of(new TinyIntType(true), new SmallIntType(true));
        List<Integer> values = List.of(Integer.MIN_VALUE, -32769, -32768, -1, 0, 32767, 32768, Integer.MAX_VALUE);
        List<RowData> rows = new ArrayList<>(values.stream()
                .map(value -> (RowData) GenericRowData.of(value))
                .collect(java.util.stream.Collectors.toList()));
        rows.add(GenericRowData.of((Object) null));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, inputType, allocator);
                ArrowRowDataBatch output =
                        ArrowCDataBridge.execute(integerNarrowingPlan(), input, outputType, allocator)) {
            for (int index = 0; index < values.size(); index++) {
                assertThat(output.rowView(index).getByte(0)).isEqualTo((byte) (int) values.get(index));
                assertThat(output.rowView(index).getShort(1)).isEqualTo((short) (int) values.get(index));
            }
            assertThat(output.rowView(values.size()).isNullAt(0)).isTrue();
            assertThat(output.rowView(values.size()).isNullAt(1)).isTrue();
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

    @Test
    void filtersNullsThroughDataFusion() {
        RowType rowType = RowType.of(new IntType());
        List<RowData> rows = List.of(GenericRowData.of(1), GenericRowData.of((Object) null), GenericRowData.of(3));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, rowType, allocator);
                ArrowRowDataBatch output = ArrowCDataBridge.execute(nullCheckPlan(false), input, rowType, allocator)) {
            assertThat(output.size()).isOne();
            assertThat(output.rowView(0).isNullAt(0)).isTrue();
        }
    }

    @ParameterizedTest(name = "TIME({0})")
    @ValueSource(ints = {0, 3, 6, 9})
    void filtersEveryTimePrecisionThroughDataFusion(int precision) {
        RowType rowType = RowType.of(new TimeType(false, precision));
        List<RowData> rows =
                List.of(GenericRowData.of(0), GenericRowData.of(43_200_000), GenericRowData.of(86_399_000));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, rowType, allocator);
                ArrowRowDataBatch output = ArrowCDataBridge.execute(timePlan(precision), input, rowType, allocator)) {
            assertThat(output.size()).isEqualTo(2);
            assertThat(output.rowView(0).getInt(0)).isEqualTo(43_200_000);
            assertThat(output.rowView(1).getInt(0)).isEqualTo(86_399_000);
        }
    }

    @ParameterizedTest(name = "TIMESTAMP({0})")
    @ValueSource(ints = {0, 3, 6, 9})
    void filtersEveryTimestampPrecisionThroughDataFusion(int precision) {
        RowType rowType = RowType.of(new TimestampType(false, precision));
        TimestampData beforeEpoch = TimestampData.fromEpochMillis(-1_000);
        TimestampData noon = TimestampData.fromEpochMillis(43_200_000, precision > 3 ? 456_000 : 0);
        TimestampData evening = TimestampData.fromEpochMillis(64_800_000, precision > 3 ? 789_000 : 0);
        List<RowData> rows =
                List.of(GenericRowData.of(beforeEpoch), GenericRowData.of(noon), GenericRowData.of(evening));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, rowType, allocator);
                ArrowRowDataBatch output =
                        ArrowCDataBridge.execute(timestampPlan(precision, noon), input, rowType, allocator)) {
            assertThat(output.size()).isEqualTo(2);
            assertThat(output.rowView(0).getTimestamp(0, precision)).isEqualTo(noon);
            assertThat(output.rowView(1).getTimestamp(0, precision)).isEqualTo(evening);
        }
    }

    @Test
    void importsPreEpochNanosecondTimestampWithNonNegativeRemainder() {
        RowType rowType = RowType.of(new TimestampType(false, 9));
        TimestampData oneNanosecondBeforeEpoch = TimestampData.fromEpochMillis(-1, 999_999);

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(
                        List.of(GenericRowData.of(oneNanosecondBeforeEpoch)), rowType, allocator);
                ArrowRowDataBatch output = ArrowCDataBridge.execute(projectionPlan(0), input, rowType, allocator)) {
            assertThat(output.rowView(0).getTimestamp(0, 9)).isEqualTo(oneNanosecondBeforeEpoch);
        }
    }

    @ParameterizedTest(name = "DECIMAL({0}, {1})")
    @org.junit.jupiter.params.provider.CsvSource({"10, 2, 12.34", "38, 9, 12345678901234567890.123456789"})
    void filtersCompactAndWideDecimalsThroughDataFusion(int precision, int scale, String literalText) {
        DecimalType decimalType = new DecimalType(false, precision, scale);
        RowType rowType = RowType.of(decimalType);
        java.math.BigDecimal literal = new java.math.BigDecimal(literalText);
        List<RowData> rows = List.of(
                GenericRowData.of(
                        DecimalData.fromBigDecimal(literal.subtract(java.math.BigDecimal.ONE), precision, scale)),
                GenericRowData.of(DecimalData.fromBigDecimal(literal, precision, scale)),
                GenericRowData.of(DecimalData.fromBigDecimal(literal.add(java.math.BigDecimal.ONE), precision, scale)));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, rowType, allocator);
                ArrowRowDataBatch output =
                        ArrowCDataBridge.execute(decimalPlan(precision, scale, literal), input, rowType, allocator)) {
            assertThat(output.size()).isEqualTo(2);
            assertThat(output.rowView(0).getDecimal(0, precision, scale).toBigDecimal())
                    .isEqualByComparingTo(literal);
            assertThat(output.rowView(1).getDecimal(0, precision, scale).toBigDecimal())
                    .isEqualByComparingTo(literal.add(java.math.BigDecimal.ONE));
        }
    }

    @Test
    void computesFloatArithmeticThroughDataFusion() {
        RowType rowType = RowType.of(new FloatType(false));
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(
                        List.of(GenericRowData.of(-2.5F), GenericRowData.of(0.0F), GenericRowData.of(3.25F)),
                        rowType,
                        allocator);
                ArrowRowDataBatch output =
                        ArrowCDataBridge.execute(floatingPointArithmeticPlan(true), input, rowType, allocator)) {
            assertThat(output.rowView(0).getFloat(0)).isEqualTo(-1.0F);
            assertThat(output.rowView(1).getFloat(0)).isEqualTo(1.5F);
            assertThat(output.rowView(2).getFloat(0)).isEqualTo(4.75F);
        }
    }

    @Test
    void computesDoubleArithmeticThroughDataFusion() {
        RowType rowType = RowType.of(new DoubleType(false));
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(
                        List.of(
                                GenericRowData.of(-2.5D),
                                GenericRowData.of(0.0D),
                                GenericRowData.of(3.25D),
                                GenericRowData.of(Double.NaN),
                                GenericRowData.of(Double.POSITIVE_INFINITY),
                                GenericRowData.of(Double.NEGATIVE_INFINITY),
                                GenericRowData.of(-0.0D)),
                        rowType,
                        allocator);
                ArrowRowDataBatch output =
                        ArrowCDataBridge.execute(floatingPointArithmeticPlan(false), input, rowType, allocator)) {
            assertThat(output.rowView(0).getDouble(0)).isEqualTo(-1.0D);
            assertThat(output.rowView(1).getDouble(0)).isEqualTo(1.5D);
            assertThat(output.rowView(2).getDouble(0)).isEqualTo(4.75D);
            assertThat(output.rowView(3).getDouble(0)).isNaN();
            assertThat(output.rowView(4).getDouble(0)).isEqualTo(Double.POSITIVE_INFINITY);
            assertThat(output.rowView(5).getDouble(0)).isEqualTo(Double.NEGATIVE_INFINITY);
            assertThat(output.rowView(6).getDouble(0)).isEqualTo(1.5D);
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

    private static byte[] truthTestPlan() {
        Expression reference = Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(0))
                .build();
        Calc.Builder calc = Calc.newBuilder().setInput(Operator.newBuilder().setInput(Input.newBuilder()));
        for (TruthTestOperator operator : List.of(
                TruthTestOperator.TRUTH_TEST_OPERATOR_IS_TRUE,
                TruthTestOperator.TRUTH_TEST_OPERATOR_IS_FALSE,
                TruthTestOperator.TRUTH_TEST_OPERATOR_IS_NOT_TRUE,
                TruthTestOperator.TRUTH_TEST_OPERATOR_IS_NOT_FALSE)) {
            calc.addProjections(Expression.newBuilder()
                    .setTruthTest(TruthTest.newBuilder().setOperand(reference).setOperator(operator)));
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    private static byte[] nullSafeComparisonPlan() {
        Expression reference = Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(0))
                .build();
        Expression literal = Expression.newBuilder()
                .setIntegerLiteral(IntegerLiteral.newBuilder().setValue(2))
                .build();
        Calc.Builder calc = Calc.newBuilder().setInput(Operator.newBuilder().setInput(Input.newBuilder()));
        for (ComparisonOperator operator : List.of(
                ComparisonOperator.COMPARISON_OPERATOR_IS_DISTINCT_FROM,
                ComparisonOperator.COMPARISON_OPERATOR_IS_NOT_DISTINCT_FROM)) {
            calc.addProjections(Expression.newBuilder()
                    .setComparison(Comparison.newBuilder()
                            .setLeft(reference)
                            .setRight(literal)
                            .setOperator(operator)));
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    private static byte[] integerWideningPlan() {
        Calc.Builder calc = Calc.newBuilder().setInput(Operator.newBuilder().setInput(Input.newBuilder()));
        addCast(
                calc,
                0,
                LogicalType.newBuilder().setSmallint(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_TINYINT_TO_SMALLINT);
        addCast(
                calc,
                0,
                LogicalType.newBuilder().setInteger(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_TINYINT_TO_INTEGER);
        addCast(
                calc,
                0,
                LogicalType.newBuilder().setBigint(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_TINYINT_TO_BIGINT);
        addCast(
                calc,
                1,
                LogicalType.newBuilder().setInteger(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_SMALLINT_TO_INTEGER);
        addCast(
                calc,
                1,
                LogicalType.newBuilder().setBigint(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_SMALLINT_TO_BIGINT);
        addCast(
                calc,
                2,
                LogicalType.newBuilder().setBigint(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_INTEGER_TO_BIGINT);
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    private static byte[] integerToFloatingPlan() {
        Calc.Builder calc = Calc.newBuilder().setInput(Operator.newBuilder().setInput(Input.newBuilder()));
        addCast(
                calc,
                0,
                LogicalType.newBuilder().setFloat(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_TINYINT_TO_FLOAT);
        addCast(
                calc,
                0,
                LogicalType.newBuilder().setDouble(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_TINYINT_TO_DOUBLE);
        addCast(
                calc,
                1,
                LogicalType.newBuilder().setFloat(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_SMALLINT_TO_FLOAT);
        addCast(
                calc,
                1,
                LogicalType.newBuilder().setDouble(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_SMALLINT_TO_DOUBLE);
        addCast(
                calc,
                2,
                LogicalType.newBuilder().setDouble(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_INTEGER_TO_DOUBLE);
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    private static byte[] floatToDoublePlan() {
        Calc.Builder calc = Calc.newBuilder().setInput(Operator.newBuilder().setInput(Input.newBuilder()));
        addCast(
                calc,
                0,
                LogicalType.newBuilder().setDouble(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_FLOAT_TO_DOUBLE);
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    private static byte[] integerNarrowingPlan() {
        Calc.Builder calc = Calc.newBuilder().setInput(Operator.newBuilder().setInput(Input.newBuilder()));
        addCast(
                calc,
                0,
                LogicalType.newBuilder().setTinyint(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_INTEGER_TO_TINYINT);
        addCast(
                calc,
                0,
                LogicalType.newBuilder().setSmallint(EmptyType.getDefaultInstance()),
                CastKind.CAST_KIND_INTEGER_TO_SMALLINT);
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    private static void addCast(Calc.Builder calc, int inputIndex, LogicalType.Builder targetType, CastKind kind) {
        calc.addProjections(Expression.newBuilder()
                .setCast(Cast.newBuilder()
                        .setOperand(Expression.newBuilder()
                                .setInputReference(InputReference.newBuilder().setIndex(inputIndex)))
                        .setTargetType(targetType.setNullable(false))
                        .setKind(kind)));
    }

    private static byte[] nullCheckPlan(boolean negated) {
        Expression reference = Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(0))
                .build();
        Calc calc = Calc.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addProjections(reference)
                .setCondition(Expression.newBuilder()
                        .setNullCheck(
                                NullCheck.newBuilder().setOperand(reference).setNegated(negated)))
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    private static byte[] timePlan(int precision) {
        Expression reference = Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(0))
                .build();
        Expression noon = Expression.newBuilder()
                .setTimeLiteral(
                        TimeLiteral.newBuilder().setMillisecondOfDay(43_200_000).setPrecision(precision))
                .build();
        Calc calc = Calc.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addProjections(reference)
                .setCondition(Expression.newBuilder()
                        .setComparison(Comparison.newBuilder()
                                .setLeft(reference)
                                .setRight(noon)
                                .setOperator(ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL)))
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    private static byte[] timestampPlan(int precision, TimestampData literal) {
        Expression reference = Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(0))
                .build();
        Expression timestamp = Expression.newBuilder()
                .setTimestampLiteral(TimestampLiteral.newBuilder()
                        .setEpochMillisecond(literal.getMillisecond())
                        .setNanoOfMillisecond(literal.getNanoOfMillisecond())
                        .setPrecision(precision))
                .build();
        Calc calc = Calc.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addProjections(reference)
                .setCondition(Expression.newBuilder()
                        .setComparison(Comparison.newBuilder()
                                .setLeft(reference)
                                .setRight(timestamp)
                                .setOperator(ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL)))
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    private static byte[] decimalPlan(int precision, int scale, java.math.BigDecimal literal) {
        Expression reference = Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(0))
                .build();
        Expression decimal = Expression.newBuilder()
                .setDecimalLiteral(DecimalLiteral.newBuilder()
                        .setUnscaledValue(literal.unscaledValue().toString())
                        .setPrecision(precision)
                        .setScale(scale))
                .build();
        Calc calc = Calc.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addProjections(reference)
                .setCondition(Expression.newBuilder()
                        .setComparison(Comparison.newBuilder()
                                .setLeft(reference)
                                .setRight(decimal)
                                .setOperator(ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL)))
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }

    private static byte[] floatingPointArithmeticPlan(boolean singlePrecision) {
        Expression reference = Expression.newBuilder()
                .setInputReference(InputReference.newBuilder().setIndex(0))
                .build();
        Expression literal = singlePrecision
                ? Expression.newBuilder()
                        .setFloatLiteral(FloatLiteral.newBuilder().setValue(1.5F))
                        .build()
                : Expression.newBuilder()
                        .setDoubleLiteral(DoubleLiteral.newBuilder().setValue(1.5D))
                        .build();
        Expression addition = Expression.newBuilder()
                .setArithmetic(Arithmetic.newBuilder()
                        .setLeft(reference)
                        .setRight(literal)
                        .setOperator(ArithmeticOperator.ARITHMETIC_OPERATOR_ADD))
                .build();
        Calc calc = Calc.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addProjections(addition)
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setCalc(calc))
                .build()
                .toByteArray();
    }
}

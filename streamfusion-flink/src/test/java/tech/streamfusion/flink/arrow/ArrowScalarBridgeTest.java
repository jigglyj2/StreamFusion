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
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.proto.plan.v1.ComparisonOperator;

class ArrowScalarBridgeTest extends ArrowCDataBridgeTestSupport {
    @Test
    void importsAnEmptyNativeResultWithoutLeakingItsSchemaOrBuffers() {
        RowType rowType = RowType.of(new IntType(false));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input =
                        ArrowRowDataBatch.transpose(List.of(GenericRowData.of(1)), rowType, allocator);
                ArrowRowDataBatch output = ArrowCDataBridge.execute(
                        plan(0, 2),
                        input,
                        rowType,
                        allocator,
                        tech.streamfusion.flink.TestingNativeMemoryManager.create())) {
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
                        comparisonPlan(ComparisonOperator.COMPARISON_OPERATOR_EQUAL),
                        input,
                        rowType,
                        allocator,
                        tech.streamfusion.flink.TestingNativeMemoryManager.create())) {
            assertThat(output.size()).isOne();
            assertThat(output.rowView(0).getInt(0)).isEqualTo(2);
        }
    }

    @Test
    void comparesFixedWidthBinaryThroughTheCDataBoundary() {
        RowType rowType = RowType.of(new BinaryType(false, 3));
        List<RowData> rows = List.of(
                GenericRowData.of(new byte[] {0, 0, 1}),
                GenericRowData.of(new byte[] {0x7f, (byte) 0xff, (byte) 0xff}),
                GenericRowData.of(new byte[] {(byte) 0x80, 0, 0}));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, rowType, allocator);
                ArrowRowDataBatch output = ArrowCDataBridge.execute(
                        fixedBinaryComparisonPlan(),
                        input,
                        rowType,
                        allocator,
                        tech.streamfusion.flink.TestingNativeMemoryManager.create())) {
            assertThat(output.size()).isEqualTo(2);
            assertThat(output.rowView(0).getBinary(0)).containsExactly(0, 0, 1);
            assertThat(output.rowView(1).getBinary(0)).containsExactly(0x7f, 0xff, 0xff);
        }
    }

    @Test
    void comparesFixedWidthCharactersThroughTheCDataBoundary() {
        RowType rowType = RowType.of(new CharType(false, 5));
        List<RowData> rows = List.of(
                GenericRowData.of(StringData.fromString("a    ")),
                GenericRowData.of(StringData.fromString("m    ")),
                GenericRowData.of(StringData.fromString("東京   ")));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, rowType, allocator);
                ArrowRowDataBatch output = ArrowCDataBridge.execute(
                        charComparisonPlan(),
                        input,
                        rowType,
                        allocator,
                        tech.streamfusion.flink.TestingNativeMemoryManager.create())) {
            assertThat(output.size()).isEqualTo(2);
            assertThat(output.rowView(0).getString(0).toString()).isEqualTo("m    ");
            assertThat(output.rowView(1).getString(0).toString()).isEqualTo("東京   ");
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
                ArrowRowDataBatch output = ArrowCDataBridge.execute(
                        projectionPlan(3, 2, 1, 0),
                        input,
                        outputType,
                        allocator,
                        tech.streamfusion.flink.TestingNativeMemoryManager.create())) {
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
                ArrowRowDataBatch output = ArrowCDataBridge.execute(
                        nullCheckPlan(false),
                        input,
                        rowType,
                        allocator,
                        tech.streamfusion.flink.TestingNativeMemoryManager.create())) {
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
                ArrowRowDataBatch output = ArrowCDataBridge.execute(
                        timePlan(precision),
                        input,
                        rowType,
                        allocator,
                        tech.streamfusion.flink.TestingNativeMemoryManager.create())) {
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
                ArrowRowDataBatch output = ArrowCDataBridge.execute(
                        timestampPlan(precision, noon),
                        input,
                        rowType,
                        allocator,
                        tech.streamfusion.flink.TestingNativeMemoryManager.create())) {
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
                ArrowRowDataBatch output = ArrowCDataBridge.execute(
                        projectionPlan(0),
                        input,
                        rowType,
                        allocator,
                        tech.streamfusion.flink.TestingNativeMemoryManager.create())) {
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
                ArrowRowDataBatch output = ArrowCDataBridge.execute(
                        decimalPlan(precision, scale, literal),
                        input,
                        rowType,
                        allocator,
                        tech.streamfusion.flink.TestingNativeMemoryManager.create())) {
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
                ArrowRowDataBatch output = ArrowCDataBridge.execute(
                        floatingPointArithmeticPlan(true),
                        input,
                        rowType,
                        allocator,
                        tech.streamfusion.flink.TestingNativeMemoryManager.create())) {
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
                ArrowRowDataBatch output = ArrowCDataBridge.execute(
                        floatingPointArithmeticPlan(false),
                        input,
                        rowType,
                        allocator,
                        tech.streamfusion.flink.TestingNativeMemoryManager.create())) {
            assertThat(output.rowView(0).getDouble(0)).isEqualTo(-1.0D);
            assertThat(output.rowView(1).getDouble(0)).isEqualTo(1.5D);
            assertThat(output.rowView(2).getDouble(0)).isEqualTo(4.75D);
            assertThat(output.rowView(3).getDouble(0)).isNaN();
            assertThat(output.rowView(4).getDouble(0)).isEqualTo(Double.POSITIVE_INFINITY);
            assertThat(output.rowView(5).getDouble(0)).isEqualTo(Double.NEGATIVE_INFINITY);
            assertThat(output.rowView(6).getDouble(0)).isEqualTo(1.5D);
        }
    }
}

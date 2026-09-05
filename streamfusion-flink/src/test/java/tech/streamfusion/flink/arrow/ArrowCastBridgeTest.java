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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.junit.jupiter.api.Test;

class ArrowCastBridgeTest extends ArrowCDataBridgeTestSupport {
    @Test
    void truthTestsReturnNonNullResultsForNullableBooleans() {
        RowType inputType = RowType.of(new BooleanType(true));
        RowType outputType = RowType.of(
                new BooleanType(false), new BooleanType(false), new BooleanType(false), new BooleanType(false));
        List<RowData> rows =
                List.of(GenericRowData.of(true), GenericRowData.of(false), GenericRowData.of((Object) null));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, inputType, allocator);
                ArrowRowDataBatch output = ArrowCDataBridge.execute(
                        truthTestPlan(),
                        input,
                        outputType,
                        allocator,
                        tech.streamfusion.flink.TestingNativeMemoryManager.create())) {
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
                ArrowRowDataBatch output = ArrowCDataBridge.execute(
                        nullSafeComparisonPlan(),
                        input,
                        outputType,
                        allocator,
                        tech.streamfusion.flink.TestingNativeMemoryManager.create())) {
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
                ArrowRowDataBatch output = ArrowCDataBridge.execute(
                        integerWideningPlan(),
                        input,
                        outputType,
                        allocator,
                        tech.streamfusion.flink.TestingNativeMemoryManager.create())) {
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
                ArrowRowDataBatch output = ArrowCDataBridge.execute(
                        integerToFloatingPlan(),
                        input,
                        outputType,
                        allocator,
                        tech.streamfusion.flink.TestingNativeMemoryManager.create())) {
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
                ArrowRowDataBatch output = ArrowCDataBridge.execute(
                        floatToDoublePlan(),
                        input,
                        outputType,
                        allocator,
                        tech.streamfusion.flink.TestingNativeMemoryManager.create())) {
            for (int index = 0; index < values.size(); index++) {
                assertThat(Double.doubleToLongBits(output.rowView(index).getDouble(0)))
                        .isEqualTo(Double.doubleToLongBits((double) values.get(index)));
            }
        }
    }

    @Test
    void narrowsDoubleToFloatWithFlinkCastParity() {
        RowType inputType = RowType.of(new DoubleType(false));
        RowType outputType = RowType.of(new FloatType(false));
        List<Double> values = List.of(
                Double.NEGATIVE_INFINITY,
                -Double.MAX_VALUE,
                -(double) Float.MAX_VALUE,
                -0.0d,
                0.0d,
                1.0000000596046448d,
                (double) Float.MAX_VALUE,
                Double.MAX_VALUE,
                Double.POSITIVE_INFINITY,
                Double.NaN);
        List<RowData> rows = values.stream()
                .map(value -> (RowData) GenericRowData.of(value))
                .collect(java.util.stream.Collectors.toList());

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, inputType, allocator);
                ArrowRowDataBatch output = ArrowCDataBridge.execute(
                        doubleToFloatPlan(),
                        input,
                        outputType,
                        allocator,
                        tech.streamfusion.flink.TestingNativeMemoryManager.create())) {
            for (int index = 0; index < values.size(); index++) {
                assertThat(Float.floatToIntBits(output.rowView(index).getFloat(0)))
                        .isEqualTo(Float.floatToIntBits((float) (double) values.get(index)));
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
                ArrowRowDataBatch output = ArrowCDataBridge.execute(
                        integerNarrowingPlan(),
                        input,
                        outputType,
                        allocator,
                        tech.streamfusion.flink.TestingNativeMemoryManager.create())) {
            for (int index = 0; index < values.size(); index++) {
                assertThat(output.rowView(index).getByte(0)).isEqualTo((byte) (int) values.get(index));
                assertThat(output.rowView(index).getShort(1)).isEqualTo((short) (int) values.get(index));
            }
            assertThat(output.rowView(values.size()).isNullAt(0)).isTrue();
            assertThat(output.rowView(values.size()).isNullAt(1)).isTrue();
        }
    }

    @Test
    void narrowsSmallintToTinyintWithFlinkOverflowParity() {
        RowType inputType = RowType.of(new SmallIntType(true));
        RowType outputType = RowType.of(new TinyIntType(true));
        List<Short> values = List.of(
                Short.MIN_VALUE,
                (short) -129,
                (short) -128,
                (short) -1,
                (short) 0,
                (short) 127,
                (short) 128,
                Short.MAX_VALUE);
        List<RowData> rows = new ArrayList<>(values.stream()
                .map(value -> (RowData) GenericRowData.of(value))
                .collect(java.util.stream.Collectors.toList()));
        rows.add(GenericRowData.of((Object) null));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, inputType, allocator);
                ArrowRowDataBatch output = ArrowCDataBridge.execute(
                        smallintToTinyintPlan(),
                        input,
                        outputType,
                        allocator,
                        tech.streamfusion.flink.TestingNativeMemoryManager.create())) {
            for (int index = 0; index < values.size(); index++) {
                assertThat(output.rowView(index).getByte(0)).isEqualTo((byte) (short) values.get(index));
            }
            assertThat(output.rowView(values.size()).isNullAt(0)).isTrue();
        }
    }

    @Test
    void narrowsBigintWithFlinkOverflowParity() {
        RowType inputType = RowType.of(new BigIntType(true));
        RowType outputType = RowType.of(new TinyIntType(true), new SmallIntType(true), new IntType(true));
        List<Long> values = List.of(
                Long.MIN_VALUE,
                (long) Integer.MIN_VALUE - 1,
                (long) Integer.MIN_VALUE,
                -1L,
                0L,
                (long) Integer.MAX_VALUE,
                (long) Integer.MAX_VALUE + 1,
                Long.MAX_VALUE);
        List<RowData> rows = new ArrayList<>(values.stream()
                .map(value -> (RowData) GenericRowData.of(value))
                .collect(java.util.stream.Collectors.toList()));
        rows.add(GenericRowData.of((Object) null));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, inputType, allocator);
                ArrowRowDataBatch output = ArrowCDataBridge.execute(
                        bigintNarrowingPlan(),
                        input,
                        outputType,
                        allocator,
                        tech.streamfusion.flink.TestingNativeMemoryManager.create())) {
            for (int index = 0; index < values.size(); index++) {
                assertThat(output.rowView(index).getByte(0)).isEqualTo((byte) (long) values.get(index));
                assertThat(output.rowView(index).getShort(1)).isEqualTo((short) (long) values.get(index));
                assertThat(output.rowView(index).getInt(2)).isEqualTo((int) (long) values.get(index));
            }
            assertThat(output.rowView(values.size()).isNullAt(0)).isTrue();
            assertThat(output.rowView(values.size()).isNullAt(1)).isTrue();
            assertThat(output.rowView(values.size()).isNullAt(2)).isTrue();
        }
    }

    @Test
    void roundsWideIntegersToFloatingPointWithFlinkParity() {
        RowType inputType = RowType.of(new IntType(false), new BigIntType(false));
        RowType outputType = RowType.of(new FloatType(false), new FloatType(false), new DoubleType(false));
        List<RowData> rows = List.of(
                GenericRowData.of(Integer.MIN_VALUE, Long.MIN_VALUE),
                GenericRowData.of(-16_777_217, -9_007_199_254_740_993L),
                GenericRowData.of(-16_777_216, -9_007_199_254_740_992L),
                GenericRowData.of(0, 0L),
                GenericRowData.of(16_777_216, 9_007_199_254_740_992L),
                GenericRowData.of(16_777_217, 9_007_199_254_740_993L),
                GenericRowData.of(Integer.MAX_VALUE, Long.MAX_VALUE));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowRowDataBatch input = ArrowRowDataBatch.transpose(rows, inputType, allocator);
                ArrowRowDataBatch output = ArrowCDataBridge.execute(
                        wideIntegerToFloatingPlan(),
                        input,
                        outputType,
                        allocator,
                        tech.streamfusion.flink.TestingNativeMemoryManager.create())) {
            for (int index = 0; index < rows.size(); index++) {
                RowData inputRow = rows.get(index);
                assertThat(Float.floatToIntBits(output.rowView(index).getFloat(0)))
                        .isEqualTo(Float.floatToIntBits((float) inputRow.getInt(0)));
                assertThat(Float.floatToIntBits(output.rowView(index).getFloat(1)))
                        .isEqualTo(Float.floatToIntBits((float) inputRow.getLong(1)));
                assertThat(Double.doubleToLongBits(output.rowView(index).getDouble(2)))
                        .isEqualTo(Double.doubleToLongBits((double) inputRow.getLong(1)));
            }
        }
    }
}

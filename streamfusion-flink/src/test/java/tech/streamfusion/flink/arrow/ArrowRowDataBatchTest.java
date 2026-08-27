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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

class ArrowRowDataBatchTest {
    private static final RowType NESTED_TYPE = RowType.of(new IntType(), new VarCharType());
    private static final RowType ALL_TYPES = RowType.of(
            new TinyIntType(),
            new SmallIntType(),
            new IntType(),
            new BigIntType(),
            new BooleanType(),
            new FloatType(),
            new DoubleType(),
            new VarCharType(),
            new BinaryType(3),
            new VarBinaryType(),
            new DecimalType(10, 3),
            new DateType(),
            new TimeType(6),
            new TimestampType(6),
            new LocalZonedTimestampType(6),
            new ArrayType(new VarCharType()),
            new MapType(new VarCharType(false, VarCharType.MAX_LENGTH), new IntType()),
            NESTED_TYPE,
            new NullType());

    @Test
    void roundTripsEverySupportedFlinkLogicalTypeThroughArrowViews() {
        Map<StringData, Integer> map = new LinkedHashMap<>();
        map.put(StringData.fromString("one"), 1);
        GenericRowData input = GenericRowData.of(
                (byte) 1,
                (short) 2,
                3,
                4L,
                true,
                5.5f,
                6.5d,
                StringData.fromString("hello"),
                new byte[] {1, 2, 3},
                new byte[] {4, 5},
                DecimalData.fromUnscaledLong(12345, 10, 3),
                20_000,
                12_345,
                TimestampData.fromEpochMillis(1_700_000_000_123L, 456_000),
                TimestampData.fromEpochMillis(1_700_000_000_123L, 456_000),
                new GenericArrayData(new StringData[] {StringData.fromString("a"), null}),
                new GenericMapData(map),
                GenericRowData.of(9, StringData.fromString("nested")),
                null);

        GenericRowData nulls = new GenericRowData(ALL_TYPES.getFieldCount());
        try (ArrowRowDataBatch batch = ArrowRowDataBatch.transpose(List.of(input, nulls), ALL_TYPES)) {
            RowData row = batch.rowView(0);
            assertThat(row.getByte(0)).isEqualTo((byte) 1);
            assertThat(row.getShort(1)).isEqualTo((short) 2);
            assertThat(row.getInt(2)).isEqualTo(3);
            assertThat(row.getLong(3)).isEqualTo(4L);
            assertThat(row.getBoolean(4)).isTrue();
            assertThat(row.getFloat(5)).isEqualTo(5.5f);
            assertThat(row.getDouble(6)).isEqualTo(6.5d);
            assertThat(row.getString(7).toString()).isEqualTo("hello");
            assertThat(row.getBinary(8)).containsExactly(1, 2, 3);
            assertThat(row.getBinary(9)).containsExactly(4, 5);
            assertThat(row.getDecimal(10, 10, 3).toUnscaledLong()).isEqualTo(12345);
            assertThat(row.getInt(11)).isEqualTo(20_000);
            assertThat(row.getInt(12)).isEqualTo(12_345);
            assertThat(row.getTimestamp(13, 6)).isEqualTo(input.getTimestamp(13, 6));
            assertThat(row.getTimestamp(14, 6)).isEqualTo(input.getTimestamp(14, 6));
            assertThat(row.getArray(15).getString(0).toString()).isEqualTo("a");
            assertThat(row.getArray(15).isNullAt(1)).isTrue();
            assertThat(row.getMap(16).size()).isEqualTo(1);
            assertThat(row.getRow(17, 2).getInt(0)).isEqualTo(9);
            assertThat(row.getRow(17, 2).getString(1).toString()).isEqualTo("nested");
            assertThat(row.isNullAt(18)).isTrue();
            RowData nullRow = batch.rowView(1);
            for (int field = 0; field < ALL_TYPES.getFieldCount(); field++) {
                assertThat(nullRow.isNullAt(field)).isTrue();
            }
        }
    }

    @Test
    void rebasesFixedAndVariableWidthSlicesForJavaConsumers() {
        RowType rowType = RowType.of(new IntType(), new VarCharType());
        List<RowData> rows = List.of(
                GenericRowData.of(1, StringData.fromString("skip")),
                GenericRowData.of(2, StringData.fromString("two")),
                GenericRowData.of(3, StringData.fromString("three")));

        try (ArrowRowDataBatch batch = ArrowRowDataBatch.transpose(rows, rowType);
                BufferAllocator allocator = batch.root()
                        .getFieldVectors()
                        .get(0)
                        .getAllocator()
                        .newChildAllocator("rebased", 0, Long.MAX_VALUE);
                VectorSchemaRoot rebased = ArrowBatchRebaser.rebase(batch.root(), 1, 2, allocator)) {
            ArrowReader reader = ArrowUtils.createArrowReader(rebased, rowType);
            assertThat(reader.read(0).getInt(0)).isEqualTo(2);
            assertThat(reader.read(0).getString(1).toString()).isEqualTo("two");
            assertThat(reader.read(1).getInt(0)).isEqualTo(3);
            assertThat(reader.read(1).getString(1).toString()).isEqualTo("three");
        }
    }

    @Test
    void supportsEveryTemporalPrecisionAndArrowDecimal128() {
        RowType rowType = RowType.of(
                new CharType(4),
                new DecimalType(38, 18),
                new TimeType(0),
                new TimeType(3),
                new TimeType(6),
                new TimeType(9),
                new TimestampType(0),
                new TimestampType(3),
                new TimestampType(6),
                new TimestampType(9),
                new LocalZonedTimestampType(0),
                new LocalZonedTimestampType(3),
                new LocalZonedTimestampType(6),
                new LocalZonedTimestampType(9));
        TimestampData timestamp = TimestampData.fromEpochMillis(1_700_000_000_123L, 456_789);
        GenericRowData input = GenericRowData.of(
                StringData.fromString("char"),
                DecimalData.fromBigDecimal(new java.math.BigDecimal("12345678901234567890.123456789012345678"), 38, 18),
                12_000,
                12_345,
                12_345,
                12_345,
                timestamp,
                timestamp,
                timestamp,
                timestamp,
                timestamp,
                timestamp,
                timestamp,
                timestamp);

        try (ArrowRowDataBatch batch = ArrowRowDataBatch.transpose(List.of(input), rowType)) {
            RowData row = batch.rowView(0);
            assertThat(row.getString(0).toString()).isEqualTo("char");
            assertThat(row.getDecimal(1, 38, 18).toBigDecimal())
                    .isEqualByComparingTo("12345678901234567890.123456789012345678");
            for (int field = 2; field <= 5; field++) {
                assertThat(row.getInt(field)).isEqualTo(input.getInt(field));
            }
            for (int field = 6; field < rowType.getFieldCount(); field++) {
                int precision = new int[] {0, 3, 6, 9}[(field - 6) % 4];
                TimestampData expected = new TimestampData[] {
                            TimestampData.fromEpochMillis(1_700_000_000_000L),
                            TimestampData.fromEpochMillis(1_700_000_000_123L),
                            TimestampData.fromEpochMillis(1_700_000_000_123L, 456_000),
                            timestamp
                        }
                        [(field - 6) % 4];
                assertThat(row.getTimestamp(field, precision)).isEqualTo(expected);
            }
        }
    }
}

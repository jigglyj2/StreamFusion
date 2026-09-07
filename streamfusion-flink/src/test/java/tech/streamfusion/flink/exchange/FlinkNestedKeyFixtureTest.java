/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0.
 */
package tech.streamfusion.flink.exchange;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.LinkedHashMap;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.StringUtils;
import org.junit.jupiter.api.Test;

/** Locks the shared native nested-key fixtures to Flink's actual serializers. */
class FlinkNestedKeyFixtureTest {
    @Test
    void nestedBinaryLayoutsMatchNativeFixtures() {
        check(
                new ArrayType(new IntType()),
                new GenericArrayData(new Integer[] {1, null, -2}),
                "0000000000000000180000001000000003000000020000000100000000000000feffffff00000000");
        var map = new LinkedHashMap<StringData, Integer>();
        map.put(StringData.fromString("a"), 7);
        map.put(StringData.fromString("b"), null);
        check(
                new MapType(new VarCharType(), new IntType()),
                new GenericMapData(map),
                "00000000000000002c00000010000000180000000200000000000000610000000000008162000000000000810200000002000000070000000000000000000000");
        check(
                RowType.of(new IntType(), new VarCharType()),
                GenericRowData.of(1, StringData.fromString("abc")),
                "00000000000000001800000010000000000000000000000001000000000000006162630000000083");
    }

    private static void check(LogicalType type, Object value, String expected) {
        var row = new RowDataSerializer(RowType.of(type)).toBinaryRow(GenericRowData.of(value));
        assertThat(StringUtils.byteToHexString(Arrays.copyOfRange(
                        row.getSegments()[0].getArray(), row.getOffset(), row.getOffset() + row.getSizeInBytes())))
                .isEqualTo(expected);
    }

    @Test
    void recursiveOffsetsAndArrayNanNormalizationMatchNativeFixtures() {
        check(
                RowType.of(new ArrayType(new IntType())),
                GenericRowData.of(new GenericArrayData(new Integer[] {1, null, -2})),
                "000000000000000028000000100000000000000000000000180000001000000003000000020000000100000000000000feffffff00000000");
        check(
                new ArrayType(new FloatType()),
                new GenericArrayData(
                        new float[] {Float.intBitsToFloat(0x7f800001), -0.0f, Float.intBitsToFloat(0xffc00012)}),
                "0000000000000000180000001000000003000000000000000000c07f000000800000c07f00000000");
    }
}

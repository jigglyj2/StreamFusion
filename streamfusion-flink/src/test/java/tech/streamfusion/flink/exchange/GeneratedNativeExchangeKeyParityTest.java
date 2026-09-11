/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.exchange;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Random;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;
import org.junit.jupiter.api.Test;

class GeneratedNativeExchangeKeyParityTest {
    @Test
    void generatedNativeKeysMatchFlinkBinaryBytesAndRescaledRouting() throws Exception {
        for (int family = 0; family < 19; family++) {
            for (int seed : new int[] {3, 19, 71}) {
                verify(family, seed);
            }
        }
    }

    private void verify(int family, int seed) throws Exception {
        LogicalType element;
        switch (family) {
            case 0:
                element = new IntType();
                break;
            case 1:
                element = new VarCharType();
                break;
            case 2:
                element = new DoubleType();
                break;
            case 3:
                element = new DecimalType(25, 3);
                break;
            case 4:
                element = new TimestampType(9);
                break;
            case 5:
                element = new ArrayType(new VarCharType());
                break;
            case 6:
                element = RowType.of(new IntType(), new VarCharType());
                break;
            case 7:
                element = new MapType(new VarCharType(false, VarCharType.MAX_LENGTH), new IntType());
                break;
            case 8:
                element = new BooleanType();
                break;
            case 9:
                element = new TinyIntType();
                break;
            case 10:
                element = new SmallIntType();
                break;
            case 11:
                element = new BigIntType();
                break;
            case 12:
                element = new FloatType();
                break;
            case 13:
                element = new DateType();
                break;
            case 14:
                element = new TimeType(3);
                break;
            case 15:
                element = new VarBinaryType();
                break;
            case 16:
                element = new CharType(4);
                break;
            case 17:
                element = new LocalZonedTimestampType(9);
                break;
            default:
                element = new DecimalType(18, 3);
        }
        LogicalType keyType = seed == 3
                ? new ArrayType(element)
                : seed == 19
                        ? RowType.of(new ArrayType(element), new IntType(), new VarCharType())
                        : new MapType(new VarCharType(false, VarCharType.MAX_LENGTH), new ArrayType(element));
        RowType type = RowType.of(new IntType(false), keyType);
        var rows = new ArrayList<RowData>();
        var random = new Random(seed);
        for (int row = 0; row < 36; row++) {
            var values = new Object[new int[] {0, 1, 7, 8, 31, 32, 33, 63, 64, 65}[row % 10]];
            for (int i = 0; i < values.length; i++) values[i] = i % 4 == 0 ? null : value(family, random, row + i);
            Object key = new GenericArrayData(values);
            if (seed == 19)
                key = GenericRowData.of(key, row % 3 == 0 ? null : row, StringData.fromString("key-" + row));
            else if (seed == 71) {
                var entries = new LinkedHashMap<StringData, Object>();
                entries.put(StringData.fromString("z"), key);
                entries.put(StringData.fromString("a"), null);
                key = new GenericMapData(entries);
            }
            var input = GenericRowData.of(row, row % 7 == 0 ? null : key);
            rows.add(input);
        }
        ExchangeKeyParityFixture.verify(type, rows, "family " + family + " seed " + seed);
    }

    private Object value(int family, Random random, int index) {
        switch (family) {
            case 0:
                return random.nextInt();
            case 1:
                return StringData.fromString("é-" + random.nextLong());
            case 2:
                return index % 3 == 0
                        ? Double.longBitsToDouble(0xfff0000000000001L)
                        : index % 3 == 1 ? -0.0d : random.nextDouble();
            case 3:
                return DecimalData.fromBigDecimal(BigDecimal.valueOf(random.nextLong(), 3), 25, 3);
            case 4:
                return TimestampData.fromEpochMillis(-random.nextInt(10000), random.nextInt(1000000));
            case 5:
                return new GenericArrayData(
                        new StringData[] {StringData.fromString("nested-" + index), null, StringData.fromString("")});
            case 6:
                return GenericRowData.of(
                        index, index % 2 == 0 ? null : StringData.fromString("row-" + random.nextLong()));
            case 8:
                return index % 2 == 0;
            case 9:
                return (byte) random.nextInt();
            case 10:
                return (short) random.nextInt();
            case 11:
                return random.nextLong();
            case 12:
                return index % 3 == 0 ? Float.intBitsToFloat(0xff800001) : index % 3 == 1 ? -0.0f : random.nextFloat();
            case 13:
                return random.nextInt(40000) - 20000;
            case 14:
                return random.nextInt(86400000);
            case 15:
                return new byte[index % 17];
            case 16:
                return StringData.fromString("ab  ");
            case 17:
                return TimestampData.fromEpochMillis(-random.nextInt(10000), random.nextInt(1000000));
            case 18:
                return DecimalData.fromBigDecimal(
                        BigDecimal.valueOf(random.nextLong() % 1000000000000000000L, 3), 18, 3);
            default: {
                var map = new LinkedHashMap<StringData, Integer>();
                map.put(StringData.fromString("b"), null);
                map.put(StringData.fromString("a"), index);
                return new GenericMapData(map);
            }
        }
    }
}

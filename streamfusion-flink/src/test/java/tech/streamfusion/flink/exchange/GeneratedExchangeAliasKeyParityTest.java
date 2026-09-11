/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.exchange;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;
import org.junit.jupiter.api.Test;

class GeneratedExchangeAliasKeyParityTest {
    @Test
    void physicalAliasesKeepNativeKeyEncodingAndExactFlinkBytes() throws Exception {
        for (int family = 0; family < 6; family++) {
            for (int seed : new int[] {3, 19, 71}) {
                for (boolean nested : List.of(false, true)) {
                    var element = type(family);
                    var key = nested ? RowType.of(element, new ArrayType(element)) : element;
                    var type = RowType.of(new IntType(false), key);
                    var random = new Random(seed);
                    var rows = new ArrayList<RowData>();
                    for (int row = 0; row < 36; row++) {
                        var value = value(family, row, random);
                        Object keyValue = nested
                                ? GenericRowData.of(value, new GenericArrayData(new Object[] {
                                    null, value, value(family, row + 1, random)
                                }))
                                : value;
                        rows.add(GenericRowData.of(row, row % 7 == 0 ? null : keyValue));
                    }
                    ExchangeKeyParityFixture.verify(
                            type, rows, "alias " + family + " seed " + seed + " nested " + nested);
                }
            }
        }
    }

    private static LogicalType type(int family) {
        switch (family) {
            case 0:
                return new YearMonthIntervalType(YearMonthIntervalType.YearMonthResolution.YEAR_TO_MONTH);
            case 1:
                return new DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND);
            case 2:
                return new MultisetType(new VarCharType(false, VarCharType.MAX_LENGTH));
            case 3:
                return DistinctType.newBuilder(
                                ObjectIdentifier.of("streamfusion", "test", "distinct_int"), new IntType())
                        .build();
            case 4:
                return DistinctType.newBuilder(
                                ObjectIdentifier.of("streamfusion", "test", "distinct_array"),
                                new ArrayType(new VarCharType()))
                        .build();
            default:
                return StructuredType.newBuilder(ObjectIdentifier.of("streamfusion", "test", "structured"))
                        .attributes(List.of(
                                new StructuredType.StructuredAttribute("id", new BigIntType()),
                                new StructuredType.StructuredAttribute("name", new VarCharType())))
                        .comparison(StructuredType.StructuredComparison.EQUALS)
                        .build();
        }
    }

    private static Object value(int family, int row, Random random) {
        switch (family) {
            case 0:
            case 3:
                return row % 3 == 0 ? Integer.MIN_VALUE : row % 3 == 1 ? Integer.MAX_VALUE : random.nextInt();
            case 1:
                return row % 3 == 0 ? Long.MIN_VALUE : row % 3 == 1 ? Long.MAX_VALUE : random.nextLong();
            case 2:
                var map = new LinkedHashMap<StringData, Integer>();
                if (row % 5 != 0) {
                    map.put(StringData.fromString("é-" + random.nextLong()), row + 1);
                    map.put(StringData.fromString("a"), 2);
                }
                return new GenericMapData(map);
            case 4:
                return new GenericArrayData(
                        new Object[] {StringData.fromString("é-" + random.nextLong()), null, StringData.fromString("")
                        });
            default:
                return GenericRowData.of(random.nextLong(), row % 3 == 0 ? null : StringData.fromString("é-" + row));
        }
    }
}

/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.sql;

import java.util.Arrays;
import java.util.LinkedHashMap;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class MapUnnestFallbackParityTest extends SqlParityTestSupport {
    private static final java.util.List<Row> INPUTS = Arrays.asList(
            Row.of(mapOf("second", 2, "first", null)),
            Row.of(mapOf("unicode-你好", -4)),
            Row.of(new LinkedHashMap<>()),
            Row.of((Object) null));

    @Test
    void fallbackMapUnnestMatchesKeyValuePairsAndStoredOrdinality() throws Exception {
        assertFallbackDataStreamParity(
                "SELECT map_key, map_value, ord_idx FROM map_unnest_input "
                        + "CROSS JOIN UNNEST(metric) WITH ORDINALITY "
                        + "AS expanded(map_key, map_value, ord_idx)",
                Types.MAP(Types.STRING, Types.INT),
                DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.INT()),
                INPUTS,
                "map_unnest_input");

        assertFallbackExecution();
    }

    @Test
    void fallbackLeftMapUnnestNullExtendsNullAndEmptyMaps() throws Exception {
        assertFallbackDataStreamParity(
                "SELECT map_key, map_value, ord_idx FROM left_map_unnest_input "
                        + "LEFT JOIN UNNEST(metric) WITH ORDINALITY "
                        + "AS expanded(map_key, map_value, ord_idx) ON TRUE",
                Types.MAP(Types.STRING, Types.INT),
                DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.INT()),
                INPUTS,
                "left_map_unnest_input");

        assertFallbackExecution();
    }

    @Test
    void fallbackMapUnnestPreservesRowValuesAsOneComplexColumn() throws Exception {
        java.util.List<Row> inputs = Arrays.asList(
                Row.of(rowMap("first", Row.of(7, "seven"), "nullable", Row.of(null, "值"))),
                Row.of(new LinkedHashMap<>()),
                Row.of((Object) null));

        assertFallbackDataStreamParity(
                "SELECT map_key, map_value, ord_idx FROM row_value_map_unnest_input "
                        + "LEFT JOIN UNNEST(metric) WITH ORDINALITY "
                        + "AS expanded(map_key, map_value, ord_idx) ON TRUE",
                Types.MAP(Types.STRING, Types.ROW_NAMED(new String[] {"number", "label"}, Types.INT, Types.STRING)),
                DataTypes.MAP(
                        DataTypes.STRING().notNull(),
                        DataTypes.ROW(
                                DataTypes.FIELD("number", DataTypes.INT()),
                                DataTypes.FIELD("label", DataTypes.STRING()))),
                inputs,
                "row_value_map_unnest_input");

        assertFallbackExecution();
    }

    @Test
    void fallbackMapUnnestPreservesNonNullRowKeysAsOneComplexColumn() throws Exception {
        LinkedHashMap<Row, String> metric = new LinkedHashMap<>();
        metric.put(Row.of(2, "second"), "two");
        metric.put(Row.of(1, null), "one");

        assertFallbackDataStreamParity(
                "SELECT map_key, map_value, ord_idx FROM row_key_map_unnest_input "
                        + "CROSS JOIN UNNEST(metric) WITH ORDINALITY "
                        + "AS expanded(map_key, map_value, ord_idx)",
                Types.MAP(Types.ROW_NAMED(new String[] {"number", "label"}, Types.INT, Types.STRING), Types.STRING),
                DataTypes.MAP(
                        DataTypes.ROW(
                                        DataTypes.FIELD("number", DataTypes.INT()),
                                        DataTypes.FIELD("label", DataTypes.STRING()))
                                .notNull(),
                        DataTypes.STRING()),
                java.util.List.of(Row.of(metric)),
                "row_key_map_unnest_input");

        assertFallbackExecution();
    }

    @Test
    void fallbackMapUnnestPreservesScalarArrayValuesAsOneComplexColumn() throws Exception {
        LinkedHashMap<String, Integer[]> metric = new LinkedHashMap<>();
        metric.put("values", new Integer[] {1, null, 3});
        metric.put("empty", new Integer[] {});
        metric.put("null", null);

        assertFallbackDataStreamParity(
                "SELECT map_key, map_value, ord_idx FROM array_value_map_unnest_input "
                        + "LEFT JOIN UNNEST(metric) WITH ORDINALITY "
                        + "AS expanded(map_key, map_value, ord_idx) ON TRUE",
                Types.MAP(Types.STRING, Types.OBJECT_ARRAY(Types.INT)),
                DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.ARRAY(DataTypes.INT())),
                Arrays.asList(Row.of(metric), Row.of(new LinkedHashMap<>()), Row.of((Object) null)),
                "array_value_map_unnest_input");

        assertFallbackExecution();
    }

    @Test
    void fallbackMapUnnestPreservesRowValuesContainingScalarArrays() throws Exception {
        LinkedHashMap<String, Row> metric = new LinkedHashMap<>();
        metric.put("values", Row.of("alpha", new Integer[] {1, null, 3}));
        metric.put("empty", Row.of(null, new Integer[] {}));
        metric.put("null-array", Row.of("omega", null));
        metric.put("null-row", null);

        assertFallbackDataStreamParity(
                "SELECT map_key, map_value FROM nested_row_value_map_unnest_input "
                        + "CROSS JOIN UNNEST(metric) AS expanded(map_key, map_value)",
                Types.MAP(
                        Types.STRING,
                        Types.ROW_NAMED(new String[] {"label", "values"}, Types.STRING, Types.OBJECT_ARRAY(Types.INT))),
                DataTypes.MAP(
                        DataTypes.STRING().notNull(),
                        DataTypes.ROW(
                                DataTypes.FIELD("label", DataTypes.STRING()),
                                DataTypes.FIELD("values", DataTypes.ARRAY(DataTypes.INT())))),
                java.util.List.of(Row.of(metric)),
                "nested_row_value_map_unnest_input");

        assertFallbackExecution();
    }

    private static LinkedHashMap<String, Integer> mapOf(Object... entries) {
        LinkedHashMap<String, Integer> map = new LinkedHashMap<>();
        for (int index = 0; index < entries.length; index += 2) {
            map.put((String) entries[index], (Integer) entries[index + 1]);
        }
        return map;
    }

    private static LinkedHashMap<String, Row> rowMap(Object... entries) {
        LinkedHashMap<String, Row> map = new LinkedHashMap<>();
        for (int index = 0; index < entries.length; index += 2) {
            map.put((String) entries[index], (Row) entries[index + 1]);
        }
        return map;
    }

    private static void assertFallbackExecution() {
        SqlFallbackAssertions.nativeBatchesAreZero(StreamFusionPlannerFactory.nativePlanBatchCount());
        SqlFallbackAssertions.admission();
    }
}

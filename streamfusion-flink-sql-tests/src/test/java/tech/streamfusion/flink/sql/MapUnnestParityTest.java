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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.LinkedHashMap;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class MapUnnestParityTest extends SqlParityTestSupport {
    private static final java.util.List<Row> INPUTS = Arrays.asList(
            Row.of(mapOf("second", 2, "first", null)),
            Row.of(mapOf("unicode-你好", -4)),
            Row.of(new LinkedHashMap<>()),
            Row.of((Object) null));

    @Test
    void nativeMapUnnestMatchesKeyValuePairsAndStoredOrdinality() throws Exception {
        assertDataStreamParity(
                "SELECT map_key, map_value, ord_idx FROM map_unnest_input "
                        + "CROSS JOIN UNNEST(metric) WITH ORDINALITY "
                        + "AS expanded(map_key, map_value, ord_idx)",
                Types.MAP(Types.STRING, Types.INT),
                DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.INT()),
                INPUTS,
                "map_unnest_input");

        assertNativeExecution();
    }

    @Test
    void nativeLeftMapUnnestNullExtendsNullAndEmptyMaps() throws Exception {
        assertDataStreamParity(
                "SELECT map_key, map_value, ord_idx FROM left_map_unnest_input "
                        + "LEFT JOIN UNNEST(metric) WITH ORDINALITY "
                        + "AS expanded(map_key, map_value, ord_idx) ON TRUE",
                Types.MAP(Types.STRING, Types.INT),
                DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.INT()),
                INPUTS,
                "left_map_unnest_input");

        assertNativeExecution();
    }

    @Test
    void nativeMapUnnestPreservesRowValuesAsOneComplexColumn() throws Exception {
        java.util.List<Row> inputs = Arrays.asList(
                Row.of(rowMap("first", Row.of(7, "seven"), "nullable", Row.of(null, "值"))),
                Row.of(new LinkedHashMap<>()),
                Row.of((Object) null));

        assertDataStreamParity(
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

        assertNativeExecution();
    }

    @Test
    void nativeMapUnnestPreservesNonNullRowKeysAsOneComplexColumn() throws Exception {
        LinkedHashMap<Row, String> metric = new LinkedHashMap<>();
        metric.put(Row.of(2, "second"), "two");
        metric.put(Row.of(1, null), "one");

        assertDataStreamParity(
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

        assertNativeExecution();
    }

    @Test
    void nativeMapUnnestPreservesScalarArrayValuesAsOneComplexColumn() throws Exception {
        LinkedHashMap<String, Integer[]> metric = new LinkedHashMap<>();
        metric.put("values", new Integer[] {1, null, 3});
        metric.put("empty", new Integer[] {});
        metric.put("null", null);

        assertDataStreamParity(
                "SELECT map_key, map_value, ord_idx FROM array_value_map_unnest_input "
                        + "LEFT JOIN UNNEST(metric) WITH ORDINALITY "
                        + "AS expanded(map_key, map_value, ord_idx) ON TRUE",
                Types.MAP(Types.STRING, Types.OBJECT_ARRAY(Types.INT)),
                DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.ARRAY(DataTypes.INT())),
                Arrays.asList(Row.of(metric), Row.of(new LinkedHashMap<>()), Row.of((Object) null)),
                "array_value_map_unnest_input");

        assertNativeExecution();
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

    private static void assertNativeExecution() {
        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isEqualTo(1);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }
}

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

    private static LinkedHashMap<String, Integer> mapOf(Object... entries) {
        LinkedHashMap<String, Integer> map = new LinkedHashMap<>();
        for (int index = 0; index < entries.length; index += 2) {
            map.put((String) entries[index], (Integer) entries[index + 1]);
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

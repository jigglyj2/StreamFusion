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

class ComputedMapUnnestParityTest extends SqlParityTestSupport {
    @Test
    void nativeUnnestEvaluatesMapConstructorInsideTheSamePlan() throws Exception {
        assertDataStreamParity(
                "SELECT map_key, map_value, ord_idx FROM computed_map_unnest_input "
                        + "CROSS JOIN UNNEST(MAP['original', metric, 'incremented', metric + 1]) "
                        + "WITH ORDINALITY AS expanded(map_key, map_value, ord_idx)",
                Types.INT,
                DataTypes.INT(),
                Arrays.asList(Row.of(1), Row.of(-2), Row.of((Object) null)),
                "computed_map_unnest_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isEqualTo(1);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void nativeUnnestEvaluatesMapKeysInsideTheSamePlan() throws Exception {
        assertDataStreamParity(
                "SELECT map_key, ord_idx FROM computed_map_keys_unnest_input "
                        + "LEFT JOIN UNNEST(MAP_KEYS(metric)) WITH ORDINALITY "
                        + "AS expanded(map_key, ord_idx) ON TRUE",
                Types.MAP(Types.STRING, Types.INT),
                DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.INT()),
                Arrays.asList(
                        Row.of(java.util.Map.of("first", 1, "second", 2)), Row.of(java.util.Map.of()), Row.of((Object)
                                null)),
                "computed_map_keys_unnest_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isEqualTo(1);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void nativeUnnestEvaluatesNullableMapValuesInsideTheSamePlan() throws Exception {
        LinkedHashMap<String, Integer> nullableValues = new LinkedHashMap<>();
        nullableValues.put("first", 1);
        nullableValues.put("missing", null);

        assertDataStreamParity(
                "SELECT map_value, ord_idx FROM computed_map_values_unnest_input "
                        + "LEFT JOIN UNNEST(MAP_VALUES(metric)) WITH ORDINALITY "
                        + "AS expanded(map_value, ord_idx) ON TRUE",
                Types.MAP(Types.STRING, Types.INT),
                DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.INT()),
                Arrays.asList(Row.of(nullableValues), Row.of(java.util.Map.of()), Row.of((Object) null)),
                "computed_map_values_unnest_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isEqualTo(1);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void mapEntriesUnnestFallsBackForFlinkNullableEntryRowTyping() throws Exception {
        assertFallbackDataStreamParity(
                "SELECT map_key, map_value FROM map_entries_unnest_fallback_input "
                        + "CROSS JOIN UNNEST(MAP_ENTRIES(metric)) AS expanded(map_key, map_value)",
                Types.MAP(Types.STRING, Types.INT),
                DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.INT()),
                Arrays.asList(Row.of(java.util.Map.of("first", 1)), Row.of(java.util.Map.of()), Row.of((Object) null)),
                "map_entries_unnest_fallback_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isZero();
        assertThat(StreamFusionPlanningDiagnostics.explain())
                .contains("array UNNEST output field 0 does not match its ROW element field")
                .contains("Accelerated: no");
    }
}

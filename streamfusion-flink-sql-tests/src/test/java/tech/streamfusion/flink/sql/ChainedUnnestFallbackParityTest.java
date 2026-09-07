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

class ChainedUnnestFallbackParityTest extends SqlParityTestSupport {
    @Test
    void adjacentArrayUnnestsAndProjectionFallBackAsAWholePlan() throws Exception {
        assertFallbackDataStreamParity(
                "SELECT outer_pos, item, inner_pos FROM chained_array_unnest_input "
                        + "CROSS JOIN UNNEST(metric) WITH ORDINALITY AS outer_values(inner_array, outer_pos) "
                        + "CROSS JOIN UNNEST(inner_array) WITH ORDINALITY AS inner_values(item, inner_pos)",
                Types.OBJECT_ARRAY(Types.OBJECT_ARRAY(Types.INT)),
                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())),
                Arrays.asList(
                        Row.of((Object) new Integer[][] {{1, null}, {}, {3, 4}}),
                        Row.of((Object) new Integer[][] {}),
                        Row.of((Object) null)),
                "chained_array_unnest_input");

        SqlFallbackAssertions.nativeBatchesAreZero(StreamFusionPlannerFactory.nativePlanBatchCount());
        SqlFallbackAssertions.admission();
    }

    @Test
    void adjacentArrayUnnestsWithoutProjectionFallBackAsAWholePlan() throws Exception {
        assertFallbackDataStreamParity(
                "SELECT * FROM direct_chained_array_unnest_input "
                        + "CROSS JOIN UNNEST(metric) WITH ORDINALITY AS outer_values(inner_array, outer_pos) "
                        + "CROSS JOIN UNNEST(inner_array) WITH ORDINALITY AS inner_values(item, inner_pos)",
                Types.OBJECT_ARRAY(Types.OBJECT_ARRAY(Types.INT)),
                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())),
                Arrays.asList(
                        Row.of((Object) new Integer[][] {{1, null}, {}, {3, 4}}),
                        Row.of((Object) new Integer[][] {}),
                        Row.of((Object) null)),
                "direct_chained_array_unnest_input");

        SqlFallbackAssertions.nativeBatchesAreZero(StreamFusionPlannerFactory.nativePlanBatchCount());
        SqlFallbackAssertions.admission();
    }

    @Test
    void adjacentMapAndArrayUnnestsFallBackAsAWholePlan() throws Exception {
        LinkedHashMap<String, Integer[]> populated = new LinkedHashMap<>();
        populated.put("first", new Integer[] {1, null});
        populated.put("empty", new Integer[] {});
        populated.put("second", new Integer[] {3, 4});

        assertFallbackDataStreamParity(
                "SELECT map_key, map_pos, item, array_pos FROM chained_map_array_unnest_input "
                        + "CROSS JOIN UNNEST(metric) WITH ORDINALITY "
                        + "AS map_values(map_key, inner_array, map_pos) "
                        + "CROSS JOIN UNNEST(inner_array) WITH ORDINALITY "
                        + "AS array_values(item, array_pos)",
                Types.MAP(Types.STRING, Types.OBJECT_ARRAY(Types.INT)),
                DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.ARRAY(DataTypes.INT())),
                Arrays.asList(Row.of(populated), Row.of(new LinkedHashMap<>()), Row.of((Object) null)),
                "chained_map_array_unnest_input");

        SqlFallbackAssertions.nativeBatchesAreZero(StreamFusionPlannerFactory.nativePlanBatchCount());
        SqlFallbackAssertions.admission();
    }

    @Test
    void adjacentLeftArrayUnnestsPreserveEachEmptyLevelDuringWholePlanFallback() throws Exception {
        assertFallbackDataStreamParity(
                "SELECT outer_pos, item, inner_pos FROM chained_left_unnest_input "
                        + "LEFT JOIN UNNEST(metric) WITH ORDINALITY AS outer_values(inner_array, outer_pos) ON TRUE "
                        + "LEFT JOIN UNNEST(inner_array) WITH ORDINALITY AS inner_values(item, inner_pos) ON TRUE",
                Types.OBJECT_ARRAY(Types.OBJECT_ARRAY(Types.INT)),
                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())),
                Arrays.asList(
                        Row.of((Object) new Integer[][] {{1, null}, {}, {3}}),
                        Row.of((Object) new Integer[][] {}),
                        Row.of((Object) null)),
                "chained_left_unnest_input");

        SqlFallbackAssertions.nativeBatchesAreZero(StreamFusionPlannerFactory.nativePlanBatchCount());
        SqlFallbackAssertions.admission();
    }
}

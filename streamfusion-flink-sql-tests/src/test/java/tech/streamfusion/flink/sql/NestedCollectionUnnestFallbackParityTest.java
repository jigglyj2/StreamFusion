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

class NestedCollectionUnnestFallbackParityTest extends SqlParityTestSupport {
    @Test
    void fallbackLeftUnnestReadsNestedMapField() throws Exception {
        LinkedHashMap<String, Integer> populated = new LinkedHashMap<>();
        populated.put("second", 2);
        populated.put("nullable", null);

        assertFallbackDataStreamParity(
                "SELECT map_key, map_value, ord_idx FROM nested_map_unnest_input "
                        + "LEFT JOIN UNNEST(nested_map_unnest_input.metric.items) WITH ORDINALITY "
                        + "AS expanded(map_key, map_value, ord_idx) ON TRUE",
                Types.ROW_NAMED(new String[] {"items"}, Types.MAP(Types.STRING, Types.INT)),
                DataTypes.ROW(DataTypes.FIELD(
                        "items", DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.INT()))),
                Arrays.asList(
                        Row.of(Row.of(populated)),
                        Row.of(Row.of(new LinkedHashMap<>())),
                        Row.of(Row.of((Object) null)),
                        Row.of((Object) null)),
                "nested_map_unnest_input");

        assertFallbackExecution();
    }

    @Test
    void fallbackLeftUnnestReadsNestedMultisetField() throws Exception {
        LinkedHashMap<String, Integer> populated = new LinkedHashMap<>();
        populated.put("repeat", 2);
        populated.put("once", 1);
        populated.put("zero", 0);

        assertFallbackDataStreamParity(
                "SELECT item, ord_idx FROM nested_multiset_unnest_input "
                        + "LEFT JOIN UNNEST(nested_multiset_unnest_input.metric.items) WITH ORDINALITY "
                        + "AS expanded(item, ord_idx) ON TRUE",
                Types.ROW_NAMED(new String[] {"items"}, Types.MAP(Types.STRING, Types.INT)),
                DataTypes.ROW(DataTypes.FIELD(
                        "items", DataTypes.MULTISET(DataTypes.STRING().notNull()))),
                Arrays.asList(
                        Row.of(Row.of(populated)),
                        Row.of(Row.of(new LinkedHashMap<>())),
                        Row.of(Row.of((Object) null)),
                        Row.of((Object) null)),
                "nested_multiset_unnest_input");

        assertFallbackExecution();
    }

    private static void assertFallbackExecution() {
        SqlFallbackAssertions.nativeBatchesAreZero(StreamFusionPlannerFactory.nativePlanBatchCount());
        SqlFallbackAssertions.admission();
    }
}

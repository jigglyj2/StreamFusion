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
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class MapUnionFallbackTest extends SqlParityTestSupport {
    @Test
    void mapUnionFallsBackForMapRepresentationParity() throws Exception {
        Map<Integer, Integer> first = new LinkedHashMap<>();
        first.put(1, 10);
        first.put(2, 20);
        Map<Integer, Integer> second = new LinkedHashMap<>();
        second.put(2, 200);

        assertDataStreamParity(
                "SELECT CARDINALITY(MAP_UNION(metric, MAP[3, 30])) FROM map_input",
                Types.MAP(Types.INT, Types.INT),
                DataTypes.MAP(DataTypes.INT(), DataTypes.INT()),
                Arrays.asList(Row.of(first), Row.of(second), Row.of((Object) null)),
                "map_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isZero();
        assertThat(StreamFusionPlanningDiagnostics.explain())
                .contains("MapDataForMapUnion")
                .contains("Arrow maps require non-null keys")
                .contains("Accelerated: no");
    }
}

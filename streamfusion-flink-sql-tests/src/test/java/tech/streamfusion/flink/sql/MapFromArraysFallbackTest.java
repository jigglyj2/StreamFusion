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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class MapFromArraysFallbackTest extends SqlParityTestSupport {
    @Test
    void mapFromArraysFallsBackForFlinkGeneratedConsumerParity() throws Exception {
        assertDataStreamParity(
                "SELECT CARDINALITY(MAP_FROM_ARRAYS(metric, ARRAY['first', 'second'])) FROM array_input",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                Arrays.asList(Row.of((Object) new Integer[] {1, 2}), Row.of((Object) new Integer[] {3, 4})),
                "array_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isZero();
        assertThat(StreamFusionPlanningDiagnostics.explain())
                .contains("MapDataForMapFromArrays")
                .contains("GenericMapData")
                .contains("Accelerated: no");
    }
}

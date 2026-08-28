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

class ChainedUnnestParityTest extends SqlParityTestSupport {
    @Test
    void adjacentArrayUnnestsAndProjectionUseOneNativePlan() throws Exception {
        assertDataStreamParity(
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

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isEqualTo(1);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void adjacentArrayUnnestsWithoutProjectionUseOneNativePlan() throws Exception {
        assertDataStreamParity(
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

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isEqualTo(1);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }
}

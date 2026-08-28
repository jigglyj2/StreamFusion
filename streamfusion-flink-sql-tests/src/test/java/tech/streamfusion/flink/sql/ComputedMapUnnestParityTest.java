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
}

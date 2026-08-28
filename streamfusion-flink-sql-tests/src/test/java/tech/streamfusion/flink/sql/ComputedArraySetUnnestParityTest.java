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

class ComputedArraySetUnnestParityTest extends SqlParityTestSupport {
    @Test
    void nativeUnnestPreservesArrayDistinctEncounterOrder() throws Exception {
        assertDataStreamParity(
                "SELECT item, ord_idx FROM array_distinct_unnest_input "
                        + "LEFT JOIN UNNEST(ARRAY_DISTINCT(metric)) WITH ORDINALITY "
                        + "AS expanded(item, ord_idx) ON TRUE",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                Arrays.asList(
                        Row.of((Object) new Integer[] {2, null, 1, 2, null, 1}),
                        Row.of((Object) new Integer[] {}),
                        Row.of((Object) null)),
                "array_distinct_unnest_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isEqualTo(1);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void nativeUnnestPreservesArrayUnionEncounterOrder() throws Exception {
        assertDataStreamParity(
                "SELECT item, ord_idx FROM array_union_unnest_input "
                        + "CROSS JOIN UNNEST(ARRAY_UNION(metric, ARRAY[3, 2, CAST(NULL AS INT)])) "
                        + "WITH ORDINALITY AS expanded(item, ord_idx)",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                Arrays.asList(
                        Row.of((Object) new Integer[] {2, null, 1, 2}),
                        Row.of((Object) new Integer[] {}),
                        Row.of((Object) null)),
                "array_union_unnest_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isEqualTo(1);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void nativeUnnestPreservesArrayIntersectLeftEncounterOrder() throws Exception {
        assertDataStreamParity(
                "SELECT item, ord_idx FROM array_intersect_unnest_input "
                        + "CROSS JOIN UNNEST(ARRAY_INTERSECT(metric, ARRAY[3, 2, CAST(NULL AS INT)])) "
                        + "WITH ORDINALITY AS expanded(item, ord_idx)",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                Arrays.asList(
                        Row.of((Object) new Integer[] {2, null, 1, 2, 3}),
                        Row.of((Object) new Integer[] {}),
                        Row.of((Object) null)),
                "array_intersect_unnest_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isEqualTo(1);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void nativeUnnestPreservesArrayExceptLeftEncounterOrder() throws Exception {
        assertDataStreamParity(
                "SELECT item, ord_idx FROM array_except_unnest_input "
                        + "LEFT JOIN UNNEST(ARRAY_EXCEPT(metric, ARRAY[2, CAST(NULL AS INT)])) "
                        + "WITH ORDINALITY AS expanded(item, ord_idx) ON TRUE",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                Arrays.asList(
                        Row.of((Object) new Integer[] {2, null, 1, 2, 3, 1}),
                        Row.of((Object) new Integer[] {}),
                        Row.of((Object) null)),
                "array_except_unnest_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isEqualTo(1);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }
}

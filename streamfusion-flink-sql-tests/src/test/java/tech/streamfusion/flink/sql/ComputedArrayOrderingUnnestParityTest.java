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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class ComputedArrayOrderingUnnestParityTest extends SqlParityTestSupport {
    @Test
    void nativeUnnestObservesArraySortDirectionAndNullOrdering() throws Exception {
        assertDataStreamParity(
                "SELECT item, ord_idx FROM array_sort_unnest_input "
                        + "LEFT JOIN UNNEST(ARRAY_SORT(metric, FALSE, TRUE)) WITH ORDINALITY "
                        + "AS expanded(item, ord_idx) ON TRUE",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                Arrays.asList(
                        Row.of((Object) new Integer[] {2, null, 1, 3, null}),
                        Row.of((Object) new Integer[] {}),
                        Row.of((Object) null)),
                "array_sort_unnest_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isEqualTo(1);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void nativeUnnestObservesNegativeArraySliceBounds() throws Exception {
        assertDataStreamParity(
                "SELECT item, ord_idx FROM array_slice_unnest_input "
                        + "LEFT JOIN UNNEST(ARRAY_SLICE(metric, -3, -1)) WITH ORDINALITY "
                        + "AS expanded(item, ord_idx) ON TRUE",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                Arrays.asList(
                        Row.of((Object) new Integer[] {1, null, 3, 4, 5}),
                        Row.of((Object) new Integer[] {}),
                        Row.of((Object) null)),
                "array_slice_unnest_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isEqualTo(1);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void nativeUnnestEvaluatesPerRowArraySliceBoundsInsideTheFusedPlan() throws Exception {
        TypeInformation<Row> metricType = Types.ROW_NAMED(
                new String[] {"items", "start_pos", "end_pos"}, Types.OBJECT_ARRAY(Types.INT), Types.INT, Types.INT);
        DataType logicalType = DataTypes.ROW(
                DataTypes.FIELD("items", DataTypes.ARRAY(DataTypes.INT())),
                DataTypes.FIELD("start_pos", DataTypes.INT()),
                DataTypes.FIELD("end_pos", DataTypes.INT()));

        assertDataStreamParity(
                "SELECT item, ord_idx FROM dynamic_array_slice_unnest_input "
                        + "LEFT JOIN UNNEST(ARRAY_SLICE(dynamic_array_slice_unnest_input.metric.items, "
                        + "dynamic_array_slice_unnest_input.metric.start_pos, "
                        + "dynamic_array_slice_unnest_input.metric.end_pos)) "
                        + "WITH ORDINALITY AS expanded(item, ord_idx) ON TRUE",
                metricType,
                logicalType,
                Arrays.asList(
                        Row.of(Row.of(new Integer[] {1, 2, 3, 4, 5}, 2, 4)),
                        Row.of(Row.of(new Integer[] {1, 2, 3, 4, 5}, -3, -1)),
                        Row.of(Row.of(new Integer[] {1, 2, 3}, 1, 0)),
                        Row.of(Row.of(new Integer[] {1, 2, 3}, null, 2)),
                        Row.of(Row.of(new Integer[] {}, 1, 2)),
                        Row.of(Row.of(null, 1, 2))),
                "dynamic_array_slice_unnest_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isEqualTo(1);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }
}

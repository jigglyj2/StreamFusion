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

class CollectionSliceParityTest extends SqlParityTestSupport {
    @Test
    void arraySliceMatchesFlinkForPositiveNegativeAndClampedBounds() throws Exception {
        assertDataStreamParity(
                "SELECT ARRAY_SLICE(metric, -3), ARRAY_SLICE(metric, 0, -1), ARRAY_SLICE(metric, 1, 0), ARRAY_SLICE(metric, -123, 123), ARRAY_SLICE(metric, 20, 30) FROM array_input",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                Arrays.asList(
                        Row.of((Object) new Integer[] {null, 1, 2, 3, 4, 5, 6, null}),
                        Row.of((Object) new Integer[] {1, 2, 3, 4, 5}),
                        Row.of((Object) new Integer[] {}),
                        Row.of((Object) null)),
                "array_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    @Test
    void arraySliceMatchesFlinkForPerRowNullableBounds() throws Exception {
        TypeInformation<Row> metricType = Types.ROW_NAMED(
                new String[] {"items", "start_pos", "end_pos"}, Types.OBJECT_ARRAY(Types.INT), Types.INT, Types.INT);
        DataType logicalType = DataTypes.ROW(
                DataTypes.FIELD("items", DataTypes.ARRAY(DataTypes.INT())),
                DataTypes.FIELD("start_pos", DataTypes.INT()),
                DataTypes.FIELD("end_pos", DataTypes.INT()));

        assertDataStreamParity(
                "SELECT ARRAY_SLICE(metric.items, metric.start_pos), "
                        + "ARRAY_SLICE(metric.items, metric.start_pos, metric.end_pos) "
                        + "FROM dynamic_array_slice_input",
                metricType,
                logicalType,
                Arrays.asList(
                        Row.of(Row.of(new Integer[] {1, 2, 3, 4, 5}, 2, 4)),
                        Row.of(Row.of(new Integer[] {1, 2, 3, 4, 5}, -3, -1)),
                        Row.of(Row.of(new Integer[] {1, 2, 3}, 0, 0)),
                        Row.of(Row.of(new Integer[] {1, 2, 3}, -99, 99)),
                        Row.of(Row.of(new Integer[] {1, 2, 3}, null, 2)),
                        Row.of(Row.of(new Integer[] {1, 2, 3}, 1, null)),
                        Row.of(Row.of(new Integer[] {}, 1, 2)),
                        Row.of(Row.of(null, 1, 2)),
                        Row.of((Object) null)),
                "dynamic_array_slice_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }
}

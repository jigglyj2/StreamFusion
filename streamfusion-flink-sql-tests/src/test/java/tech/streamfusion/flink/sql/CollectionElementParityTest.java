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
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.Arrays;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class CollectionElementParityTest extends SqlParityTestSupport {
    @Test
    void elementMatchesFlinkForSingletonEmptyAndNullArrays() throws Exception {
        assertDataStreamParity(
                "SELECT ELEMENT(metric) FROM array_input",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                Arrays.asList(
                        Row.of((Object) new Integer[] {7}), Row.of((Object) new Integer[] {}), Row.of((Object) null)),
                "array_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    @Test
    void elementRejectsArraysWithMoreThanOneElementOnTheNativePath() {
        java.util.List<Row> rows = Arrays.asList(Row.of((Object) new Integer[] {1, 2}));
        Throwable flinkFailure = catchThrowable(() -> executeDataStream(
                "SELECT ELEMENT(metric) FROM flink_array_input",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                rows,
                "flink_array_input",
                false));
        Throwable streamFusionFailure = catchThrowable(() -> executeDataStream(
                "SELECT ELEMENT(metric) FROM native_array_input",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                rows,
                "native_array_input",
                true));

        assertThat(flinkFailure).isNotNull();
        assertThat(streamFusionFailure)
                .isNotNull()
                .hasStackTraceContaining("ELEMENT requires an array with at most one element");
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }
}

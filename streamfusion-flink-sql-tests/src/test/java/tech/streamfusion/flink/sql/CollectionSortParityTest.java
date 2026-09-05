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

import java.time.LocalDate;
import java.util.Arrays;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class CollectionSortParityTest extends SqlParityTestSupport {
    @Test
    void arraySortMatchesFlinkAcrossOrderAndNullPlacement() throws Exception {
        assertDataStreamParity(
                "SELECT ARRAY_SORT(metric), ARRAY_SORT(metric, FALSE), ARRAY_SORT(metric, TRUE, FALSE), ARRAY_SORT(metric, FALSE, TRUE) FROM array_input",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                Arrays.asList(
                        Row.of((Object) new Integer[] {1, 2, 2, null, -3}),
                        Row.of((Object) new Integer[] {null, null}),
                        Row.of((Object) new Integer[] {}),
                        Row.of((Object) null)),
                "array_input");
        assertNativeCalcRan();
    }

    @Test
    void arraySortMatchesFlinkForStringsAndDates() throws Exception {
        assertDataStreamParity(
                "SELECT ARRAY_SORT(metric), ARRAY_SORT(metric, FALSE) FROM string_array_input",
                Types.OBJECT_ARRAY(Types.STRING),
                DataTypes.ARRAY(DataTypes.STRING()),
                Arrays.asList(Row.of((Object) new String[] {"a", "cv", null, "234", "12", "ä"}), Row.of((Object) null)),
                "string_array_input");
        assertNativeCalcRan();

        assertDataStreamParity(
                "SELECT ARRAY_SORT(metric), ARRAY_SORT(metric, FALSE) FROM date_array_input",
                Types.OBJECT_ARRAY(Types.LOCAL_DATE),
                DataTypes.ARRAY(DataTypes.DATE()),
                Arrays.asList(
                        Row.of((Object) new LocalDate[] {LocalDate.of(2022, 1, 2), null, LocalDate.of(1969, 12, 31)}),
                        Row.of((Object) null)),
                "date_array_input");
        assertNativeCalcRan();
    }

    @Test
    void floatingPointArraySortFallsBackBecauseNanOrderingIsNotApproved() throws Exception {
        assertFallbackDataStreamParity(
                "SELECT ARRAY_SORT(metric) FROM double_array_input",
                Types.OBJECT_ARRAY(Types.DOUBLE),
                DataTypes.ARRAY(DataTypes.DOUBLE()),
                Arrays.asList(
                        Row.of((Object) new Double[] {Double.NaN, -0.0d, 0.0d, Double.POSITIVE_INFINITY}),
                        Row.of((Object) null)),
                "double_array_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isZero();
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("NaN ordering is intentionally excluded");
    }

    private static void assertNativeCalcRan() {
        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }
}

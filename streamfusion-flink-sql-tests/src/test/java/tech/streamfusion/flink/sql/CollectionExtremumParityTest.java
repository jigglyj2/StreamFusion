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

class CollectionExtremumParityTest extends SqlParityTestSupport {
    @Test
    void arrayMinimumAndMaximumMatchFlinkForIntegersAndStrings() throws Exception {
        assertDataStreamParity(
                "SELECT ARRAY_MIN(metric), ARRAY_MAX(metric) FROM int_array_input",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                Arrays.asList(
                        Row.of((Object) new Integer[] {3, null, -2, 9}),
                        Row.of((Object) new Integer[] {null, null}),
                        Row.of((Object) new Integer[] {}),
                        Row.of((Object) null)),
                "int_array_input");
        assertNativeCalcRan();

        assertDataStreamParity(
                "SELECT ARRAY_MIN(metric), ARRAY_MAX(metric) FROM string_array_input",
                Types.OBJECT_ARRAY(Types.STRING),
                DataTypes.ARRAY(DataTypes.STRING()),
                Arrays.asList(
                        Row.of((Object) new String[] {"z", null, "aa", "ä", "a"}),
                        Row.of((Object) new String[] {}),
                        Row.of((Object) null)),
                "string_array_input");
        assertNativeCalcRan();
    }

    @Test
    void arrayMinimumAndMaximumMatchFlinkForDates() throws Exception {
        assertDataStreamParity(
                "SELECT ARRAY_MIN(metric), ARRAY_MAX(metric) FROM date_array_input",
                Types.OBJECT_ARRAY(Types.LOCAL_DATE),
                DataTypes.ARRAY(DataTypes.DATE()),
                Arrays.asList(
                        Row.of((Object) new LocalDate[] {LocalDate.of(2024, 2, 29), null, LocalDate.of(1969, 12, 31)}),
                        Row.of((Object) new LocalDate[] {}),
                        Row.of((Object) null)),
                "date_array_input");
        assertNativeCalcRan();
    }

    @Test
    void floatingPointArrayExtremaFallBackBecauseNanOrderingDiffers() throws Exception {
        assertFallbackDataStreamParity(
                "SELECT ARRAY_MIN(metric), ARRAY_MAX(metric) FROM double_array_input",
                Types.OBJECT_ARRAY(Types.DOUBLE),
                DataTypes.ARRAY(DataTypes.DOUBLE()),
                Arrays.asList(
                        Row.of((Object) new Double[] {Double.NaN, -0.0d, 0.0d, Double.NEGATIVE_INFINITY, 7.0d}),
                        Row.of((Object) new Double[] {7.0d, Double.NaN, Double.POSITIVE_INFINITY}),
                        Row.of((Object) new Double[] {null, null}),
                        Row.of((Object) null)),
                "double_array_input");
        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isZero();
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Flink and DataFusion order NaN differently");
    }

    private static void assertNativeCalcRan() {
        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }
}

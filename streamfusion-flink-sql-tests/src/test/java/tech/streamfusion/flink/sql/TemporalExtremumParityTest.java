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

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class TemporalExtremumParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @MethodSource("queries")
    void timezoneFreeTemporalExtremaMatchFlinkByteForByte(String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    private static Stream<String> queries() {
        return Stream.of(
                timeQuery(0),
                timeQuery(3),
                timestampQuery(0),
                timestampQuery(3),
                timestampQuery(6),
                timestampQuery(9),
                "SELECT a FROM " + timestampInput(3) + " WHERE GREATEST(a, b) = b");
    }

    private static String timeQuery(int precision) {
        return "SELECT GREATEST(a, b), LEAST(a, b) FROM " + timeInput(precision);
    }

    private static String timeInput(int precision) {
        return "(VALUES (CAST('00:00:00.000' AS TIME("
                + precision
                + ")), CAST('22:11:22.987' AS TIME("
                + precision
                + "))), (CAST('12:34:56.123' AS TIME("
                + precision
                + ")), CAST('01:02:03.456' AS TIME("
                + precision
                + "))), (CAST(NULL AS TIME("
                + precision
                + ")), CAST('12:00:00.000' AS TIME("
                + precision
                + ")))) input(a, b)";
    }

    private static String timestampQuery(int precision) {
        return "SELECT GREATEST(a, b), LEAST(a, b) FROM " + timestampInput(precision);
    }

    private static String timestampInput(int precision) {
        return "(VALUES (CAST('1969-12-31 23:59:59.123456789' AS TIMESTAMP("
                + precision
                + ")), CAST('1970-01-01 00:00:00.000000000' AS TIMESTAMP("
                + precision
                + "))), (CAST('2024-02-29 12:34:56.987654321' AS TIMESTAMP("
                + precision
                + ")), CAST('9999-12-30 22:11:22.987654321' AS TIMESTAMP("
                + precision
                + "))), (CAST(NULL AS TIMESTAMP("
                + precision
                + ")), CAST('2000-02-29 00:00:00.000000000' AS TIMESTAMP("
                + precision
                + ")))) input(a, b)";
    }
}

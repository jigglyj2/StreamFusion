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

class TimePartParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @MethodSource("queries")
    void clockFieldsMatchFlinkByteForByte(String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    private static Stream<String> queries() {
        return Stream.of(
                "SELECT HOUR(time_value), MINUTE(time_value), SECOND(time_value) FROM " + input(0),
                "SELECT HOUR(time_value), MINUTE(time_value), SECOND(time_value) FROM " + input(3),
                subsecondQuery(0),
                subsecondQuery(3),
                "SELECT EXTRACT(HOUR FROM time_value), EXTRACT(MINUTE FROM time_value), "
                        + "EXTRACT(SECOND FROM time_value) FROM "
                        + input(3),
                "SELECT time_value FROM " + input(3) + " WHERE HOUR(time_value) >= 12");
    }

    private static String input(int precision) {
        return "(VALUES "
                + "(CAST(TIME '00:00:00' AS TIME("
                + precision
                + "))), "
                + "(CAST(TIME '12:34:56' AS TIME("
                + precision
                + "))), "
                + "(CAST(TIME '23:59:59' AS TIME("
                + precision
                + "))), "
                + "(CAST(NULL AS TIME("
                + precision
                + ")))) input(time_value)";
    }

    private static String subsecondQuery(int precision) {
        return "SELECT EXTRACT(MILLISECOND FROM time_value) FROM " + subsecondInput(precision);
    }

    private static String subsecondInput(int precision) {
        return "(VALUES "
                + "(CAST('00:00:00.000000000' AS TIME("
                + precision
                + "))), "
                + "(CAST('12:34:56.123456789' AS TIME("
                + precision
                + "))), "
                + "(CAST('22:11:22.987654321' AS TIME("
                + precision
                + "))), "
                + "(CAST(NULL AS TIME("
                + precision
                + ")))) input(time_value)";
    }
}

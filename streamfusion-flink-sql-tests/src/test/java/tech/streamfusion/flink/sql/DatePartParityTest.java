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

class DatePartParityTest extends SqlParityTestSupport {
    private static final String INPUT = "(VALUES (DATE '1969-12-29'), (DATE '1970-01-01'), "
            + "(DATE '2000-02-29'), (DATE '2020-12-31'), (DATE '2021-01-01'), "
            + "(DATE '9999-12-31'), (CAST(NULL AS DATE))) input(date_value)";

    @ParameterizedTest
    @MethodSource("queries")
    void calendarFieldsMatchFlinkByteForByte(String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    private static Stream<String> queries() {
        return Stream.of(
                "SELECT YEAR(date_value), QUARTER(date_value), MONTH(date_value), WEEK(date_value) FROM " + INPUT,
                "SELECT EXTRACT(YEAR FROM date_value), EXTRACT(MONTH FROM date_value) FROM " + INPUT,
                "SELECT DAYOFMONTH(date_value), DAYOFYEAR(date_value), DAYOFWEEK(date_value) FROM " + INPUT,
                "SELECT EXTRACT(DAY FROM date_value), EXTRACT(DOY FROM date_value), "
                        + "EXTRACT(DOW FROM date_value), EXTRACT(ISODOW FROM date_value), "
                        + "EXTRACT(ISOYEAR FROM date_value) FROM " + INPUT,
                "SELECT date_value FROM " + INPUT + " WHERE YEAR(date_value) = 2000");
    }
}

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

class DateExtremumParityTest extends SqlParityTestSupport {
    private static final String INPUT = "(VALUES (DATE '1969-12-31', DATE '1970-01-01'), "
            + "(DATE '2000-02-29', DATE '1999-12-31'), "
            + "(DATE '9999-12-31', DATE '0001-01-01'), "
            + "(CAST(NULL AS DATE), DATE '2024-02-29'), "
            + "(DATE '2024-02-29', CAST(NULL AS DATE))) input(a, b)";

    @ParameterizedTest
    @MethodSource("queries")
    void dateGreatestAndLeastMatchFlinkByteForByte(String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    private static Stream<String> queries() {
        return Stream.of(
                "SELECT GREATEST(a, b), LEAST(a, b) FROM " + INPUT,
                "SELECT a FROM " + INPUT + " WHERE GREATEST(a, b) = DATE '2000-02-29'",
                "SELECT LEAST(a, b, DATE '1970-01-01') FROM " + INPUT);
    }
}

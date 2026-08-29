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

class StringExtremumParityTest extends SqlParityTestSupport {
    private static final String INPUT = "(VALUES (CAST('alpha' AS VARCHAR), CAST('beta' AS VARCHAR)), "
            + "(CAST('é' AS VARCHAR), CAST('é' AS VARCHAR)), "
            + "(CAST('' AS VARCHAR), CAST('𐀀' AS VARCHAR)), "
            + "(CAST('' AS VARCHAR), CAST('x' AS VARCHAR)), "
            + "(CAST(NULL AS VARCHAR), CAST('value' AS VARCHAR))) input(a, b)";

    @ParameterizedTest
    @MethodSource("queries")
    void stringGreatestAndLeastMatchFlinkByteForByte(String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    private static Stream<String> queries() {
        return Stream.of(
                "SELECT GREATEST(a, b), LEAST(a, b) FROM " + INPUT,
                "SELECT GREATEST(a, b, CAST('middle' AS VARCHAR)) FROM " + INPUT,
                "SELECT a FROM " + INPUT + " WHERE LEAST(a, b) = a");
    }
}

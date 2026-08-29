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

class IntegerTruncateParityTest extends SqlParityTestSupport {
    private static final String INPUT = "(VALUES "
            + "(123456789, CAST(1234567890123456789 AS BIGINT), -2), "
            + "(-123456789, CAST(-1234567890123456789 AS BIGINT), -3), "
            + "(0, CAST(0 AS BIGINT), 4), "
            + "(CAST(NULL AS INT), CAST(NULL AS BIGINT), CAST(NULL AS INT))) "
            + "input(int_value, big_value, scale_value)";

    @ParameterizedTest
    @MethodSource("queries")
    void integerWidthsAndDynamicScalesMatchFlinkByteForByte(String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    private static Stream<String> queries() {
        return Stream.of(
                "SELECT TRUNCATE(int_value), TRUNCATE(int_value, scale_value), " + "TRUNCATE(big_value, -18) FROM "
                        + INPUT,
                "SELECT int_value FROM " + INPUT + " WHERE TRUNCATE(int_value, -4) = 123450000",
                "SELECT TRUNCATE(int_value + 1, scale_value) FROM " + INPUT);
    }
}

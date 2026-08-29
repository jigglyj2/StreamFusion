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

class IntegerExtremumParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @MethodSource("queries")
    void integerGreatestAndLeastMatchFlinkByteForByte(String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    private static Stream<String> queries() {
        String integers = "(VALUES (1, 3, 2), (-5, -2, -9), "
                + "(CAST(NULL AS INT), 1, 2), (4, CAST(NULL AS INT), 3)) input(a, b, c)";
        return Stream.of(
                "SELECT GREATEST(a, b, c), LEAST(a, b, c) FROM " + integers,
                "SELECT a FROM " + integers + " WHERE GREATEST(a, b, c) = 3",
                "SELECT GREATEST(v, CAST(1 AS TINYINT)) FROM (VALUES (CAST(-2 AS TINYINT)), "
                        + "(CAST(NULL AS TINYINT))) input(v)",
                "SELECT LEAST(v, CAST(256 AS SMALLINT)) FROM (VALUES (CAST(300 AS SMALLINT)), "
                        + "(CAST(NULL AS SMALLINT))) input(v)",
                "SELECT GREATEST(v, CAST(2147483648 AS BIGINT)) FROM "
                        + "(VALUES (CAST(1 AS BIGINT)), (CAST(NULL AS BIGINT))) input(v)");
    }
}

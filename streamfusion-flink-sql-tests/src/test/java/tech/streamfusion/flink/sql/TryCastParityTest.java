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

class TryCastParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @MethodSource("queries")
    void infallibleIntegerConversionsMatchFlinkByteForByte(String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    private static Stream<String> queries() {
        return Stream.of(
                "SELECT TRY_CAST(metric AS BIGINT) FROM (VALUES (CAST(-128 AS TINYINT)), (CAST(127 AS TINYINT)), (CAST(NULL AS TINYINT))) input(metric)",
                "SELECT TRY_CAST(metric AS BIGINT) FROM (VALUES (-2147483648), (2147483647), (CAST(NULL AS INT))) input(metric)",
                "SELECT metric FROM (VALUES (-1), (0), (7)) input(metric) WHERE TRY_CAST(metric AS BIGINT) IS NOT NULL");
    }
}

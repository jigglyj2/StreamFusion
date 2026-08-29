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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class NullIfParityTest extends SqlParityTestSupport {
    @ParameterizedTest(name = "{0}")
    @MethodSource("queries")
    void rewrittenConditionalMatchesFlinkByteForByte(String ignoredName, String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    private static Stream<Arguments> queries() {
        return Stream.of(
                Arguments.of(
                        "integer",
                        "SELECT NULLIF(lhs, rhs) FROM (VALUES (1, 1), (1, 2), (CAST(NULL AS INT), 1), (1, CAST(NULL AS INT))) input(lhs, rhs)"),
                Arguments.of(
                        "varchar",
                        "SELECT NULLIF(lhs, rhs) FROM (VALUES ('a', 'a'), ('a', 'b'), (CAST(NULL AS STRING), 'a'), ('a', CAST(NULL AS STRING))) input(lhs, rhs)"),
                Arguments.of(
                        "decimal",
                        "SELECT NULLIF(lhs, rhs) FROM (VALUES (1.20, 1.20), (1.20, 2.30), (CAST(NULL AS DECIMAL(3, 2)), 1.20)) input(lhs, rhs)"),
                Arguments.of(
                        "filter",
                        "SELECT lhs FROM (VALUES (1, 1), (1, 2), (3, 4)) input(lhs, rhs) WHERE NULLIF(lhs, rhs) IS NOT NULL"));
    }
}

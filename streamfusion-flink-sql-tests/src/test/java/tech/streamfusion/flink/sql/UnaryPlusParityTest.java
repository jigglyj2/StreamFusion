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

class UnaryPlusParityTest extends SqlParityTestSupport {
    @ParameterizedTest(name = "{0}")
    @MethodSource("queries")
    void identityOperatorMatchesFlinkByteForByte(String ignoredName, String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    private static Stream<Arguments> queries() {
        return Stream.of(
                Arguments.of(
                        "integer", "SELECT +metric FROM (VALUES (-1), (0), (7), (CAST(NULL AS INT))) input(metric)"),
                Arguments.of(
                        "bigint",
                        "SELECT +metric FROM (VALUES (CAST(-2147483649 AS BIGINT)), (CAST(2147483649 AS BIGINT)), (CAST(NULL AS BIGINT))) input(metric)"),
                Arguments.of(
                        "double",
                        "SELECT +metric FROM (VALUES (-0.0E0), (3.25E0), (CAST(NULL AS DOUBLE))) input(metric)"),
                Arguments.of(
                        "decimal",
                        "SELECT +metric FROM (VALUES (-12.34), (0.00), (CAST(NULL AS DECIMAL(4, 2)))) input(metric)"),
                Arguments.of("filter", "SELECT metric FROM (VALUES (-1), (0), (7)) input(metric) WHERE +metric > 0"));
    }
}

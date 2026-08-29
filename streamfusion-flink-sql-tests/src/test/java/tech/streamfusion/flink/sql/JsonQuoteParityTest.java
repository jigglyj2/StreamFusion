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

class JsonQuoteParityTest extends SqlParityTestSupport {
    private static final String INPUT = "(VALUES "
            + "('null'), ('\"null\"'), ('[1,2,3]'), ('a/b'), ('≠ will be escaped'), "
            + "('😀'), ('你好'), (''), (CAST(NULL AS STRING))) input(value_text)";

    @ParameterizedTest
    @MethodSource("queries")
    void flinkEscapingMatchesByteForByte(String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    private static Stream<String> queries() {
        return Stream.of(
                "SELECT JSON_QUOTE(value_text) FROM " + INPUT,
                "SELECT value_text FROM " + INPUT + " WHERE JSON_QUOTE(value_text) = '\"null\"'",
                "SELECT JSON_QUOTE(UPPER(value_text)) FROM " + INPUT,
                "SELECT JSON_QUOTE(CONCAT(value_text, CHR(10), CHR(9), CHR(8), CHR(12), CHR(13))) FROM " + INPUT);
    }
}

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

class UrlEncodeParityTest extends SqlParityTestSupport {
    private static final String INPUT = "(VALUES "
            + "('https://flink.apache.org/'), ('https%3A%2F%2Fflink.apache.org%2F'), "
            + "('inva lid://user:pass@host/file;param?query;p2'), ('.-*_'), ('a+b'), "
            + "('你好😀'), (''), (CAST(NULL AS STRING))) input(url_value)";

    @ParameterizedTest
    @MethodSource("queries")
    void formEncodingMatchesFlinkByteForByte(String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    private static Stream<String> queries() {
        return Stream.of(
                "SELECT URL_ENCODE(url_value) FROM " + INPUT,
                "SELECT url_value FROM " + INPUT + " WHERE URL_ENCODE(url_value) = 'a%2Bb'",
                "SELECT URL_ENCODE(LOWER(url_value)) FROM " + INPUT);
    }
}

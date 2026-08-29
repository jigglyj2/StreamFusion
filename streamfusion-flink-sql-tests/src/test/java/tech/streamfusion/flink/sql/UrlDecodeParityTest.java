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

class UrlDecodeParityTest extends SqlParityTestSupport {
    private static final String INPUT = "(VALUES "
            + "('https%3A%2F%2Fflink.apache.org%2F'), ('https://flink.apache.org/'), "
            + "('inva+lid%3A%2F%2Fuser%3Apass%40host%2Ffile%3Bparam%3Fquery%3Bp2'), "
            + "('%E4%BD%A0%E5%A5%BD%F0%9F%98%80'), ('%FF'), ('%C3é'), "
            + "('%'), ('%2G'), (''), (CAST(NULL AS STRING))) input(url_value)";

    @ParameterizedTest
    @MethodSource("queries")
    void formDecodingMatchesFlinkByteForByte(String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    private static Stream<String> queries() {
        return Stream.of(
                "SELECT URL_DECODE(url_value) FROM " + INPUT,
                "SELECT url_value FROM " + INPUT + " WHERE URL_DECODE(url_value) = '你好😀'",
                "SELECT URL_DECODE(LOWER(url_value)) FROM " + INPUT);
    }
}

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

class StringHashCodeParityTest extends SqlParityTestSupport {
    private static final String INPUT = "(VALUES "
            + "('abc'), ('a'), (''), ('你好'), ('😀'), ('a😀b'), ('polygenelubricants'), "
            + "(CAST(NULL AS STRING))) input(text_value)";

    @ParameterizedTest
    @MethodSource("queries")
    void javaStringHashMatchesFlinkByteForByte(String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    private static Stream<String> queries() {
        return Stream.of(
                "SELECT HASH_CODE(text_value) FROM " + INPUT,
                "SELECT text_value FROM " + INPUT + " WHERE HASH_CODE(text_value) = 96354",
                "SELECT HASH_CODE(UPPER(text_value)) FROM " + INPUT);
    }
}

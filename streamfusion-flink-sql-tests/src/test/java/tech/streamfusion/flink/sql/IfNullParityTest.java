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

class IfNullParityTest extends SqlParityTestSupport {
    private static final String INPUT =
            "(VALUES (1, 'one'), (CAST(NULL AS INT), 'missing'), " + "(-2, CAST(NULL AS VARCHAR(8)))) input(id, name)";

    @ParameterizedTest
    @MethodSource("queries")
    void canonicalIfNullMatchesFlinkByteForByte(String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    private static Stream<String> queries() {
        return Stream.of(
                "SELECT IFNULL(id, 9), IFNULL(name, 'fallback') FROM " + INPUT,
                "SELECT IFNULL(CAST(id AS BIGINT), CAST(2147483648 AS BIGINT)) FROM " + INPUT,
                "SELECT id FROM " + INPUT + " WHERE IFNULL(id, 0) > 0");
    }
}

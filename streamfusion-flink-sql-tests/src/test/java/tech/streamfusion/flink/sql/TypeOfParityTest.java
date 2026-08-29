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

class TypeOfParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @MethodSource("queries")
    void specializedTypeNamesMatchFlinkByteForByte(String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    private static Stream<String> queries() {
        String input = "(VALUES (1, CAST('abc' AS VARCHAR(8))), "
                + "(CAST(NULL AS INT), CAST(NULL AS VARCHAR(8)))) input(id, name)";
        return Stream.of(
                "SELECT TYPEOF(id), TYPEOF(name) FROM " + input,
                "SELECT TYPEOF(CAST(id AS DECIMAL(12, 3))), TYPEOF(ARRAY[id, id + 1]) FROM " + input,
                "SELECT TYPEOF(ROW(id, name), TRUE) FROM " + input,
                "SELECT id FROM " + input + " WHERE TYPEOF(id) = 'INT'");
    }
}

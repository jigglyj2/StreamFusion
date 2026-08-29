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

class BinaryStringParityTest extends SqlParityTestSupport {
    private static final String INPUT = "(VALUES "
            + "(CAST(42 AS TINYINT), CAST(43 AS SMALLINT), 44, CAST(3 AS BIGINT)), "
            + "(CAST(-1 AS TINYINT), CAST(-1 AS SMALLINT), -1, CAST(-1 AS BIGINT)), "
            + "(CAST(0 AS TINYINT), CAST(0 AS SMALLINT), 0, CAST(0 AS BIGINT)), "
            + "(CAST(NULL AS TINYINT), CAST(NULL AS SMALLINT), CAST(NULL AS INT), CAST(NULL AS BIGINT))) "
            + "input(tiny_value, small_value, int_value, big_value)";

    @ParameterizedTest
    @MethodSource("queries")
    void signedIntegerWidthsMatchFlinkByteForByte(String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    private static Stream<String> queries() {
        return Stream.of(
                "SELECT BIN(tiny_value), BIN(small_value), BIN(int_value), BIN(big_value) FROM " + INPUT,
                "SELECT int_value FROM " + INPUT + " WHERE BIN(big_value) = '11'",
                "SELECT BIN(int_value + 0) FROM " + INPUT);
    }
}

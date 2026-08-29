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

class DecimalExtremumParityTest extends SqlParityTestSupport {
    private static final String SAME_TYPE_INPUT =
            "(VALUES (CAST('-9999999999.9999' AS DECIMAL(14, 4)), CAST('1.2500' AS DECIMAL(14, 4))), "
                    + "(CAST('0.0000' AS DECIMAL(14, 4)), CAST('-0.0001' AS DECIMAL(14, 4))), "
                    + "(CAST(NULL AS DECIMAL(14, 4)), CAST('12.3400' AS DECIMAL(14, 4))), "
                    + "(CAST('12.3400' AS DECIMAL(14, 4)), CAST(NULL AS DECIMAL(14, 4)))) input(a, b)";

    @ParameterizedTest
    @MethodSource("queries")
    void decimalGreatestAndLeastMatchFlinkByteForByte(String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    private static Stream<String> queries() {
        return Stream.of(
                "SELECT GREATEST(a, b), LEAST(a, b) FROM " + SAME_TYPE_INPUT,
                "SELECT a FROM " + SAME_TYPE_INPUT + " WHERE GREATEST(a, b) = CAST('1.2500' AS DECIMAL(14, 4))",
                "SELECT GREATEST(a, b, CAST('0.0000' AS DECIMAL(14, 4))) FROM " + SAME_TYPE_INPUT,
                "SELECT GREATEST(CAST('12.34' AS DECIMAL(4, 2)), CAST('12.3456' AS DECIMAL(8, 4))), "
                        + "LEAST(CAST('-1' AS DECIMAL(1, 0)), CAST('-1.001' AS DECIMAL(4, 3)))",
                "SELECT GREATEST(CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0)), "
                        + "CAST('-99999999999999999999999999999999999999' AS DECIMAL(38, 0)))");
    }
}

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

class InetNtoaParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @MethodSource("queries")
    void numericIpv4FormattingMatchesFlinkByteForByte(String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    private static Stream<String> queries() {
        String bigints = "(VALUES (CAST(0 AS BIGINT)), (CAST(1 AS BIGINT)), "
                + "(CAST(2130706433 AS BIGINT)), (CAST(4294967295 AS BIGINT)), "
                + "(CAST(-1 AS BIGINT)), (CAST(4294967296 AS BIGINT)), "
                + "(CAST(NULL AS BIGINT))) input(ip_value)";
        return Stream.of(
                "SELECT INET_NTOA(ip_value) FROM " + bigints,
                "SELECT ip_value FROM " + bigints + " WHERE INET_NTOA(ip_value) = '127.0.0.1'",
                "SELECT INET_NTOA(ip_value) FROM (VALUES (CAST(127 AS TINYINT)), "
                        + "(CAST(-1 AS TINYINT)), (CAST(NULL AS TINYINT))) input(ip_value)",
                "SELECT INET_NTOA(ip_value) FROM (VALUES (CAST(256 AS SMALLINT)), "
                        + "(CAST(-1 AS SMALLINT)), (CAST(NULL AS SMALLINT))) input(ip_value)",
                "SELECT INET_NTOA(ip_value) FROM (VALUES (2130706433), (-1), "
                        + "(CAST(NULL AS INT))) input(ip_value)");
    }
}

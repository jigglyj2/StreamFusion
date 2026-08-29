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

class InetAtonParityTest extends SqlParityTestSupport {
    private static final String INPUT = "(VALUES ('127.0.0.1'), ('0.0.0.0'), "
            + "('255.255.255.255'), ('127.1'), ('127.0.1'), ('010.000.000.001'), "
            + "('1'), ('255'), ('256'), (''), ('invalid'), ('1.2.3.4.5'), "
            + "('1.2.3.'), ('.1.2.3'), ('1..2.3'), (' 127.0.0.1'), "
            + "('127.0.0.1 '), (CAST(NULL AS VARCHAR))) input(ip_value)";

    @ParameterizedTest
    @MethodSource("queries")
    void mysqlCompatibleIpv4ParsingMatchesFlinkByteForByte(String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    private static Stream<String> queries() {
        return Stream.of(
                "SELECT INET_ATON(ip_value) FROM " + INPUT,
                "SELECT ip_value FROM " + INPUT + " WHERE INET_ATON(ip_value) = 2130706433",
                "SELECT INET_NTOA(INET_ATON(ip_value)) FROM " + INPUT);
    }
}

/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TimestampPartParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @MethodSource("queries")
    void upstreamClockCasesMatchThroughTheSqlHarness(String sql) throws Exception {
        assertParity(sql, true);
    }

    private static Stream<String> queries() {
        // Flink 2.3 TimeFunctionsITCase.extractTestCases clock inputs, restricted
        // explicitly to the admitted millisecond precision. Keep the upstream
        // null/leap-day/subsecond cases alongside generated full-range coverage.
        String input = "(VALUES (TIMESTAMP '2000-01-31 11:22:33.123'), "
                + "(TIMESTAMP '2020-02-29 01:56:59.987'), "
                + "(CAST(NULL AS TIMESTAMP(3)))) input(ts)";
        return Stream.of(
                "SELECT EXTRACT(HOUR FROM ts), EXTRACT(MINUTE FROM ts), "
                        + "EXTRACT(SECOND FROM ts), EXTRACT(MILLISECOND FROM ts) FROM " + input,
                "SELECT ts FROM " + input + " WHERE HOUR(ts) >= 8 AND MINUTE(ts) < 30");
    }
}

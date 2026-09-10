/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class SelectedWindowJoinSqlIntegrationTest extends SqlParityTestSupport {
    @ParameterizedTest
    @CsvSource({"false,3,1", "true,3,1", "false,71,2", "true,71,2"})
    void reusedWindowCountsAndMaximumUseOrdinaryWindowJoinSelection(boolean rocks, int seed, int parallelism)
            throws Exception {
        String sql = "WITH counts AS (" + SelectedWindowSqlFixture.counts(6)
                + ") SELECT a.k,a.n FROM counts a JOIN (SELECT MAX(n) m,s,e FROM counts GROUP BY s,e) b "
                + "ON a.s=b.s AND a.e=b.e AND a.n>=b.m";
        byte[] expected = SelectedWindowSqlFixture.execute(false, rocks, sql, seed, parallelism, false);
        byte[] actual = SelectedWindowSqlFixture.execute(true, rocks, sql, seed, parallelism, false);
        assertThat(actual).isNotEmpty().isEqualTo(expected);
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
    }
}

/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

/** Ordinary SQL selection over generated nullable keys, negative times, gaps and shuffled arrivals. */
class SelectedSessionWindowSqlIntegrationTest extends SqlParityTestSupport {
    @Test
    void generatedSessionsPreserveCompleteChangelogBytesOnBothBackends() throws Exception {
        for (boolean rocks : List.of(false, true))
            for (int seed : List.of(3, 19, 71)) {
                int seconds = seed == 19 ? 10 : 1;
                int parallelism = seed == 71 ? 2 : 1;
                String sql = "WITH filtered AS (SELECT * FROM window_input"
                        + (seed == 19 ? " WHERE k IS NULL" : "")
                        + ") SELECT k, COUNT(*) n, window_start, window_end, window_time "
                        + "FROM TABLE(SESSION(TABLE filtered PARTITION BY k, DESCRIPTOR(ts), INTERVAL '"
                        + seconds + "' SECOND)) GROUP BY k, window_start, window_end, window_time";
                var expected = SelectedWindowSqlFixture.execute(false, rocks, sql, seed, parallelism);
                var actual = SelectedWindowSqlFixture.execute(true, rocks, sql, seed, parallelism);
                assertThat(actual)
                        .as("rocks=%s seed=%s", rocks, seed)
                        .isNotEmpty()
                        .isEqualTo(expected);
                assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
                assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
            }
    }

    @Test
    void sessionRowtimeFeedsTwoPhaseWindowsWithOriginalFlinkResourceShares() throws Exception {
        String sql = "WITH sessions AS (SELECT k, COUNT(*) n, window_start s, window_end e, window_time rt "
                + "FROM TABLE(SESSION(TABLE window_input PARTITION BY k, DESCRIPTOR(ts), INTERVAL '1' SECOND)) "
                + "GROUP BY k, window_start, window_end, window_time) "
                + "SELECT k, MAX(n) mx, window_start, window_end "
                + "FROM TABLE(TUMBLE(TABLE sessions, DESCRIPTOR(rt), INTERVAL '60' SECOND)) "
                + "GROUP BY k, window_start, window_end";
        for (boolean rocks : List.of(false, true)) {
            var expected = SelectedWindowSqlFixture.execute(false, rocks, sql, 71, 2);
            var actual = SelectedWindowSqlFixture.execute(true, rocks, sql, 71, 2);
            assertThat(actual).isNotEmpty().isEqualTo(expected);
            assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
            assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
        }
    }
}

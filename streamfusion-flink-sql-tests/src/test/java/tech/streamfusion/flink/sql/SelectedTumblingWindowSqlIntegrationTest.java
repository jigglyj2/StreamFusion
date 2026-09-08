/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class SelectedTumblingWindowSqlIntegrationTest extends SqlParityTestSupport {
    @Test
    void generatedCountAndExtremaMatchFlinkIncludingAllNullWindows() throws Exception {
        for (boolean rocks : List.of(false, true))
            for (int seed : List.of(3, 19, 71)) {
                int seconds = seed == 19 ? 10 : 2;
                int parallelism = seed == 71 ? 2 : 1;
                boolean grouped = seed == 71;
                String sql = "WITH filtered AS (SELECT * FROM window_input"
                        + (seed == 19 ? " WHERE k IS NULL" : "")
                        + ") SELECT " + (grouped ? "k, " : "")
                        + "MAX(k) mx, MIN(k) mn, COUNT(k) n, window_start, window_end "
                        + "FROM TABLE(TUMBLE(TABLE filtered, DESCRIPTOR(ts), INTERVAL '" + seconds + "' SECOND)) "
                        + "GROUP BY " + (grouped ? "k, " : "") + "window_start, window_end";
                var expected = SelectedWindowSqlFixture.execute(false, rocks, sql, seed, parallelism);
                var actual = SelectedWindowSqlFixture.execute(true, rocks, sql, seed, parallelism);
                assertThat(actual)
                        .as("rocks=%s seed=%s", rocks, seed)
                        .isNotEmpty()
                        .isEqualTo(expected);
                assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
                assertThat(StreamFusionPlannerFactory.nativeLocalWindowAggregateBatchCount())
                        .isZero();
            }
    }
}

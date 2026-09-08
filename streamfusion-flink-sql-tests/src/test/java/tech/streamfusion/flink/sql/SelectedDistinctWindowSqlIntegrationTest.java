/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class SelectedDistinctWindowSqlIntegrationTest extends SqlParityTestSupport {
    @Test
    void generatedDistinctWindowsMatchFlinkForNullableCompositeKeys() throws Exception {
        for (boolean rocks : List.of(false, true))
            for (int seed : List.of(3, 19, 71)) {
                int seconds = seed == 19 ? 10 : 2;
                int parallelism = seed == 71 ? 2 : 1;
                String sql = "SELECT k, " + (seed == 3 ? "" : "label, ") + "window_start, window_end "
                        + "FROM TABLE(TUMBLE(TABLE window_input, DESCRIPTOR(ts), INTERVAL '" + seconds + "' SECOND)) "
                        + "GROUP BY k, " + (seed == 3 ? "" : "label, ") + "window_start, window_end";
                var expected = SelectedWindowSqlFixture.execute(false, rocks, sql, seed, parallelism, false, true);
                var actual = SelectedWindowSqlFixture.execute(true, rocks, sql, seed, parallelism, false, true);
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

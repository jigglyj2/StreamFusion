/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class SelectedSharedWindowSqlIntegrationTest extends SqlParityTestSupport {
    @Test
    void generatedReusedCountAndAttachedMaxChangelogsMatchFlink() throws Exception {
        for (boolean rocks : List.of(false, true))
            for (int seed : List.of(3, 19, 71)) {
                int seconds = seed == 19 ? 10 : 6;
                int parallelism = seed == 71 ? 2 : 1;
                String sql = "WITH counts AS (" + SelectedWindowSqlFixture.counts(seconds)
                        + ") SELECT a.k, a.n FROM counts a JOIN (SELECT MAX(n) m, s, e FROM counts GROUP BY s, e) b "
                        + "ON a.s=b.s AND a.e=b.e AND a.n>=b.m";
                var flink = SelectedWindowSqlFixture.execute(false, rocks, sql, seed, parallelism, true);
                var actual = SelectedWindowSqlFixture.execute(true, rocks, sql, seed, parallelism, true);
                assertThat(actual)
                        .as("rocks=%s seed=%s parallelism=%s", rocks, seed, parallelism)
                        .isEqualTo(flink);
                assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
                assertThat(StreamFusionPlannerFactory.nativeLocalWindowAggregateBatchCount())
                        .isZero();
            }
    }
}

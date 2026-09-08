/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class SelectedLocalWindowSqlIntegrationTest extends SqlParityTestSupport {
    @Test
    void generatedDirectAndAttachedHopGraphsMatchFlinkOnBothBackends() throws Exception {
        for (boolean rocks : List.of(false, true))
            for (boolean attached : List.of(false, true))
                for (int seed : List.of(3, 19, 71)) {
                    var flink = execute(false, rocks, attached, seed);
                    var nativeResult = execute(true, rocks, attached, seed);
                    assertThat(nativeResult)
                            .as("rocks=%s attached=%s seed=%s", rocks, attached, seed)
                            .isEqualTo(flink);
                    assertThat(StreamFusionPlannerFactory.nativePlanBatchCount())
                            .isPositive();
                    assertThat(StreamFusionPlannerFactory.nativeLocalWindowAggregateBatchCount())
                            .isZero();
                }
    }

    private static byte[] execute(boolean selected, boolean rocks, boolean attached, int seed) throws Exception {
        String counts = SelectedWindowSqlFixture.counts(6);
        String sql = attached ? "SELECT MAX(n) m, s, e FROM (" + counts + ") counts GROUP BY s, e" : counts;
        return SelectedWindowSqlFixture.execute(selected, rocks, sql, seed, 1);
    }
}

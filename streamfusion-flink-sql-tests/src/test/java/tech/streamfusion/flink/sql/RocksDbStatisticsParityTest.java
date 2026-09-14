/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.rocksdb.RocksDBNativeMetricOptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.RocksDbStatisticsProfiles;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class RocksDbStatisticsParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void tickerSelectionPreservesGeneratedCompleteChangelogsOnBothBackends(boolean all) throws Exception {
        var config = all ? RocksDbStatisticsProfiles.allTickers() : new Configuration();
        config.set(RocksDBNativeMetricOptions.MONITOR_BYTES_WRITTEN, true);
        for (boolean rocks : new boolean[] {false, true}) {
            var expected = GroupAggregateAdmissionParityTest.executeFilteredRetractions(false, rocks, 0.7, 0.2, config);
            var actual = GroupAggregateAdmissionParityTest.executeFilteredRetractions(true, rocks, 0.7, 0.2, config);
            assertThat(actual).isEqualTo(expected);
            assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
            assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
        }
    }
}

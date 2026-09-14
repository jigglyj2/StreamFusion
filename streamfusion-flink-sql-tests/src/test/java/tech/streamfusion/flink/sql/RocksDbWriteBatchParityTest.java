/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.state.rocksdb.RocksDBConfigurableOptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.RocksDbConfigurationProfiles;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class RocksDbWriteBatchParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @ValueSource(longs = {0, 1, 50, 4096, 2097152})
    void boundedDirtyFlushesPreserveGeneratedCompleteChangelogs(long bytes) throws Exception {
        var config = RocksDbConfigurationProfiles.indexOptions(1);
        config.set(RocksDBConfigurableOptions.WRITE_BATCH_SIZE, new MemorySize(bytes));
        for (boolean rocks : new boolean[] {false, true}) {
            var expected = GroupAggregateAdmissionParityTest.executeFilteredRetractions(false, rocks, 0.7, 0.2, config);
            var actual = GroupAggregateAdmissionParityTest.executeFilteredRetractions(true, rocks, 0.7, 0.2, config);
            assertThat(actual).isEqualTo(expected);
            assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
            assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
        }
    }
}

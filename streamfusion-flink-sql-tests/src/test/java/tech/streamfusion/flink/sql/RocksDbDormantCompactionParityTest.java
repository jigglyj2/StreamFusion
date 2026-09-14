/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.apache.flink.state.rocksdb.sstmerge.RocksDBManualCompactionOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.RocksDbDormantCompactionProfiles;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class RocksDbDormantCompactionParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @ValueSource(longs = {0, 1, 999999})
    void dormantSettingsPreserveOrdinaryAccelerationAndGeneratedFlinkChangelogs(long nanos) throws Exception {
        for (boolean rocks : new boolean[] {false, true}) {
            var options = RocksDbDormantCompactionProfiles.disabled(nanos);
            byte[] expected =
                    GroupAggregateAdmissionParityTest.executeFilteredRetractions(false, rocks, 0.7, 0.2, options);
            byte[] actual =
                    GroupAggregateAdmissionParityTest.executeFilteredRetractions(true, rocks, 0.7, 0.2, options);
            assertThat(actual).isEqualTo(expected);
            assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
            assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
        }
    }

    @Test
    void enablingTheSchedulerStillFallsBackAsAWholePlan() throws Exception {
        var options = RocksDbDormantCompactionProfiles.disabled(0);
        options.set(RocksDBManualCompactionOptions.MIN_INTERVAL, Duration.ofSeconds(30));
        byte[] expected = GroupAggregateAdmissionParityTest.executeFilteredRetractions(false, true, 0.7, 0.2, options);
        byte[] actual = GroupAggregateAdmissionParityTest.executeFilteredRetractions(true, true, 0.7, 0.2, options);
        assertThat(actual).isEqualTo(expected);
        assertThat(StreamFusionPlanningDiagnostics.explain())
                .contains(
                        "Accelerated: no",
                        RocksDBManualCompactionOptions.MIN_INTERVAL.key(),
                        "manual compaction scheduler");
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
    }
}

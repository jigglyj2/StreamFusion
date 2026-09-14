/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class RocksDbUnsupportedConfigurationParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @CsvSource({
        "state.backend.rocksdb.memory.fixed-per-slot,16 mb,memory.fixed-per-slot",
        "state.backend.rocksdb.memory.managed,false,memory.managed",
        "state.backend.rocksdb.timer-service.factory,HEAP,timer-service.factory",
        "state.backend.rocksdb.use-ingest-db-restore-mode,true,use-ingest-db-restore-mode",
        "state.backend.rocksdb.metrics.estimate-num-keys,true,metrics",
        "state.backend.latency-track.keyed-state-enabled,true,latency",
        "execution.checkpointing.during-recovery.enabled,true,checkpointing during channel recovery"
    })
    void completeFallbackPreservesGeneratedChangelogsAndLeavesTheFlinkBackendUsable(
            String key, String value, String reason) throws Exception {
        var config = new Configuration();
        config.setString(key, value);
        var expected = GroupAggregateAdmissionParityTest.executeFilteredRetractions(false, true, 0.7, 0.2, config);
        var actual = GroupAggregateAdmissionParityTest.executeFilteredRetractions(true, true, 0.7, 0.2, config);
        assertThat(actual).isEqualTo(expected);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: no", reason);
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
    }
}

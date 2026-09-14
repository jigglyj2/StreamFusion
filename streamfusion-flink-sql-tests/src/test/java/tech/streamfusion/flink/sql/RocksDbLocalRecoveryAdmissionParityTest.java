/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class RocksDbLocalRecoveryAdmissionParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @ValueSource(strings = {"execution.state-recovery.from-local", "state.backend.local-recovery"})
    void currentAndDeprecatedSettingsKeepOrdinarySqlAccelerationAndExactChangelogs(String key) throws Exception {
        var options = new Configuration();
        options.setString(key, "true");
        var expected = GroupAggregateAdmissionParityTest.executeFilteredRetractions(false, true, 0.7, 0.2, options);
        var actual = GroupAggregateAdmissionParityTest.executeFilteredRetractions(true, true, 0.7, 0.2, options);
        assertThat(actual).isEqualTo(expected);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
    }
}

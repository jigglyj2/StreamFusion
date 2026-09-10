/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

@ResourceLock("streamfusion-planner-property")
class NexmarkQ21ProductionIT {
    @AfterEach
    void clearPlanner() {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
    }

    @ParameterizedTest
    @CsvSource({"hashmap,1", "rocksdb,1", "hashmap,4", "rocksdb,4"})
    void channelIdentifiersAndBlackholeCountsMatchFlink(String backend, int parallelism) throws Exception {
        var flink = LocalRowDataNexmarkBenchmark.run(50_000, "q21", false, backend, parallelism);
        assertThat(flink.completed()).isTrue();
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
        var nativeResult = LocalRowDataNexmarkBenchmark.run(50_000, "q21", true, backend, parallelism);
        assertThat(nativeResult.completed()).isTrue();
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
        assertThat(nativeResult.materializedSha256()).isEqualTo(flink.materializedSha256());
        assertThat(nativeResult.outputRows()).isEqualTo(flink.outputRows()).isPositive();
        assertThat(nativeResult.outputSha256()).isEqualTo(flink.outputSha256());
        if (parallelism == 1) assertThat(nativeResult.orderedSha256()).isEqualTo(flink.orderedSha256());
        StreamFusionPlannerFactory.resetMetrics();
        long flinkBlackhole = NexmarkRowDataJob.runBlackhole(50_000, "q21", false, backend, parallelism, false);
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
        StreamFusionPlannerFactory.resetMetrics();
        long nativeBlackhole = NexmarkRowDataJob.runBlackhole(50_000, "q21", true, backend, parallelism, false);
        assertThat(nativeBlackhole).isEqualTo(flinkBlackhole).isPositive();
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
    }
}

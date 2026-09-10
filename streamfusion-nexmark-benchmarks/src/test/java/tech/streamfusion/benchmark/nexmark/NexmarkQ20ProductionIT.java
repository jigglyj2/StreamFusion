/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

@ResourceLock("streamfusion-planner-property")
class NexmarkQ20ProductionIT {
    @AfterEach
    void clearPlanner() {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
        System.clearProperty(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED.key());
    }

    @ParameterizedTest
    @CsvSource({
        "hashmap,1,false",
        "rocksdb,1,false",
        "hashmap,4,false",
        "rocksdb,4,false",
        "hashmap,1,true",
        "rocksdb,1,true",
        "hashmap,4,true",
        "rocksdb,4,true"
    })
    void expandedBidsAndBlackholeCountsMatchFlink(String backend, int parallelism, boolean multiJoin) throws Exception {
        System.setProperty(
                OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED.key(), Boolean.toString(multiJoin));
        var flink = LocalRowDataNexmarkBenchmark.run(50_000, "q20", false, backend, parallelism);
        assertThat(flink.completed()).isTrue();
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
        var nativeResult = LocalRowDataNexmarkBenchmark.run(50_000, "q20", true, backend, parallelism);
        assertThat(nativeResult.completed()).isTrue();
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
        assertThat(nativeResult.materializedSha256()).isEqualTo(flink.materializedSha256());
        assertThat(nativeResult.outputRows()).isEqualTo(flink.outputRows()).isPositive();
        assertThat(nativeResult.outputSha256()).isEqualTo(flink.outputSha256());
        // Two independently scheduled join inputs may interleave outputs differently.
        // Fixed-arrival shared join tests compare every ordered changelog byte.
        StreamFusionPlannerFactory.resetMetrics();
        long flinkBlackhole = NexmarkRowDataJob.runBlackhole(50_000, "q20", false, backend, parallelism, false);
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
        StreamFusionPlannerFactory.resetMetrics();
        long nativeBlackhole = NexmarkRowDataJob.runBlackhole(50_000, "q20", true, backend, parallelism, false);
        assertThat(nativeBlackhole).isEqualTo(flinkBlackhole).isPositive();
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
    }
}

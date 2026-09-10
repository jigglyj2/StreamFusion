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

/** Official Q8 distinct windows under default WindowJoin and enabled MultiJoin selection. */
@ResourceLock("streamfusion-planner-property")
class NexmarkQ8ProductionIT {
    @AfterEach
    void clearPlanner() {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        System.clearProperty(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED.key());
        System.clearProperty("streamfusion.nexmark.mini-batch");
        StreamFusionPlannerFactory.resetMetrics();
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
    void bothJoinSelectionsMatchCollectedBytesAndBlackholeCounts(String backend, int parallelism, boolean multiJoin)
            throws Exception {
        System.setProperty(
                OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED.key(), Boolean.toString(multiJoin));
        System.setProperty("streamfusion.nexmark.mini-batch", "false");
        var flink = LocalRowDataNexmarkBenchmark.run(10_000, "q8", false, backend, parallelism);
        assertThat(flink.completed()).isTrue();
        assertThat(flink.nativePlanBatches()).isZero();
        var accelerated = LocalRowDataNexmarkBenchmark.run(10_000, "q8", true, backend, parallelism);
        assertThat(accelerated.completed()).isTrue();
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(accelerated.nativePlanBatches()).isPositive();
        assertThat(accelerated.outputRows()).isEqualTo(flink.outputRows()).isPositive();
        assertThat(accelerated.outputSha256()).isEqualTo(flink.outputSha256());
        assertThat(accelerated.debugRows()).containsExactlyElementsOf(flink.debugRows());
        assertThat(accelerated.materializedSha256()).isEqualTo(flink.materializedSha256());
        assertThat(accelerated.nativeLocalWindowAggregateBatches()).isZero();
        // Independent network inputs may interleave windows. Fixed-arrival operator tests
        // compare the ordered changelog; this compares every collected record's bytes.
        StreamFusionPlannerFactory.resetMetrics();
        long baselineRows = NexmarkRowDataJob.runBlackhole(10_000, "q8", false, backend, parallelism, false);
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
        StreamFusionPlannerFactory.resetMetrics();
        long nativeRows = NexmarkRowDataJob.runBlackhole(10_000, "q8", true, backend, parallelism, false);
        assertThat(nativeRows).isEqualTo(baselineRows).isEqualTo(flink.outputRows());
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
    }
}

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

/** Official Q9 parity under both default and enabled multi-join planner selection. */
@ResourceLock("streamfusion-planner-property")
class NexmarkQ9ProductionIT {
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
    void topOneAndRangeJoinPreserveMaterializedNexmarkResults(String backend, int parallelism, boolean multiJoin)
            throws Exception {
        System.setProperty(
                OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED.key(), Boolean.toString(multiJoin));
        System.setProperty("streamfusion.nexmark.mini-batch", "false");
        var flink = LocalRowDataNexmarkBenchmark.run(10_000, "q9", false, backend, parallelism);
        assertThat(flink.completed()).isTrue();
        assertThat(flink.outputRows()).isPositive();
        assertThat(flink.nativePlanBatches()).isZero();
        var accelerated = LocalRowDataNexmarkBenchmark.run(10_000, "q9", true, backend, parallelism);
        assertThat(accelerated.completed()).isTrue();
        // Repeated unmodified Flink jobs produce different intermediate winning-bid
        // transitions as the join inputs interleave. Fixed-arrival native/Flink harnesses
        // compare the complete changelog; this independent-job check compares final bytes.
        assertThat(accelerated.outputRows()).isPositive();
        assertThat(accelerated.materializedRows()).isPositive().isEqualTo(flink.materializedRows());
        assertThat(accelerated.materializedDebugRows()).containsExactlyElementsOf(flink.materializedDebugRows());
        assertThat(accelerated.materializedSha256()).isEqualTo(flink.materializedSha256());
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(accelerated.nativePlanBatches()).isPositive();
        assertThat(accelerated.nativeTopNBatches()).isZero();

        StreamFusionPlannerFactory.resetMetrics();
        assertThat(NexmarkRowDataJob.runBlackhole(10_000, "q9", false, backend, parallelism, false))
                .isPositive();
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
        StreamFusionPlannerFactory.resetMetrics();
        assertThat(NexmarkRowDataJob.runBlackhole(10_000, "q9", true, backend, parallelism, false))
                .isPositive();
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
    }
}

/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

/** Opt-in official Q9 parity through ordinary planner selection and the Nexmark RowData source. */
@ResourceLock("streamfusion-planner-property")
class NexmarkQ9ProductionIT {
    @AfterEach
    void clearPlanner() {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void topOneAndRangeJoinPreserveMaterializedNexmarkResults(String backend) throws Exception {
        for (int parallelism : List.of(1, 4)) {
            var flink = LocalRowDataNexmarkBenchmark.run(10_000, "q9", false, backend, parallelism);
            assertThat(flink.completed()).isTrue();
            assertThat(flink.outputRows()).isPositive();
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
        }
    }
}

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

/** Opt-in official Q7 parity through ordinary planner selection and the Nexmark RowData source. */
@ResourceLock("streamfusion-planner-property")
class NexmarkQ7ProductionIT {
    @AfterEach
    void clearPlanner() {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void tumblingWindowAndTimestampJoinPreserveCompleteNexmarkResults(String backend) throws Exception {
        for (int parallelism : List.of(1, 4)) {
            var flink = LocalRowDataNexmarkBenchmark.run(10_000, "q7", false, backend, parallelism);
            assertThat(flink.completed()).isTrue();
            assertThat(flink.outputRows()).isPositive();
            var accelerated = LocalRowDataNexmarkBenchmark.run(10_000, "q7", true, backend, parallelism);
            assertThat(accelerated.completed()).isTrue();
            assertThat(accelerated.outputRows()).isEqualTo(flink.outputRows());
            assertThat(accelerated.outputSha256()).isEqualTo(flink.outputSha256());
            assertThat(accelerated.debugRows()).containsExactlyElementsOf(flink.debugRows());
            assertThat(accelerated.materializedSha256()).isEqualTo(flink.materializedSha256());
            assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
            assertThat(accelerated.nativePlanBatches()).isPositive();
            assertThat(accelerated.nativeLocalWindowAggregateBatches()).isZero();
        }
    }
}

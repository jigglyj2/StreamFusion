/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

@ResourceLock("streamfusion-planner-property")
class NexmarkQ13ProductionIT {
    @AfterEach
    void clearPlanner() {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
    }

    @Test
    void catalogSelectIsTheUnchangedUpstreamQuery() throws Exception {
        try (var input = getClass().getResourceAsStream("/queries/q13.sql")) {
            assertThat(input).isNotNull();
            var original = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            var select = original.substring(original.indexOf("SELECT\n", original.indexOf("INSERT INTO")))
                    .replace(";", "")
                    .trim();
            assertThat(NexmarkRowDataQueryCatalog.load("q13").trim()).isEqualTo(select);
        }
    }

    @ParameterizedTest
    @CsvSource({"hashmap,1", "rocksdb,1", "hashmap,4", "rocksdb,4"})
    void fullEnrichmentBytesAndBlackholeCountMatchFlink(String backend, int parallelism) throws Exception {
        var flink = LocalRowDataNexmarkBenchmark.run(50_000, "q13", false, backend, parallelism);
        assertThat(flink.completed()).isTrue();
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
        var nativeResult = LocalRowDataNexmarkBenchmark.run(50_000, "q13", true, backend, parallelism);
        assertThat(nativeResult.completed()).isTrue();
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
        assertThat(nativeResult.outputRows()).isEqualTo(flink.outputRows()).isPositive();
        assertThat(nativeResult.outputSha256()).isEqualTo(flink.outputSha256());
        assertThat(nativeResult.materializedSha256()).isEqualTo(flink.materializedSha256());
        long blackhole = NexmarkRowDataJob.runBlackhole(50_000, "q13", true, backend, parallelism, false);
        assertThat(blackhole).isEqualTo(flink.outputRows());
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
    }
}

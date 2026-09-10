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
class NexmarkQ18ProductionIT {
    @AfterEach
    void clearPlanner() {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
    }

    @Test
    void catalogSelectIsTheUnchangedUpstreamQuery() throws Exception {
        try (var input = getClass().getResourceAsStream("/queries/q18.sql")) {
            assertThat(input).isNotNull();
            var original = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            // Formatting and the explicit bid qualifier do not change the upstream SELECT.
            var select = original.substring(original.indexOf("SELECT", original.indexOf("INSERT INTO")));
            assertThat(normalize(NexmarkRowDataQueryCatalog.load("q18"))).isEqualTo(normalize(select));
            assertThat(NexmarkRowDataQueryCatalog.sinkColumns("q18")).doesNotContain("PRIMARY KEY");
        }
    }

    private static String normalize(String sql) {
        return sql.replace("bid.*", "*").replace(";", "").replaceAll("\\s+", "").trim();
    }

    private static void legalWinners(
            java.util.List<String> rows, java.util.Map<java.util.List<Long>, java.util.Set<String>> candidates) {
        var seen = new java.util.HashSet<java.util.List<Long>>();
        for (String row : rows) {
            var fields = row.substring(3, row.indexOf(']')).split(", ", 3);
            var key = java.util.List.of(Long.parseLong(fields[0]), Long.parseLong(fields[1]));
            assertThat(seen.add(key)).as("one materialized winner for %s", key).isTrue();
            assertThat(candidates.get(key))
                    .as("legal source-order winners for %s", key)
                    .contains(row);
        }
        assertThat(seen).containsExactlyInAnyOrderElementsOf(candidates.keySet());
    }

    @ParameterizedTest
    @CsvSource({"hashmap,1", "rocksdb,1", "hashmap,4", "rocksdb,4"})
    void lastBidsAndBlackholeCountsMatchFlink(String backend, int parallelism) throws Exception {
        var flink = LocalRowDataNexmarkBenchmark.run(50_000, "q18", false, backend, parallelism);
        assertThat(flink.completed()).isTrue();
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
        var nativeResult = LocalRowDataNexmarkBenchmark.run(50_000, "q18", true, backend, parallelism);
        assertThat(nativeResult.completed()).isTrue();
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
        assertThat(nativeResult.outputRows()).isEqualTo(flink.outputRows()).isPositive();
        if (parallelism == 1) {
            assertThat(nativeResult.materializedSha256()).isEqualTo(flink.materializedSha256());
            assertThat(nativeResult.outputSha256()).isEqualTo(flink.outputSha256());
            assertThat(nativeResult.orderedSha256()).isEqualTo(flink.orderedSha256());
        } else {
            // Equal rowtimes make the final row depend on the input-channel interleaving.
            // Check complete payloads against each original reader's last tied winner;
            // shared runtime tests compare every byte for an identical arrival order.
            var candidates = com.github.nexmark.flink.source.NexmarkLastBidOracle.candidates(50_000, parallelism);
            assertThat(candidates.values().stream().anyMatch(rows -> rows.size() > 1))
                    .isTrue();
            legalWinners(flink.materializedDebugRows(), candidates);
            legalWinners(nativeResult.materializedDebugRows(), candidates);
        }
        // Blackhole does not request UPDATE_BEFORE; the collecting sink does. Compare
        // each sink's negotiated changelog with the same sink on the other engine.
        StreamFusionPlannerFactory.resetMetrics();
        long flinkBlackhole = NexmarkRowDataJob.runBlackhole(50_000, "q18", false, backend, parallelism, false);
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
        StreamFusionPlannerFactory.resetMetrics();
        long nativeBlackhole = NexmarkRowDataJob.runBlackhole(50_000, "q18", true, backend, parallelism, false);
        assertThat(nativeBlackhole).isEqualTo(flinkBlackhole).isPositive();
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
    }
}

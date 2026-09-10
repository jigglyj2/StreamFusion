/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

/** Original auction Top-10 SQL and its current whole-plan fallback on both backends. */
@ResourceLock("streamfusion-planner-property")
class NexmarkQ19PlanningIT {
    @AfterEach
    void clearPlanner() {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
    }

    @Test
    void catalogPreservesOriginalPriceOnlyOrdering() throws Exception {
        try (var input = getClass().getResourceAsStream("/queries/q19.sql")) {
            assertThat(input).isNotNull();
            var original = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            var select = original.substring(original.indexOf("SELECT", original.indexOf("INSERT INTO")));
            assertThat(normalize(NexmarkRowDataQueryCatalog.load("q19"))).isEqualTo(normalize(select));
            assertThat(NexmarkRowDataQueryCatalog.sinkColumns("q19"))
                    .contains("rank_number BIGINT")
                    .doesNotContain("PRIMARY KEY");
        }
    }

    private static String normalize(String sql) {
        return sql.replace(";", "").replaceAll("\\s+", "").trim();
    }

    @ParameterizedTest
    @CsvSource({"false,hashmap", "true,hashmap", "false,rocksdb", "true,rocksdb"})
    void originalQ19RetainsWholePlanFallbackOnBothBackends(boolean selected, String backend) throws Exception {
        if (selected)
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        var tables = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        NexmarkRowDataJob.configureMemory(tables.getConfig().getConfiguration());
        tables.getConfig().set(StateBackendOptions.STATE_BACKEND, backend);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tables.executeSql(NexmarkRowDataJob.sourceDdl(50_000));
        NexmarkSqlJob.createViews(tables);
        String sql;
        try (var input = getClass().getResourceAsStream("/queries/q19.sql")) {
            assertThat(input).isNotNull();
            sql = new String(input.readAllBytes(), StandardCharsets.UTF_8).replace("nexmark_q19", "nexmark_output");
        }
        var statements = sql.split(";");
        assertThat(statements).hasSize(2);
        tables.executeSql(statements[0]);
        var plan = tables.explainSql(statements[1]);
        assertThat(plan).contains("Rank");
        if (selected) {
            assertThat(StreamFusionPlanningDiagnostics.explain())
                    .contains("Accelerated: no")
                    .contains(
                            "rank persistent admission: verified shared execution requires append-only ROW_NUMBER range [1,1]");
        }
        try (var metrics = NexmarkBlackholeMetrics.begin()) {
            NexmarkBlackholeMetrics.configure(tables.getConfig().getConfiguration(), metrics.id);
            tables.executeSql(statements[1]).await();
            assertThat(metrics.outputRows()).isPositive();
        }
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
    }
}

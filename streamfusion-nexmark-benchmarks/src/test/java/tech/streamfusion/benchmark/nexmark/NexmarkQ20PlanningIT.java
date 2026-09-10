/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

/** Original filtered bid/auction join SQL through ordinary planner selection on both backends. */
@ResourceLock("streamfusion-planner-property")
class NexmarkQ20PlanningIT {
    @AfterEach
    void clearPlanner() {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
    }

    @Test
    void catalogPreservesOriginalJoinAndProjection() throws Exception {
        try (var input = getClass().getResourceAsStream("/queries/q20.sql")) {
            assertThat(input).isNotNull();
            var original = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            var select = original.substring(original.indexOf("SELECT", original.indexOf("INSERT INTO")));
            assertThat(normalize(NexmarkRowDataQueryCatalog.load("q20"))).isEqualTo(normalize(select));
            assertThat(NexmarkRowDataQueryCatalog.sinkColumns("q20"))
                    .contains("bid_dateTime TIMESTAMP(3)", "auction_extra STRING")
                    .doesNotContain("PRIMARY KEY");
        }
    }

    private static String normalize(String sql) {
        // Catalog aliases only label positional sink fields. Remove those aliases and
        // redundant qualifications of columns that are unique across the two inputs.
        return sql.replaceAll("(?i) AS (bid_dateTime|bid_extra|auction_dateTime|auction_extra)", "")
                .replaceAll("(?i)B\\.(auction|bidder|price|channel|url)\\b", "$1")
                .replaceAll("(?i)A\\.(itemName|description|initialBid|reserve|expires|seller|category)\\b", "$1")
                .replaceAll("(?i)INNER JOIN", "JOIN")
                .replaceAll("(?i) ON ", " ON ")
                .replace(";", "")
                .replaceAll("\\s+", "")
                .trim();
    }

    @ParameterizedTest
    @CsvSource({
        "false,hashmap,false",
        "true,hashmap,false",
        "false,rocksdb,false",
        "true,rocksdb,false",
        "false,hashmap,true",
        "true,hashmap,true",
        "false,rocksdb,true",
        "true,rocksdb,true"
    })
    void originalQ20AcceleratesOnBothBackends(boolean selected, String backend, boolean multiJoin) throws Exception {
        if (selected)
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        var tables = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        NexmarkRowDataJob.configureMemory(tables.getConfig().getConfiguration());
        tables.getConfig().set(StateBackendOptions.STATE_BACKEND, backend);
        tables.getConfig().set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, multiJoin);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tables.executeSql(NexmarkRowDataJob.sourceDdl(50_000));
        NexmarkSqlJob.createViews(tables);
        String sql;
        try (var input = getClass().getResourceAsStream("/queries/q20.sql")) {
            assertThat(input).isNotNull();
            sql = new String(input.readAllBytes(), StandardCharsets.UTF_8).replace("nexmark_q20", "nexmark_output");
        }
        var statements = sql.split(";");
        assertThat(statements).hasSize(2);
        tables.executeSql(statements[0]);
        var plan = tables.explainSql(statements[1]);
        assertThat(plan).contains("Join");
        if (selected) {
            assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        }
        try (var metrics = NexmarkBlackholeMetrics.begin()) {
            NexmarkBlackholeMetrics.configure(tables.getConfig().getConfiguration(), metrics.id);
            tables.executeSql(statements[1]).await();
            assertThat(metrics.outputRows()).isPositive();
        }
        if (selected)
            assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
        else assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
    }
}

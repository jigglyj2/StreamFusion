/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

@ResourceLock("streamfusion-planner-property")
class NexmarkQ5PlanningIT {
    @AfterEach
    void clearPlanner() {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
    }

    @Test
    void catalogPreservesOriginalHotItemsQuery() throws Exception {
        var sql = original();
        var select = sql.substring(sql.indexOf("SELECT", sql.indexOf("INSERT INTO")));
        assertThat(normalize(NexmarkRowDataQueryCatalog.load("q5"))).isEqualTo(normalize(select));
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void originalSqlSelectsNativeWindowJoinWithoutTheMultiJoinRewrite(String backend) throws Exception {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        var tables = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        NexmarkRowDataJob.configureMemory(tables.getConfig().getConfiguration());
        tables.getConfig().set(StateBackendOptions.STATE_BACKEND, backend);
        tables.getConfig().set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, false);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
        tables.executeSql(NexmarkRowDataJob.sourceDdl(20_000));
        NexmarkSqlJob.createViews(tables);
        var statements = original().split(";");
        assertThat(statements).hasSize(2);
        tables.executeSql(statements[0]);
        assertThat(tables.explainSql(statements[1])).contains("WindowJoin");
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    private static String original() throws Exception {
        try (var input = NexmarkQ5PlanningIT.class.getResourceAsStream("/queries/q5.sql")) {
            assertThat(input).isNotNull();
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static String normalize(String sql) {
        return sql.toLowerCase(Locale.ROOT)
                .replace("auction_bids", "auctionbids")
                .replace("count_bids", "countbids")
                .replace("max_bids", "maxbids")
                .replace(";", "")
                .replaceAll("\\s+", "");
    }
}

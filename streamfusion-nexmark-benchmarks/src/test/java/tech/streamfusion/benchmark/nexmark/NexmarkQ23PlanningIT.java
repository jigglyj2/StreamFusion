/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

/** Original three-table join SQL through ordinary admission on both backend configurations. */
@ResourceLock("streamfusion-planner-property")
class NexmarkQ23PlanningIT {
    @AfterEach
    void clearPlanner() {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
    }

    @org.junit.jupiter.api.Test
    void catalogPreservesOriginalSelectAndSinkSchema() throws Exception {
        try (var input = getClass().getResourceAsStream("/queries/q23.sql")) {
            assertThat(input).isNotNull();
            var original = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            var select = original.substring(original.indexOf("SELECT", original.indexOf("INSERT INTO")));
            // Flink 2.3 reserves DATETIME; quote the original identifier without changing SQL semantics.
            assertThat(NexmarkRowDataQueryCatalog.load("q23").trim())
                    .isEqualTo(select.replace("A.dateTime", "A.`dateTime`").trim());
            assertThat(NexmarkRowDataQueryCatalog.sinkColumns("q23"))
                    .isEqualTo("bidder BIGINT, price BIGINT, channel STRING, url STRING, bid_extra STRING, "
                            + "person_id BIGINT, name STRING, emailAddress STRING, creditCard STRING, city STRING, "
                            + "state STRING, person_extra STRING, itemName STRING, description STRING, "
                            + "initialBid BIGINT, reserve BIGINT, auction_dateTime TIMESTAMP(3), expires TIMESTAMP(3), "
                            + "seller BIGINT, category BIGINT, auction_extra STRING");
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"hashmap", "rocksdb"})
    void optionalThreeInputMultiJoinRetainsPreciseWholePlanFallback(String backend) throws Exception {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        var tables = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        NexmarkRowDataJob.configureMemory(tables.getConfig().getConfiguration());
        tables.getConfig().set(StateBackendOptions.STATE_BACKEND, backend);
        tables.getConfig()
                .set(org.apache.flink.table.api.config.OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, true);
        tables.executeSql(NexmarkRowDataJob.sourceDdl(100_000));
        NexmarkSqlJob.createViews(tables);
        tables.explainSql(NexmarkRowDataQueryCatalog.load("q23"));
        assertThat(StreamFusionPlanningDiagnostics.explain())
                .contains("Accelerated: no", "StreamExecMultiJoin", "multi-way join", "common fused ExecutionPlan");
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
    }

    @ParameterizedTest
    @CsvSource({"false,hashmap", "true,hashmap", "false,rocksdb", "true,rocksdb"})
    void originalQueryUsesOrdinaryAcceleration(boolean selected, String backend) throws Exception {
        if (selected)
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        var tables = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        NexmarkRowDataJob.configureMemory(tables.getConfig().getConfiguration());
        tables.getConfig().set(StateBackendOptions.STATE_BACKEND, backend);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        assertThat(tables.getConfig()
                        .get(
                                org.apache.flink.table.api.config.OptimizerConfigOptions
                                        .TABLE_OPTIMIZER_MULTI_JOIN_ENABLED))
                .isFalse();
        tables.executeSql(NexmarkRowDataJob.sourceDdl(100_000));
        NexmarkSqlJob.createViews(tables);
        String sql;
        try (var input = getClass().getResourceAsStream("/queries/q23.sql")) {
            assertThat(input).isNotNull();
            sql = new String(input.readAllBytes(), StandardCharsets.UTF_8)
                    .replace("nexmark_q23", "nexmark_output")
                    .replace("A.dateTime", "A.`dateTime`");
        }
        var statements = sql.trim().split(";");
        assertThat(statements).hasSize(2);
        tables.executeSql(statements[0]);
        tables.explainSql(statements[1]);
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

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

/** Original last-bid SQL records the current whole-plan fallback on both backends. */
@ResourceLock("streamfusion-planner-property")
class NexmarkQ18PlanningIT {
    @AfterEach
    void clearPlanner() {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
    }

    @ParameterizedTest
    @CsvSource({"false,hashmap", "true,hashmap", "false,rocksdb", "true,rocksdb"})
    void originalQ18RetainsPreciseWholePlanFallbackOnBothBackends(boolean selected, String backend) throws Exception {
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
        try (var input = getClass().getResourceAsStream("/queries/q18.sql")) {
            assertThat(input).isNotNull();
            sql = new String(input.readAllBytes(), StandardCharsets.UTF_8).replace("nexmark_q18", "nexmark_output");
        }
        var statements = sql.split(";");
        assertThat(statements).hasSize(2);
        tables.executeSql(statements[0]);
        var plan = tables.explainSql(statements[1]);
        assertThat(plan).contains("Deduplicate", "Calc");
        if (selected) {
            assertThat(StreamFusionPlanningDiagnostics.explain())
                    .contains(
                            "Accelerated: no",
                            "StreamExecDeduplicate",
                            "native persistent state is temporarily disabled");
        }
        try (var metrics = NexmarkBlackholeMetrics.begin()) {
            NexmarkBlackholeMetrics.configure(tables.getConfig().getConfiguration(), metrics.id);
            tables.executeSql(statements[1]).await();
            assertThat(metrics.outputRows()).isPositive();
        }
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
    }
}

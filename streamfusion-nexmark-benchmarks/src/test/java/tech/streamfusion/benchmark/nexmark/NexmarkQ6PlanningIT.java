/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

/** Keeps an upstream planning limitation distinct from a StreamFusion execution/admission gap. */
@ResourceLock("streamfusion-planner-property")
class NexmarkQ6PlanningIT {
    @AfterEach
    void clearPlanner() {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
    }

    @ParameterizedTest
    @CsvSource({
        "false,hashmap,false", "true,hashmap,false", "false,rocksdb,false", "true,rocksdb,false",
        "false,hashmap,true", "true,hashmap,true", "false,rocksdb,true", "true,rocksdb,true"
    })
    void boundedOverAfterWinningBidRankHasNoFlinkStreamingBaseline(
            boolean nativeEngine, String backend, boolean multiJoin) throws Exception {
        clearPlanner();
        if (nativeEngine)
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        var tables = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tables.getConfig().getConfiguration().set(StateBackendOptions.STATE_BACKEND, backend);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
        tables.getConfig().set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, multiJoin);
        tables.executeSql(NexmarkRowDataJob.sourceDdl(10_000));
        NexmarkSqlJob.createViews(tables);
        tables.executeSql(
                "CREATE TABLE nexmark_output (seller BIGINT, avg_price BIGINT) " + "WITH ('connector'='blackhole')");
        String query;
        try (var input = getClass().getResourceAsStream("/nexmark/q6-scoped.sql")) {
            assertThat(input).isNotNull();
            query = new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
        assertThatThrownBy(() -> tables.explainSql("INSERT INTO nexmark_output\n" + query))
                .isInstanceOf(TableException.class)
                .hasMessageContaining("Non-time attribute sort is not supported for bounded OVER window");
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
        if (nativeEngine) assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: no");
    }
}

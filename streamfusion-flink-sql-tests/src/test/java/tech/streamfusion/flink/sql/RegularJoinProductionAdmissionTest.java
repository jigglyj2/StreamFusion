/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class RegularJoinProductionAdmissionTest extends SqlParityTestSupport {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void regularAndMultiJoinAdmitVerifiedSemanticsAndKeepPreciseFallbacks(boolean multiJoin) {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        var tables = StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
        assertThat(tables.getConfig().get(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED))
                .isFalse();
        tables.getConfig().set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, multiJoin);
        tables.executeSql(
                "CREATE TABLE left_input (k BIGINT, v BIGINT, ts TIMESTAMP(3), label STRING) WITH ('connector'='datagen','number-of-rows'='1')");
        tables.executeSql(
                "CREATE TABLE right_input (k BIGINT, v BIGINT, ts TIMESTAMP(3), label STRING) WITH ('connector'='datagen','number-of-rows'='1')");
        String sql = "SELECT a.k,a.v,b.v FROM left_input a JOIN right_input b ON a.k=b.k";
        assertThat(tables.explainSql(sql)).contains("Accelerated: yes", "StreamFusionRegularJoin");
        assertThat(tables.explainSql(sql + " AND a.v>=b.v")).contains("Accelerated: yes");
        assertThat(tables.explainSql(sql + " AND a.ts>=b.ts-INTERVAL '10' SECOND"))
                .contains("Accelerated: yes");
        assertThat(tables.explainSql(sql + " AND a.v>=b.v+1"))
                .contains("Accelerated: no", "binary join residual workspace");
        assertThat(tables.explainSql(sql.replace(" JOIN ", " LEFT JOIN "))).contains("Accelerated: no");
        tables.getConfig().setIdleStateRetention(Duration.ofSeconds(1));
        assertThat(tables.explainSql(sql)).contains("Accelerated: no", "state TTL");
        tables.getConfig().setIdleStateRetention(Duration.ZERO);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, true);
        assertThat(tables.explainSql(sql)).contains("Accelerated: no", "async-state");
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 10L);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofMillis(100));
        assertThat(tables.explainSql(sql)).contains("Accelerated: no", "mini-batch");
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
        tables.getConfig().set(org.apache.flink.configuration.StateChangelogOptions.ENABLE_STATE_CHANGE_LOG, true);
        assertThat(tables.explainSql(sql)).contains("Accelerated: no", "changelog-state");
        tables.getConfig().set(org.apache.flink.configuration.StateChangelogOptions.ENABLE_STATE_CHANGE_LOG, false);
        assertThat(tables.explainSql(sql)).contains("Accelerated: yes");
    }
}

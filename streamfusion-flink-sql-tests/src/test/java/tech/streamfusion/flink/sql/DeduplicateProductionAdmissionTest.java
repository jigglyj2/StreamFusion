/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class DeduplicateProductionAdmissionTest extends SqlParityTestSupport {
    @Test
    void rowtimeSelectionIsOrdinaryAndUnverifiedSubsetsExplainWholePlanFallback() {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        var tables = StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
        tables.executeSql("CREATE TABLE dedup_admission (k BIGINT, label STRING, ts TIMESTAMP(3), "
                + "floating DOUBLE, pt AS PROCTIME(), WATERMARK FOR ts AS ts - INTERVAL '1' SECOND) "
                + "WITH ('connector'='datagen', 'number-of-rows'='1')");
        String sql = query("ts DESC", false);
        assertThat(tables.explainSql(sql)).contains("Accelerated: yes", "StreamFusionDeduplicate");
        assertThat(tables.explainSql(query("pt DESC", false))).contains("Accelerated: no", "processing-time");
        assertThat(tables.explainSql(query("ts DESC", true))).contains("Accelerated: no", "input type DOUBLE");
        tables.getConfig().setIdleStateRetention(Duration.ofSeconds(1));
        assertThat(tables.explainSql(sql)).contains("Accelerated: no", "state TTL");
        tables.getConfig().setIdleStateRetention(Duration.ZERO);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, true);
        assertThat(tables.explainSql(sql)).contains("Accelerated: no", "synchronous state");
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofMillis(100));
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 10L);
        assertThat(tables.explainSql(sql)).contains("Accelerated: no", "mini-batching");
    }

    private static String query(String order, boolean floating) {
        String columns = "k,label,ts" + (floating ? ",floating" : "");
        return "SELECT " + columns + " FROM (SELECT " + columns
                + ",ROW_NUMBER() OVER (PARTITION BY k,label ORDER BY " + order
                + ") rn FROM dedup_admission) WHERE rn<=1";
    }
}

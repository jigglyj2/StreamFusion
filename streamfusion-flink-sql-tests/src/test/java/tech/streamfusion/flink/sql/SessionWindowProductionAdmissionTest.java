/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class SessionWindowProductionAdmissionTest extends SqlParityTestSupport {
    @Test
    void selectsVerifiedSessionsAndExplainsEveryUnverifiedSemanticSubset() {
        var tables = tables();
        for (int gap : new int[] {1, 10, 37})
            assertThat(tables.explainSql(query("k", "COUNT(*)", "ts", gap)))
                    .contains("Accelerated: yes", "StreamFusionWindowAggregate");
        for (String call : new String[] {"MIN(k)", "SUM(k)", "COUNT(v)", "COUNT(*) FILTER (WHERE v > 0)"})
            assertThat(tables.explainSql(query("k", call, "ts", 10)))
                    .contains("Accelerated: no", "unfiltered COUNT(*)");
        assertThat(tables.explainSql(query("label", "COUNT(*)", "ts", 10)))
                .contains("Accelerated: no", "BIGINT partition key");
        assertThat(tables.explainSql(
                        query("k", "COUNT(*)", "ltz", 10).replace("TABLE session_input", "TABLE session_ltz")))
                .contains("Accelerated: no", "without time zone");
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, true);
        assertThat(tables.explainSql(query("k", "COUNT(*)", "ts", 10))).contains("Accelerated: no", "async-state");
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
        tables.getConfig().set(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG, true);
        assertThat(tables.explainSql(query("k", "COUNT(*)", "ts", 10))).contains("Accelerated: no", "changelog-state");
        tables.getConfig().set(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG, false);
        tables.getConfig().getConfiguration().setString("state.backend.rocksdb.metrics.estimate-num-keys", "true");
        assertThat(tables.explainSql(query("k", "COUNT(*)", "ts", 10))).contains("Accelerated: no", "metrics:");
    }

    private static String query(String key, String call, String time, int gap) {
        return "SELECT " + key + ", " + call + ", window_start, window_end FROM TABLE(SESSION(TABLE session_input "
                + "PARTITION BY " + key + ", DESCRIPTOR(" + time + "), INTERVAL '" + gap + "' SECOND)) "
                + "GROUP BY " + key + ", window_start, window_end";
    }

    private static StreamTableEnvironment tables() {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        var tables = StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
        tables.executeSql("CREATE TABLE session_input (k BIGINT, v BIGINT, label STRING, ts TIMESTAMP(3), "
                + "ltz TIMESTAMP_LTZ(3), WATERMARK FOR ts AS ts) WITH ('connector'='datagen', 'number-of-rows'='1')");
        tables.executeSql("CREATE TABLE session_ltz (k BIGINT, ltz TIMESTAMP_LTZ(3), WATERMARK FOR ltz AS ltz) "
                + "WITH ('connector'='datagen', 'number-of-rows'='1')");
        return tables;
    }
}

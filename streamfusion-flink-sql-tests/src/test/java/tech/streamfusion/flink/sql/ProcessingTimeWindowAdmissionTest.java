/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class ProcessingTimeWindowAdmissionTest extends SqlParityTestSupport {
    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void selectsVerifiedProcessingTimeWindowsWithoutMaterializingTheLogicalAttribute(String backend) {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        var backendConfig = new org.apache.flink.configuration.Configuration();
        var ingest = org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.USE_INGEST_DB_RESTORE_MODE;
        backendConfig.set(ingest, ingest.defaultValue());
        env.configure(backendConfig);
        var tables = StreamTableEnvironment.create(env);
        tables.getConfig().setLocalTimeZone(java.time.ZoneId.of("UTC"));
        tables.getConfig().getConfiguration().set(StateBackendOptions.STATE_BACKEND, backend);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
        tables.executeSql(
                "CREATE TABLE processing_input (k BIGINT, label STRING) WITH ('connector'='datagen', 'number-of-rows'='1')");
        for (int gap : new int[] {1, 10, 37}) {
            String sql = "WITH B AS (SELECT k, PROCTIME() AS pt FROM processing_input) "
                    + "SELECT k, COUNT(*), window_start, window_end FROM TABLE("
                    + "TUMBLE(TABLE B, DESCRIPTOR(pt), INTERVAL '" + gap + "' SECOND)) "
                    + "GROUP BY k, window_start, window_end";
            assertThat(tables.explainSql(sql))
                    .contains("Accelerated: yes", "StreamFusionWindowAggregate")
                    .doesNotContain("projection[1]/PROCTIME:");
        }
        String sql = "WITH B AS (SELECT k, PROCTIME() AS pt FROM processing_input) "
                + "SELECT k, COUNT(*), window_start, window_end FROM TABLE("
                + "TUMBLE(TABLE B, DESCRIPTOR(pt), INTERVAL '10' SECOND)) GROUP BY k, window_start, window_end";
        for (String call : new String[] {"MIN(k)", "SUM(k)", "COUNT(k)", "COUNT(*) FILTER (WHERE k > 0)"})
            assertThat(tables.explainSql(sql.replace("COUNT(*)", call)))
                    .contains("Accelerated: no", "unfiltered COUNT(*)");
        assertThat(tables.explainSql(sql.replace("SELECT k, PROCTIME()", "SELECT label AS k, PROCTIME()")))
                .contains("Accelerated: no", "BIGINT partition key");
        assertThat(tables.explainSql(sql.replace(
                        "TUMBLE(TABLE B, DESCRIPTOR(pt), INTERVAL '10' SECOND)",
                        "HOP(TABLE B, DESCRIPTOR(pt), INTERVAL '2' SECOND, INTERVAL '10' SECOND)")))
                .contains("Accelerated: no", "direct TUMBLE");
        tables.getConfig().setLocalTimeZone(java.time.ZoneId.of("America/New_York"));
        assertThat(tables.explainSql(sql)).contains("Accelerated: no", "non-UTC window assignment");
        tables.getConfig().setLocalTimeZone(java.time.ZoneId.of("UTC"));
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, true);
        assertThat(tables.explainSql(sql)).contains("Accelerated: no", "async-state");
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
        tables.getConfig().set(org.apache.flink.configuration.StateChangelogOptions.ENABLE_STATE_CHANGE_LOG, true);
        assertThat(tables.explainSql(sql)).contains("Accelerated: no", "changelog-state");
    }
}

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
    void namesTheRemainingWindowStateContractWithoutMistakingTheLogicalAttributeForAClockRead(String backend) {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        var tables = StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
        tables.getConfig().getConfiguration().set(StateBackendOptions.STATE_BACKEND, backend);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
        tables.executeSql(
                "CREATE TABLE processing_input (k BIGINT) WITH ('connector'='datagen', 'number-of-rows'='1')");
        for (int gap : new int[] {1, 10, 37}) {
            String sql = "WITH B AS (SELECT k, PROCTIME() AS pt FROM processing_input) "
                    + "SELECT k, COUNT(*), window_start, window_end FROM TABLE("
                    + "TUMBLE(TABLE B, DESCRIPTOR(pt), INTERVAL '" + gap + "' SECOND)) "
                    + "GROUP BY k, window_start, window_end";
            assertThat(tables.explainSql(sql))
                    .contains("Accelerated: no", "the entire plan will use Flink", "StreamExecWindowAggregate")
                    .contains("shared processing-time planner resource binding and production parity")
                    .doesNotContain("projection[1]/PROCTIME:");
        }
    }
}

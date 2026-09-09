/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

/** Live SQL/network integration; exact clock/changelog parity is covered by controlled-clock fixtures. */
class SelectedProcessingWindowSqlIntegrationTest extends SqlParityTestSupport {
    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void ordinarySqlExecutesProcessingTimeWindowsAcrossTheArrowExchange(String backend) throws Exception {
        for (boolean selected : new boolean[] {false, true}) {
            if (selected)
                System.setProperty(
                        StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
            else System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            StreamFusionPlannerFactory.resetMetrics();
            var env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            var config = new Configuration();
            var ingest = org.apache.flink.state.rocksdb.RocksDBConfigurableOptions.USE_INGEST_DB_RESTORE_MODE;
            config.set(ingest, ingest.defaultValue());
            env.configure(config);
            var tables = StreamTableEnvironment.create(env);
            tables.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
            tables.getConfig().set(StateBackendOptions.STATE_BACKEND, backend);
            tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
            tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
            tables.executeSql("CREATE TABLE live_processing (k BIGINT) WITH ("
                    + "'connector'='datagen', 'number-of-rows'='30000', 'rows-per-second'='10000', "
                    + "'fields.k.min'='0', 'fields.k.max'='9')");
            String sql = "WITH B AS (SELECT k, PROCTIME() AS pt FROM live_processing) "
                    + "SELECT k, COUNT(*), window_start, window_end FROM TABLE("
                    + "TUMBLE(TABLE B, DESCRIPTOR(pt), INTERVAL '1' SECOND)) GROUP BY k, window_start, window_end";
            long counted = 0;
            try (var rows = tables.executeSql(sql).collect()) {
                while (rows.hasNext()) {
                    var row = rows.next();
                    assertThat(row.getKind()).isEqualTo(RowKind.INSERT);
                    assertThat((Long) row.getField(0)).isBetween(0L, 9L);
                    long count = (Long) row.getField(1);
                    assertThat(count).isPositive();
                    counted += count;
                    long start = ((LocalDateTime) row.getField(2))
                            .toInstant(ZoneOffset.UTC)
                            .toEpochMilli();
                    long end = ((LocalDateTime) row.getField(3))
                            .toInstant(ZoneOffset.UTC)
                            .toEpochMilli();
                    assertThat(Math.floorMod(start, 1000)).isZero();
                    assertThat(end - start).isEqualTo(1000);
                }
            }
            // Finishing a bounded source does not emit the final open processing-time window.
            // Separate wall-clock jobs naturally assign different counts/absolute window labels.
            assertThat(counted).isBetween(1L, 30000L);
            if (selected) {
                assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
                assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
                assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isPositive();
            } else assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
        }
    }
}

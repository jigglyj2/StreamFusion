/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class WindowAggregateChangelogParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @ValueSource(
            strings = {
                "TUMBLE(TABLE window_changes, DESCRIPTOR(ts), INTERVAL '10' SECOND)",
                "HOP(TABLE window_changes, DESCRIPTOR(ts), INTERVAL '2' SECOND, INTERVAL '10' SECOND)",
                "CUMULATE(TABLE window_changes, DESCRIPTOR(ts), INTERVAL '2' SECOND, INTERVAL '10' SECOND)",
                "SESSION(TABLE window_changes PARTITION BY category, DESCRIPTOR(ts), INTERVAL '10' SECOND)"
            })
    void everyWindowFamilyRetractsCompleteAggregatesLikeFlink(String windowCall) throws Exception {
        byte[] flink = execute(windowCall, false);
        byte[] streamFusion = execute(windowCall, true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    private static byte[] execute(String windowCall, boolean streamFusion) throws Exception {
        if (streamFusion) {
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        } else {
            System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            StreamFusionPlannerFactory.resetMetrics();
        }
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        LocalDateTime firstTime = LocalDateTime.of(2026, 8, 29, 12, 0, 1);
        LocalDateTime secondTime = LocalDateTime.of(2026, 8, 29, 12, 0, 2);
        DataStream<Row> changes = environment.fromCollection(
                List.of(
                        row(RowKind.INSERT, "a", "first", 10L, firstTime),
                        row(RowKind.INSERT, "a", "second", 20L, secondTime),
                        row(RowKind.UPDATE_BEFORE, "a", "second", 20L, secondTime),
                        row(RowKind.UPDATE_AFTER, "a", "replacement", 5L, secondTime),
                        row(RowKind.DELETE, "a", "first", 10L, firstTime)),
                Types.ROW_NAMED(
                        new String[] {"category", "label", "amount", "ts"},
                        Types.STRING,
                        Types.STRING,
                        Types.LONG,
                        Types.LOCAL_DATE_TIME));
        Table input = tables.fromChangelogStream(
                changes,
                Schema.newBuilder()
                        .column("category", "STRING NOT NULL")
                        .column("label", "STRING")
                        .column("amount", "BIGINT")
                        .column("ts", "TIMESTAMP(3)")
                        .watermark("ts", "ts - INTERVAL '1' SECOND")
                        .build());
        tables.createTemporaryView("window_changes", input);
        String sql = "SELECT category, COUNT(*), COUNT(label), SUM(amount), MIN(amount), MAX(amount), "
                + "window_start, window_end FROM TABLE("
                + windowCall
                + ") GROUP BY category, window_start, window_end";
        return collect(tables.executeSql(sql));
    }

    private static Row row(RowKind kind, String category, String label, long amount, LocalDateTime timestamp) {
        Row row = Row.of(category, label, amount, timestamp);
        row.setKind(kind);
        return row;
    }
}

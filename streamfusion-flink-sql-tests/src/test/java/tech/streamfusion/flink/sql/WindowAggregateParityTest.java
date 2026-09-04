/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class WindowAggregateParityTest extends SqlParityTestSupport {
    @org.junit.jupiter.api.Test
    void processingTimeTumbleIsSelectedForNativeAggregation() {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                StreamExecutionEnvironment.getExecutionEnvironment(),
                EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.executeSql("CREATE TABLE proc_window_input (category STRING, amount BIGINT, "
                + "pt AS PROCTIME()) WITH ('connector'='datagen', 'number-of-rows'='1')");

        String plan = tables.explainSql("SELECT category, COUNT(*), window_start, window_end "
                + "FROM TABLE(TUMBLE(TABLE proc_window_input, DESCRIPTOR(pt), INTERVAL '10' SECOND)) "
                + "GROUP BY category, window_start, window_end");

        assertThat(plan).contains("StreamFusionWindowAggregate");
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "TUMBLE(TABLE window_input, DESCRIPTOR(ts), INTERVAL '5' SECOND)",
                "HOP(TABLE window_input, DESCRIPTOR(ts), INTERVAL '2' SECOND, INTERVAL '6' SECOND)",
                "CUMULATE(TABLE window_input, DESCRIPTOR(ts), INTERVAL '2' SECOND, INTERVAL '6' SECOND)",
                "SESSION(TABLE window_input PARTITION BY category, DESCRIPTOR(ts), INTERVAL '3' SECOND)"
            })
    void alignedWindowCountsAndPropertiesMatchFlinkByteForByte(String windowCall) throws Exception {
        byte[] flink = execute(windowCall, false);
        byte[] streamFusion = execute(windowCall, true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @org.junit.jupiter.api.Test
    void timestampLtzWindowsMatchFlinkAcrossDstGapAndOverlap() throws Exception {
        byte[] flink = executeTimestampLtz(false);
        byte[] streamFusion = executeTimestampLtz(true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @org.junit.jupiter.api.Test
    void filteredWindowAggregatesMatchFlinkByteForByte() throws Exception {
        byte[] flink = executeFilteredWindow(false);
        byte[] streamFusion = executeFilteredWindow(true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeWindowAggregateBatchCount()).isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    private static byte[] executeFilteredWindow(boolean streamFusionEnabled) throws Exception {
        if (streamFusionEnabled) {
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
        tables.createTemporaryView(
                "filtered_window_input",
                tables.fromDataStream(
                        environment.fromCollection(
                                List.of(
                                        Row.of("a", 3L, true, LocalDateTime.of(2026, 8, 29, 12, 0, 1)),
                                        Row.of("a", 5L, false, LocalDateTime.of(2026, 8, 29, 12, 0, 2)),
                                        Row.of("a", 7L, null, LocalDateTime.of(2026, 8, 29, 12, 0, 3))),
                                Types.ROW_NAMED(
                                        new String[] {"category", "amount", "selected", "ts"},
                                        Types.STRING,
                                        Types.LONG,
                                        Types.BOOLEAN,
                                        Types.LOCAL_DATE_TIME)),
                        Schema.newBuilder()
                                .column("category", DataTypes.STRING().notNull())
                                .column("amount", DataTypes.BIGINT().notNull())
                                .column("selected", DataTypes.BOOLEAN())
                                .column("ts", DataTypes.TIMESTAMP(3))
                                .watermark("ts", "ts - INTERVAL '1' SECOND")
                                .build()));
        return collect(tables.executeSql("SELECT category, "
                + "COUNT(*) FILTER (WHERE selected), SUM(amount) FILTER (WHERE selected), "
                + "AVG(amount) FILTER (WHERE selected), "
                + "MIN(amount) FILTER (WHERE selected), MAX(amount) FILTER (WHERE selected), "
                + "window_start, window_end "
                + "FROM TABLE(TUMBLE(TABLE filtered_window_input, DESCRIPTOR(ts), INTERVAL '5' SECOND)) "
                + "GROUP BY category, window_start, window_end"));
    }

    private static byte[] executeTimestampLtz(boolean streamFusionEnabled) throws Exception {
        if (streamFusionEnabled) {
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
        tables.getConfig().set(TableConfigOptions.LOCAL_TIME_ZONE, "America/Los_Angeles");
        tables.createTemporaryView(
                "ltz_window_input",
                tables.fromDataStream(
                        environment.fromCollection(
                                List.of(
                                        Row.of("spring", Instant.parse("2021-03-14T09:30:00Z")),
                                        Row.of("fall", Instant.parse("2021-11-07T08:30:00Z")),
                                        Row.of("fall", Instant.parse("2021-11-07T09:30:00Z"))),
                                Types.ROW_NAMED(new String[] {"category", "ts"}, Types.STRING, Types.INSTANT)),
                        Schema.newBuilder()
                                .column("category", DataTypes.STRING().notNull())
                                .column("ts", DataTypes.TIMESTAMP_LTZ(3))
                                .watermark("ts", "ts - INTERVAL '1' SECOND")
                                .build()));
        return collect(tables.executeSql("SELECT category, COUNT(*), window_start, window_end, window_time "
                + "FROM TABLE(TUMBLE(TABLE ltz_window_input, DESCRIPTOR(ts), INTERVAL '1' HOUR)) "
                + "GROUP BY category, window_start, window_end, window_time"));
    }

    private static byte[] execute(String windowCall, boolean streamFusionEnabled) throws Exception {
        if (streamFusionEnabled) {
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
        tables.createTemporaryView(
                "window_input",
                tables.fromDataStream(
                        environment.fromCollection(
                                List.of(
                                        Row.of("a", 3L, LocalDateTime.of(2026, 8, 29, 12, 0, 1)),
                                        Row.of("a", 5L, LocalDateTime.of(2026, 8, 29, 12, 0, 2)),
                                        Row.of("b", 7L, LocalDateTime.of(2026, 8, 29, 12, 0, 7)),
                                        Row.of("a", 11L, LocalDateTime.of(2026, 8, 29, 12, 0, 4))),
                                Types.ROW_NAMED(
                                        new String[] {"category", "amount", "ts"},
                                        Types.STRING,
                                        Types.LONG,
                                        Types.LOCAL_DATE_TIME)),
                        Schema.newBuilder()
                                .column("category", DataTypes.STRING().notNull())
                                .column("amount", DataTypes.BIGINT().notNull())
                                .column("ts", DataTypes.TIMESTAMP(3))
                                .watermark("ts", "ts - INTERVAL '2' SECOND")
                                .build()));
        String sql = "SELECT category, COUNT(*), SUM(amount), AVG(amount), window_start, window_end, window_time "
                + "FROM TABLE("
                + windowCall
                + ") GROUP BY category, window_start, window_end, window_time";
        return collect(tables.executeSql(sql));
    }
}

/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class WindowJoinParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @ValueSource(
            strings = {
                "TUMBLE(TABLE %s, DESCRIPTOR(ts), INTERVAL '5' SECOND)",
                "HOP(TABLE %s, DESCRIPTOR(ts), INTERVAL '2' SECOND, INTERVAL '6' SECOND)",
                "CUMULATE(TABLE %s, DESCRIPTOR(ts), INTERVAL '2' SECOND, INTERVAL '6' SECOND)",
                "SESSION(TABLE %s PARTITION BY category, DESCRIPTOR(ts), INTERVAL '3' SECOND)"
            })
    void innerJoinForEveryWindowFamilyMatchesFlinkByteForByte(String windowCall) throws Exception {
        byte[] flink = execute(windowCall, "JOIN", "", false);
        byte[] streamFusion = execute(windowCall, "JOIN", "", true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeWindowJoinBatchCount()).isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @ParameterizedTest
    @ValueSource(strings = {"JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"})
    void everyBinaryJoinModeAndRemainingConditionMatchesFlinkByteForByte(String joinMode) throws Exception {
        String window = "TUMBLE(TABLE %s, DESCRIPTOR(ts), INTERVAL '5' SECOND)";
        String remainingCondition = " AND l.payload.code < r.payload.code";

        byte[] flink = execute(window, joinMode, remainingCondition, false);
        byte[] streamFusion = execute(window, joinMode, remainingCondition, true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeWindowJoinBatchCount()).isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void semiAndAntiJoinModesMatchFlinkByteForByte(boolean exists) throws Exception {
        byte[] flink = executeExistenceJoin(exists, false);
        byte[] streamFusion = executeExistenceJoin(exists, true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeWindowJoinBatchCount()).isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    private static byte[] executeExistenceJoin(boolean exists, boolean streamFusionEnabled) throws Exception {
        configure(streamFusionEnabled);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        createInput(
                tables,
                environment,
                "window_join_left",
                List.of(
                        Row.of("a", Row.of("left-1", 1), LocalDateTime.of(2026, 9, 1, 12, 0, 1)),
                        Row.of("b", Row.of("left-2", 2), LocalDateTime.of(2026, 9, 1, 12, 0, 7))));
        createInput(
                tables,
                environment,
                "window_join_right",
                List.of(Row.of("a", Row.of("right-1", 11), LocalDateTime.of(2026, 9, 1, 12, 0, 2))));
        String query = "SELECT l.category, l.payload, l.window_start, l.window_end "
                + "FROM TABLE(TUMBLE(TABLE window_join_left, DESCRIPTOR(ts), INTERVAL '5' SECOND)) l "
                + "WHERE "
                + (exists ? "" : "NOT ")
                + "EXISTS (SELECT 1 FROM TABLE(TUMBLE(TABLE window_join_right, DESCRIPTOR(ts), "
                + "INTERVAL '5' SECOND)) r WHERE l.category = r.category "
                + "AND l.window_start = r.window_start AND l.window_end = r.window_end)";
        return collect(tables.executeSql(query));
    }

    private static byte[] execute(
            String windowCall, String joinMode, String remainingCondition, boolean streamFusionEnabled)
            throws Exception {
        configure(streamFusionEnabled);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        createInput(
                tables,
                environment,
                "window_join_left",
                List.of(
                        Row.of("a", Row.of("left-1", 1), LocalDateTime.of(2026, 9, 1, 12, 0, 1)),
                        Row.of("a", Row.of("left-2", 2), LocalDateTime.of(2026, 9, 1, 12, 0, 3)),
                        Row.of("b", Row.of("left-3", 3), LocalDateTime.of(2026, 9, 1, 12, 0, 7))));
        createInput(
                tables,
                environment,
                "window_join_right",
                List.of(
                        Row.of("a", Row.of("right-1", 11), LocalDateTime.of(2026, 9, 1, 12, 0, 2)),
                        Row.of("a", Row.of("right-2", 12), LocalDateTime.of(2026, 9, 1, 12, 0, 4)),
                        Row.of("c", Row.of("right-3", 13), LocalDateTime.of(2026, 9, 1, 12, 0, 7))));
        String left = String.format(windowCall, "window_join_left");
        String right = String.format(windowCall, "window_join_right");
        String query = "SELECT l.category, l.payload, r.payload, l.window_start, l.window_end "
                + "FROM TABLE("
                + left
                + ") l "
                + joinMode
                + " TABLE("
                + right
                + ") r ON l.category = r.category "
                + "AND l.window_start = r.window_start AND l.window_end = r.window_end"
                + remainingCondition;
        return collect(tables.executeSql(query));
    }

    private static void configure(boolean streamFusionEnabled) {
        if (streamFusionEnabled) {
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        } else {
            System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            StreamFusionPlannerFactory.resetMetrics();
        }
    }

    private static void createInput(
            StreamTableEnvironment tables, StreamExecutionEnvironment environment, String name, List<Row> rows) {
        tables.createTemporaryView(
                name,
                tables.fromDataStream(
                        environment.fromCollection(
                                rows,
                                Types.ROW_NAMED(
                                        new String[] {"category", "payload", "ts"},
                                        Types.STRING,
                                        Types.ROW_NAMED(new String[] {"label", "code"}, Types.STRING, Types.INT),
                                        Types.LOCAL_DATE_TIME)),
                        Schema.newBuilder()
                                .column("category", DataTypes.STRING().notNull())
                                .column(
                                        "payload",
                                        DataTypes.ROW(
                                                DataTypes.FIELD("label", DataTypes.STRING()),
                                                DataTypes.FIELD("code", DataTypes.INT())))
                                .column("ts", DataTypes.TIMESTAMP(3))
                                .watermark("ts", "ts - INTERVAL '1' SECOND")
                                .build()));
    }
}

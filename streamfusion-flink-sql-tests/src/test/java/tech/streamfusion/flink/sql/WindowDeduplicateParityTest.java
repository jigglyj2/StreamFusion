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

class WindowDeduplicateParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @ValueSource(
            strings = {
                "TUMBLE(TABLE window_dedup_input, DESCRIPTOR(ts), INTERVAL '5' SECOND)",
                "HOP(TABLE window_dedup_input, DESCRIPTOR(ts), INTERVAL '2' SECOND, INTERVAL '6' SECOND)",
                "CUMULATE(TABLE window_dedup_input, DESCRIPTOR(ts), INTERVAL '2' SECOND, INTERVAL '6' SECOND)",
                "SESSION(TABLE window_dedup_input PARTITION BY category, DESCRIPTOR(ts), INTERVAL '3' SECOND)"
            })
    void lastRowForEveryWindowFamilyMatchesFlinkByteForByte(String windowCall) throws Exception {
        byte[] flink = execute(windowCall, false);
        byte[] streamFusion = execute(windowCall, true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeWindowDeduplicateBatchCount())
                .isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
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
                "window_dedup_input",
                tables.fromDataStream(
                        environment.fromCollection(
                                List.of(
                                        Row.of("a", 1L, Row.of("one", 1), LocalDateTime.of(2026, 9, 1, 12, 0, 1)),
                                        Row.of("a", 2L, Row.of("two", 2), LocalDateTime.of(2026, 9, 1, 12, 0, 3)),
                                        Row.of("b", 3L, Row.of("three", 3), LocalDateTime.of(2026, 9, 1, 12, 0, 2)),
                                        Row.of("a", 4L, Row.of("four", 4), LocalDateTime.of(2026, 9, 1, 12, 0, 7))),
                                Types.ROW_NAMED(
                                        new String[] {"category", "amount", "payload", "ts"},
                                        Types.STRING,
                                        Types.LONG,
                                        Types.ROW_NAMED(new String[] {"label", "code"}, Types.STRING, Types.INT),
                                        Types.LOCAL_DATE_TIME)),
                        Schema.newBuilder()
                                .column("category", DataTypes.STRING().notNull())
                                .column("amount", DataTypes.BIGINT().notNull())
                                .column(
                                        "payload",
                                        DataTypes.ROW(
                                                DataTypes.FIELD("label", DataTypes.STRING()),
                                                DataTypes.FIELD("code", DataTypes.INT())))
                                .column("ts", DataTypes.TIMESTAMP(3))
                                .watermark("ts", "ts - INTERVAL '1' SECOND")
                                .build()));
        String query = "SELECT category, amount, payload, ts, window_start, window_end FROM ("
                + "SELECT *, ROW_NUMBER() OVER (PARTITION BY category, window_start, window_end "
                + "ORDER BY ts DESC) AS row_num FROM TABLE("
                + windowCall
                + ")) WHERE row_num = 1";
        return collect(tables.executeSql(query));
    }
}

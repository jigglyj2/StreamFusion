/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.rowInterval;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class LegacyGroupWindowAggregateParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @ValueSource(
            strings = {
                "SELECT category, COUNT(*), SUM(amount), AVG(amount), "
                        + "TUMBLE_START(ts, INTERVAL '5' SECOND), "
                        + "TUMBLE_END(ts, INTERVAL '5' SECOND), "
                        + "TUMBLE_ROWTIME(ts, INTERVAL '5' SECOND) "
                        + "FROM legacy_window_input "
                        + "GROUP BY category, TUMBLE(ts, INTERVAL '5' SECOND)",
                "SELECT category, COUNT(*), SUM(amount), AVG(amount), "
                        + "HOP_START(ts, INTERVAL '2' SECOND, INTERVAL '6' SECOND), "
                        + "HOP_END(ts, INTERVAL '2' SECOND, INTERVAL '6' SECOND), "
                        + "HOP_ROWTIME(ts, INTERVAL '2' SECOND, INTERVAL '6' SECOND) "
                        + "FROM legacy_window_input "
                        + "GROUP BY category, HOP(ts, INTERVAL '2' SECOND, INTERVAL '6' SECOND)",
                "SELECT category, COUNT(*), SUM(amount), AVG(amount), "
                        + "SESSION_START(ts, INTERVAL '3' SECOND), "
                        + "SESSION_END(ts, INTERVAL '3' SECOND), "
                        + "SESSION_ROWTIME(ts, INTERVAL '3' SECOND) "
                        + "FROM legacy_window_input "
                        + "GROUP BY category, SESSION(ts, INTERVAL '3' SECOND)"
            })
    void legacyTimeWindowsMatchFlinkByteForByte(String sql) throws Exception {
        byte[] flink = execute(sql, false);
        byte[] streamFusion = execute(sql, true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeWindowAggregateBatchCount()).isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void processingTimeCountWindowsMatchFlinkByteForByte(boolean sliding) throws Exception {
        byte[] flink = executeCount(sliding, false);
        byte[] streamFusion = executeCount(sliding, true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeWindowAggregateBatchCount()).isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void legacyTimeWindowsApplyRetractionsExactlyLikeFlink() throws Exception {
        byte[] flink = executeChangelog(false);
        byte[] streamFusion = executeChangelog(true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeWindowAggregateBatchCount()).isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void earlyFiringFallsBackWithAnExplicitWholePlanReason() {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set("table.exec.emit.early-fire.enabled", "true");
        tables.getConfig().set("table.exec.emit.early-fire.delay", "0 ms");
        tables.createTemporaryView(
                "early_window_input",
                tables.fromDataStream(
                        environment
                                .fromData(Row.of("a", LocalDateTime.of(2026, 8, 29, 12, 0, 1)))
                                .returns(Types.ROW_NAMED(
                                        new String[] {"category", "ts"}, Types.STRING, Types.LOCAL_DATE_TIME)),
                        Schema.newBuilder()
                                .column("category", "STRING")
                                .column("ts", "TIMESTAMP(3)")
                                .watermark("ts", "ts - INTERVAL '1' SECOND")
                                .build()));

        assertThat(tables.explainSql("SELECT category, COUNT(*) FROM early_window_input "
                        + "GROUP BY category, TUMBLE(ts, INTERVAL '10' SECOND)"))
                .contains("Accelerated: no")
                .contains("early/late firing is not implemented")
                .contains("the entire plan will use Flink");
    }

    private static byte[] execute(String sql, boolean streamFusionEnabled) throws Exception {
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
                "legacy_window_input",
                tables.fromDataStream(
                        environment.fromCollection(
                                List.of(
                                        Row.of("a", 3L, LocalDateTime.of(2026, 8, 29, 12, 0, 1)),
                                        Row.of("a", 5L, LocalDateTime.of(2026, 8, 29, 12, 0, 2)),
                                        Row.of("b", 7L, LocalDateTime.of(2026, 8, 29, 12, 0, 7)),
                                        Row.of("a", 11L, LocalDateTime.of(2026, 8, 29, 12, 0, 4)),
                                        Row.of("a", 13L, LocalDateTime.of(2026, 8, 29, 12, 0, 8))),
                                Types.ROW_NAMED(
                                        new String[] {"category", "amount", "ts"},
                                        Types.STRING,
                                        Types.LONG,
                                        Types.LOCAL_DATE_TIME)),
                        Schema.newBuilder()
                                .column("category", DataTypes.STRING().notNull())
                                .column("amount", DataTypes.BIGINT().notNull())
                                .column("ts", DataTypes.TIMESTAMP(3))
                                .watermark("ts", "ts - INTERVAL '1' SECOND")
                                .build()));
        return collect(tables.executeSql(sql));
    }

    private static byte[] executeCount(boolean sliding, boolean streamFusionEnabled) throws Exception {
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
        Table input = tables.fromDataStream(
                environment.fromCollection(
                        List.of(
                                Row.of("a", 3L),
                                Row.of("a", 5L),
                                Row.of("a", 7L),
                                Row.of("a", 11L),
                                Row.of("a", 13L),
                                Row.of("b", 17L),
                                Row.of("b", 19L),
                                Row.of("b", 23L)),
                        Types.ROW_NAMED(new String[] {"category", "amount"}, Types.STRING, Types.LONG)),
                Schema.newBuilder()
                        .column("category", "STRING")
                        .column("amount", "BIGINT")
                        .columnByExpression("pt", "PROCTIME()")
                        .build());
        tables.createTemporaryView("legacy_count_window_input", input);
        Table result = sliding
                ? input.window(Slide.over(rowInterval(3L))
                                .every(rowInterval(2L))
                                .on($("pt"))
                                .as("w"))
                        .groupBy($("category"), $("w"))
                        .select(
                                $("category"),
                                $("amount").count(),
                                $("amount").sum(),
                                $("amount").avg())
                : input.window(Tumble.over(rowInterval(3L)).on($("pt")).as("w"))
                        .groupBy($("category"), $("w"))
                        .select(
                                $("category"),
                                $("amount").count(),
                                $("amount").sum(),
                                $("amount").avg());
        return collect(result.execute());
    }

    private static byte[] executeChangelog(boolean streamFusionEnabled) throws Exception {
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
        LocalDateTime first = LocalDateTime.of(2026, 8, 29, 12, 0, 1);
        LocalDateTime second = LocalDateTime.of(2026, 8, 29, 12, 0, 2);
        DataStream<Row> changes = environment.fromCollection(
                List.of(
                        changelogRow(RowKind.INSERT, "a", "old", 10L, first),
                        changelogRow(RowKind.INSERT, "a", "replace", 20L, second),
                        changelogRow(RowKind.UPDATE_BEFORE, "a", "replace", 20L, second),
                        changelogRow(RowKind.UPDATE_AFTER, "a", "new", 5L, second),
                        changelogRow(RowKind.DELETE, "a", "old", 10L, first)),
                Types.ROW_NAMED(
                        new String[] {"category", "label", "amount", "ts"},
                        Types.STRING,
                        Types.STRING,
                        Types.LONG,
                        Types.LOCAL_DATE_TIME));
        tables.createTemporaryView(
                "legacy_window_changes",
                tables.fromChangelogStream(
                        changes,
                        Schema.newBuilder()
                                .column("category", "STRING NOT NULL")
                                .column("label", "STRING")
                                .column("amount", "BIGINT")
                                .column("ts", "TIMESTAMP(3)")
                                .watermark("ts", "ts - INTERVAL '1' SECOND")
                                .build()));
        return collect(tables.executeSql(
                "SELECT category, COUNT(*), COUNT(label), SUM(amount), AVG(amount), MIN(amount), MAX(amount), "
                        + "TUMBLE_START(ts, INTERVAL '10' SECOND), TUMBLE_END(ts, INTERVAL '10' SECOND) "
                        + "FROM legacy_window_changes GROUP BY category, TUMBLE(ts, INTERVAL '10' SECOND)"));
    }

    private static Row changelogRow(RowKind kind, String category, String label, long amount, LocalDateTime timestamp) {
        Row row = Row.of(category, label, amount, timestamp);
        row.setKind(kind);
        return row;
    }
}

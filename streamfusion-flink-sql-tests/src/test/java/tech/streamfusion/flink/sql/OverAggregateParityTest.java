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
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class OverAggregateParityTest extends SqlParityTestSupport {
    @Test
    void unboundedRowsAndRangeFramesMatchFlinkByteForByte() throws Exception {
        for (String frame : new String[] {"ROWS", "RANGE"}) {
            assertParity(
                    "SELECT category, amount, label, "
                            + "SUM(amount) OVER (PARTITION BY category ORDER BY amount "
                            + frame
                            + " BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum, "
                            + "COUNT(*) OVER (PARTITION BY category ORDER BY amount "
                            + frame
                            + " BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count "
                            + "FROM (VALUES ('a', 20, 'later'), ('a', 10, 'first'), "
                            + "('a', 20, 'peer'), ('b', 7, 'other')) AS input(category, amount, label)",
                    true);

            assertThat(StreamFusionPlannerFactory.nativeOverAggregateBatchCount())
                    .isGreaterThan(0);
            assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        }
    }

    @Test
    void insertsUpdatesDeletesAndDuplicateRowsMatchFlinkByteForByte() throws Exception {
        byte[] flink = executeRetractions(false);
        byte[] streamFusion = executeRetractions(true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(StreamFusionPlannerFactory.nativeOverAggregateBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    @Test
    void eventTimeUnboundedRowsAndRangeMatchFlinkByteForByte() throws Exception {
        for (String frame : new String[] {"ROWS", "RANGE"}) {
            byte[] flink = executeEventTime(frame, false);
            byte[] streamFusion = executeEventTime(frame, true);

            assertThat(streamFusion).isEqualTo(flink);
            assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
            assertThat(StreamFusionPlannerFactory.nativeOverAggregateBatchCount())
                    .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                    .isGreaterThan(0);
        }
    }

    @Test
    void processingTimeMaterializationStillHasAnExplicitWholePlanFallback() throws Exception {
        for (String frame : new String[] {"ROWS", "RANGE"}) {
            byte[] flink = executeProcessingTime(frame, false);
            byte[] streamFusion = executeProcessingTime(frame, true);

            assertThat(streamFusion).isEqualTo(flink);
            assertThat(StreamFusionPlannerFactory.nativeOverAggregateBatchCount())
                    .isZero();
            assertThat(StreamFusionPlanningDiagnostics.explain())
                    .contains("Accelerated: no")
                    .contains("PROCTIME");
        }
    }

    @Test
    void unsupportedAverageExpansionRemainsOnFlinkWithAnExplicitWholePlanReason() throws Exception {
        String sql = "SELECT category, amount, "
                + "AVG(amount) OVER (PARTITION BY category ORDER BY amount ROWS "
                + "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM "
                + "(VALUES ('a', 20), ('a', 10), ('a', 30)) AS input(category, amount)";

        byte[] flink = execute(sql, true, false);
        byte[] streamFusion = execute(sql, true, true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeOverAggregateBatchCount()).isZero();
        assertThat(StreamFusionPlanningDiagnostics.explain())
                .contains("Accelerated: no")
                .contains("StreamExecCalc")
                .contains("expression shape or type combination is not parity-approved");
    }

    private static byte[] executeRetractions(boolean streamFusion) throws Exception {
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
        Table changes = tables.fromChangelogStream(
                environment.fromCollection(
                        List.of(
                                changed(RowKind.INSERT, "a", 20L, "twenty"),
                                changed(RowKind.INSERT, "a", 10L, "ten"),
                                changed(RowKind.INSERT, "a", 20L, "twenty"),
                                changed(RowKind.DELETE, "a", 20L, "twenty"),
                                changed(RowKind.UPDATE_BEFORE, "a", 10L, "ten"),
                                changed(RowKind.UPDATE_AFTER, "a", 15L, "fifteen")),
                        Types.ROW_NAMED(
                                new String[] {"category", "amount", "label"}, Types.STRING, Types.LONG, Types.STRING)),
                Schema.newBuilder()
                        .column("category", "STRING")
                        .column("amount", "BIGINT")
                        .column("label", "STRING")
                        .build());
        tables.createTemporaryView("over_changes", changes);
        return collect(tables.executeSql("SELECT category, amount, label, "
                + "SUM(amount) OVER (PARTITION BY category ORDER BY amount ROWS "
                + "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum "
                + "FROM over_changes"));
    }

    private static byte[] executeProcessingTime(String frame, boolean streamFusion) throws Exception {
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
        Table input = tables.fromDataStream(
                environment.fromCollection(
                        List.of(Row.of("a", 10L), Row.of("a", 20L), Row.of("b", 7L), Row.of("a", 5L)),
                        Types.ROW_NAMED(new String[] {"category", "amount"}, Types.STRING, Types.LONG)),
                Schema.newBuilder()
                        .column("category", "STRING")
                        .column("amount", "BIGINT")
                        .columnByExpression("pt", "PROCTIME()")
                        .build());
        tables.createTemporaryView("proc_over_input", input);
        return collect(tables.executeSql("SELECT category, amount, "
                + "SUM(amount) OVER (PARTITION BY category ORDER BY pt "
                + frame
                + " BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum, "
                + "COUNT(*) OVER (PARTITION BY category ORDER BY pt "
                + frame
                + " BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count "
                + "FROM proc_over_input"));
    }

    private static byte[] executeEventTime(String frame, boolean streamFusion) throws Exception {
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
        tables.createTemporaryView(
                "event_over_input",
                tables.fromDataStream(
                        environment.fromCollection(
                                List.of(
                                        Row.of("a", 20L, LocalDateTime.of(2026, 9, 3, 12, 0, 2)),
                                        Row.of("a", 10L, LocalDateTime.of(2026, 9, 3, 12, 0, 1)),
                                        Row.of("a", 4L, LocalDateTime.of(2026, 9, 3, 12, 0, 2)),
                                        Row.of("b", 7L, LocalDateTime.of(2026, 9, 3, 12, 0, 1)),
                                        Row.of("a", 5L, LocalDateTime.of(2026, 9, 3, 12, 0, 3))),
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
        return collect(tables.executeSql("SELECT category, amount, ts, "
                + "SUM(amount) OVER (PARTITION BY category ORDER BY ts "
                + frame
                + " BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum, "
                + "COUNT(*) OVER (PARTITION BY category ORDER BY ts "
                + frame
                + " BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count "
                + "FROM event_over_input"));
    }

    private static Row changed(RowKind kind, String category, long amount, String label) {
        Row row = Row.of(category, amount, label);
        row.setKind(kind);
        return row;
    }
}

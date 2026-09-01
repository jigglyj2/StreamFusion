/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
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
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class GroupAggregateParityTest extends SqlParityTestSupport {
    @Test
    void integerAndDecimalAccumulatorsMatchFlinkByteForByte() throws Exception {
        byte[] flink = executeInsertRows(false);
        byte[] streamFusion = executeInsertRows(true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void retractChangelogMatchesFlinkByteForByte() throws Exception {
        byte[] flink = executeRetractions(false);
        byte[] streamFusion = executeRetractions(true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    private static byte[] executeRetractions(boolean streamFusion) throws Exception {
        configurePlanner(streamFusion);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        DataStream<Row> changes = environment.fromCollection(
                List.of(
                        row(RowKind.INSERT, "a", "first", 10L),
                        row(RowKind.INSERT, "a", "second", 20L),
                        row(RowKind.UPDATE_BEFORE, "a", "second", 20L),
                        row(RowKind.UPDATE_AFTER, "a", "replacement", 5L),
                        row(RowKind.DELETE, "a", "first", 10L),
                        row(RowKind.DELETE, "a", "replacement", 5L)),
                Types.ROW_NAMED(new String[] {"category", "label", "amount"}, Types.STRING, Types.STRING, Types.LONG));
        Table input = tables.fromChangelogStream(
                changes,
                Schema.newBuilder()
                        .column("category", "STRING NOT NULL")
                        .column("label", "STRING")
                        .column("amount", "BIGINT")
                        .build());
        tables.createTemporaryView("group_aggregate_changes", input);
        return collect(
                tables.executeSql("SELECT category, COUNT(*), COUNT(label), SUM(amount), MIN(amount), MAX(amount) "
                        + "FROM group_aggregate_changes GROUP BY category"));
    }

    private static byte[] executeInsertRows(boolean streamFusion) throws Exception {
        configurePlanner(streamFusion);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        DataStream<Row> rows = environment.fromCollection(
                List.of(
                        Row.of("a", "first", 10, new BigDecimal("10.25")),
                        Row.of("a", null, 20, null),
                        Row.of("a", "third", null, new BigDecimal("-2.50")),
                        Row.of("b", "only", -5, new BigDecimal("3.00"))),
                Types.ROW_NAMED(
                        new String[] {"category", "label", "amount", "decimal_amount"},
                        Types.STRING,
                        Types.STRING,
                        Types.INT,
                        Types.BIG_DEC));
        Table input = tables.fromDataStream(
                rows,
                Schema.newBuilder()
                        .column("category", "STRING NOT NULL")
                        .column("label", "STRING")
                        .column("amount", "INT")
                        .column("decimal_amount", "DECIMAL(10, 2)")
                        .build());
        tables.createTemporaryView("group_aggregate_inserts", input);
        return collect(
                tables.executeSql("SELECT category, COUNT(*), COUNT(label), SUM(amount), MIN(amount), MAX(amount), "
                        + "SUM(decimal_amount), MIN(decimal_amount), MAX(decimal_amount) "
                        + "FROM group_aggregate_inserts GROUP BY category"));
    }

    private static void configurePlanner(boolean streamFusion) {
        if (streamFusion) {
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        } else {
            System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            StreamFusionPlannerFactory.resetMetrics();
        }
    }

    private static Row row(RowKind kind, String category, String label, long amount) {
        Row row = Row.of(category, label, amount);
        row.setKind(kind);
        return row;
    }
}

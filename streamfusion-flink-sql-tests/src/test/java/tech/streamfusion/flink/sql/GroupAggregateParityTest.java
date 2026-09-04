/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
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
    void globalAggregateUsesSingletonNativeStateAndMatchesFlinkByteForByte() throws Exception {
        byte[] flink = executeGlobalAggregate(false);
        byte[] streamFusion = executeGlobalAggregate(true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeGroupAggregateBatchCount()).isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    private static byte[] executeGlobalAggregate(boolean streamFusion) throws Exception {
        configurePlanner(streamFusion);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        DataStream<Row> rows = environment.fromCollection(
                List.of(Row.of(1, "beta"), Row.of(null, "alpha"), Row.of(3, "zeta")),
                Types.ROW_NAMED(new String[] {"amount", "label"}, Types.INT, Types.STRING));
        tables.createTemporaryView("global_aggregate_input", tables.fromDataStream(rows));
        return collect(tables.executeSql("SELECT COUNT(*), COUNT(amount), SUM(amount), MIN(label), MAX(label) "
                + "FROM global_aggregate_input"));
    }

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

    @Test
    void globalRetractionsMatchFlinkByteForByte() throws Exception {
        byte[] flink = executeGlobalRetractions(false);
        byte[] streamFusion = executeGlobalRetractions(true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeGroupAggregateBatchCount()).isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void floatingBooleanStringAndTemporalAggregatesMatchFlinkByteForByte() throws Exception {
        byte[] flink = executeExpandedTypes(false);
        byte[] streamFusion = executeExpandedTypes(true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void expandedTypesRetractExtremaAndDeleteEmptyGroupLikeFlink() throws Exception {
        byte[] flink = executeExpandedRetractions(false);
        byte[] streamFusion = executeExpandedRetractions(true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void floatingNanAndSignedZeroRetractionsMatchFlinkByteForByte() throws Exception {
        byte[] flink = executeFloatingEdges(false);
        byte[] streamFusion = executeFloatingEdges(true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void integerAndDecimalSumOverflowMatchesFlinkByteForByte() throws Exception {
        byte[] flink = executeOverflow(false);
        byte[] streamFusion = executeOverflow(true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    private static byte[] executeOverflow(boolean streamFusion) throws Exception {
        configurePlanner(streamFusion);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        BigDecimal decimalMaximum = new BigDecimal("99999999999999999999999999999999999999");
        Row maximum = Row.of("a", Long.MAX_VALUE, decimalMaximum);
        Row one = Row.of("a", 1L, BigDecimal.ONE);
        Row seven = Row.of("a", 7L, new BigDecimal("7"));
        DataStream<Row> changes = environment.fromCollection(
                List.of(
                        withKind(maximum, RowKind.INSERT),
                        withKind(one, RowKind.INSERT),
                        withKind(seven, RowKind.INSERT),
                        withKind(seven, RowKind.DELETE),
                        withKind(one, RowKind.DELETE),
                        withKind(maximum, RowKind.DELETE)),
                Types.ROW_NAMED(
                        new String[] {"category", "integer_value", "decimal_value"},
                        Types.STRING,
                        Types.LONG,
                        Types.BIG_DEC));
        Table input = tables.fromChangelogStream(
                changes,
                Schema.newBuilder()
                        .column("category", "STRING NOT NULL")
                        .column("integer_value", "BIGINT")
                        .column("decimal_value", "DECIMAL(38, 0)")
                        .build());
        tables.createTemporaryView("group_aggregate_overflow", input);
        return collect(tables.executeSql("SELECT category, COUNT(*), SUM(integer_value), SUM(decimal_value) "
                + "FROM group_aggregate_overflow GROUP BY category"));
    }

    private static byte[] executeFloatingEdges(boolean streamFusion) throws Exception {
        configurePlanner(streamFusion);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        Row negativeZero = Row.of("a", -0.0F, -0.0D);
        Row positiveZero = Row.of("a", 0.0F, 0.0D);
        Row nan = Row.of("a", Float.NaN, Double.NaN);
        DataStream<Row> changes = environment.fromCollection(
                List.of(
                        withKind(negativeZero, RowKind.INSERT),
                        withKind(positiveZero, RowKind.INSERT),
                        withKind(nan, RowKind.INSERT),
                        withKind(nan, RowKind.DELETE),
                        withKind(positiveZero, RowKind.DELETE),
                        withKind(negativeZero, RowKind.DELETE)),
                Types.ROW_NAMED(
                        new String[] {"category", "float_value", "double_value"},
                        Types.STRING,
                        Types.FLOAT,
                        Types.DOUBLE));
        Table input = tables.fromChangelogStream(
                changes,
                Schema.newBuilder()
                        .column("category", "STRING NOT NULL")
                        .column("float_value", "FLOAT")
                        .column("double_value", "DOUBLE")
                        .build());
        tables.createTemporaryView("group_aggregate_float_edges", input);
        return collect(tables.executeSql("SELECT category, COUNT(*), "
                + "MIN(float_value), MAX(float_value), MIN(double_value), MAX(double_value) "
                + "FROM group_aggregate_float_edges GROUP BY category"));
    }

    private static byte[] executeExpandedRetractions(boolean streamFusion) throws Exception {
        configurePlanner(streamFusion);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        Row middle = Row.of("a", "middle", 1.5F, 4.0D, true, LocalDate.of(2026, 1, 2));
        Row low = Row.of("a", "alpha", -2.0F, 8.0D, false, LocalDate.of(2026, 1, 1));
        Row high = Row.of("a", "zeta", 3.0F, -1.0D, true, LocalDate.of(2026, 1, 3));
        Row nulls = Row.of("a", null, null, null, null, null);
        Row absent = Row.of("absent", "ghost", 0.0F, 0.0D, false, LocalDate.of(2000, 1, 1));
        DataStream<Row> changes = environment.fromCollection(
                List.of(
                        withKind(absent, RowKind.DELETE),
                        withKind(middle, RowKind.INSERT),
                        withKind(nulls, RowKind.INSERT),
                        withKind(low, RowKind.INSERT),
                        withKind(low, RowKind.INSERT),
                        withKind(high, RowKind.INSERT),
                        withKind(high, RowKind.DELETE),
                        withKind(low, RowKind.DELETE),
                        withKind(low, RowKind.DELETE),
                        withKind(nulls, RowKind.DELETE),
                        withKind(middle, RowKind.DELETE)),
                Types.ROW_NAMED(
                        new String[] {"category", "label", "float_value", "double_value", "flag", "date_value"},
                        Types.STRING,
                        Types.STRING,
                        Types.FLOAT,
                        Types.DOUBLE,
                        Types.BOOLEAN,
                        Types.LOCAL_DATE));
        Table input = tables.fromChangelogStream(
                changes,
                Schema.newBuilder()
                        .column("category", "STRING NOT NULL")
                        .column("label", "STRING")
                        .column("float_value", "FLOAT")
                        .column("double_value", "DOUBLE")
                        .column("flag", "BOOLEAN")
                        .column("date_value", "DATE")
                        .build());
        tables.createTemporaryView("group_aggregate_expanded_retractions", input);
        return collect(tables.executeSql("SELECT category, COUNT(*), SUM(float_value), SUM(double_value), "
                + "MIN(float_value), MAX(float_value), MIN(double_value), MAX(double_value), "
                + "MIN(flag), MAX(flag), MIN(label), MAX(label), MIN(date_value), MAX(date_value) "
                + "FROM group_aggregate_expanded_retractions GROUP BY category"));
    }

    private static Row withKind(Row source, RowKind kind) {
        Row copy = Row.copy(source);
        copy.setKind(kind);
        return copy;
    }

    private static byte[] executeExpandedTypes(boolean streamFusion) throws Exception {
        configurePlanner(streamFusion);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        DataStream<Row> rows = environment.fromCollection(
                List.of(
                        Row.of(
                                "a",
                                1.5F,
                                9.25D,
                                true,
                                "zeta",
                                LocalDate.of(2026, 1, 3),
                                LocalTime.of(12, 30, 0, 123_000_000),
                                LocalDateTime.of(2026, 1, 3, 12, 30, 0, 123_000_000),
                                Instant.parse("2026-01-03T12:30:00.123Z")),
                        Row.of(
                                "a",
                                -2.25F,
                                -4.5D,
                                false,
                                "alpha",
                                LocalDate.of(2025, 12, 31),
                                LocalTime.of(1, 2, 3, 4_000_000),
                                LocalDateTime.of(2025, 12, 31, 1, 2, 3, 4_000_000),
                                Instant.parse("2025-12-31T01:02:03.004Z")),
                        Row.of("a", null, null, null, null, null, null, null, null)),
                Types.ROW_NAMED(
                        new String[] {
                            "category",
                            "float_value",
                            "double_value",
                            "flag",
                            "label",
                            "date_value",
                            "time_value",
                            "timestamp_value",
                            "timestamp_ltz_value"
                        },
                        Types.STRING,
                        Types.FLOAT,
                        Types.DOUBLE,
                        Types.BOOLEAN,
                        Types.STRING,
                        Types.LOCAL_DATE,
                        Types.LOCAL_TIME,
                        Types.LOCAL_DATE_TIME,
                        Types.INSTANT));
        Table input = tables.fromDataStream(
                rows,
                Schema.newBuilder()
                        .column("category", "STRING NOT NULL")
                        .column("float_value", "FLOAT")
                        .column("double_value", "DOUBLE")
                        .column("flag", "BOOLEAN")
                        .column("label", "STRING")
                        .column("date_value", "DATE")
                        .column("time_value", "TIME(3)")
                        .column("timestamp_value", "TIMESTAMP(3)")
                        .column("timestamp_ltz_value", "TIMESTAMP_LTZ(3)")
                        .build());
        tables.createTemporaryView("group_aggregate_expanded_types", input);
        return collect(tables.executeSql("SELECT category, "
                + "SUM(float_value), SUM(double_value), MIN(float_value), MAX(float_value), "
                + "MIN(double_value), MAX(double_value), MIN(flag), MAX(flag), MIN(label), MAX(label), "
                + "MIN(date_value), MAX(date_value), MIN(time_value), MAX(time_value), "
                + "MIN(timestamp_value), MAX(timestamp_value), "
                + "MIN(timestamp_ltz_value), MAX(timestamp_ltz_value) "
                + "FROM group_aggregate_expanded_types GROUP BY category"));
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

    private static byte[] executeGlobalRetractions(boolean streamFusion) throws Exception {
        configurePlanner(streamFusion);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        DataStream<Row> changes = environment.fromCollection(
                List.of(
                        row(RowKind.INSERT, "ignored", "beta", 10L),
                        row(RowKind.INSERT, "ignored", "alpha", 20L),
                        row(RowKind.UPDATE_BEFORE, "ignored", "alpha", 20L),
                        row(RowKind.UPDATE_AFTER, "ignored", "zeta", 5L),
                        row(RowKind.DELETE, "ignored", "beta", 10L),
                        row(RowKind.DELETE, "ignored", "zeta", 5L)),
                Types.ROW_NAMED(new String[] {"unused", "label", "amount"}, Types.STRING, Types.STRING, Types.LONG));
        Table input = tables.fromChangelogStream(
                changes,
                Schema.newBuilder()
                        .column("unused", "STRING NOT NULL")
                        .column("label", "STRING")
                        .column("amount", "BIGINT")
                        .build());
        tables.createTemporaryView("global_aggregate_changes", input);
        return collect(tables.executeSql(
                "SELECT COUNT(*), COUNT(label), SUM(amount), MIN(amount), MAX(amount), MIN(label), MAX(label) "
                        + "FROM global_aggregate_changes"));
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

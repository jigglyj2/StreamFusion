/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
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

class TopNParityTest extends SqlParityTestSupport {
    @Test
    void partitionedConstantRangeWithOffsetAndRankNumberMatchesFlinkByteForByte() throws Exception {
        assertParity(
                "SELECT category, amount, label, row_num FROM ("
                        + "SELECT *, ROW_NUMBER() OVER (PARTITION BY category "
                        + "ORDER BY amount DESC NULLS LAST, label ASC NULLS FIRST) AS row_num "
                        + "FROM (VALUES ('a', 5, 'z'), ('a', 9, 'b'), ('a', 9, 'a'), "
                        + "('a', 4, 'later'), ('b', 7, 'x'), ('b', 8, 'y')) "
                        + "AS input(category, amount, label)) WHERE row_num BETWEEN 2 AND 3",
                true);

        assertThat(StreamFusionPlannerFactory.nativeTopNBatchCount()).isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void updatingAggregateInputRetractionsMatchFlinkByteForByte() throws Exception {
        assertParity(
                "SELECT category, total FROM ("
                        + "SELECT *, ROW_NUMBER() OVER (ORDER BY total DESC, category ASC) AS row_num FROM ("
                        + "SELECT category, SUM(amount) AS total FROM "
                        + "(VALUES ('a', 1), ('b', 5), ('a', 10), ('c', 3), ('b', -10)) "
                        + "AS input(category, amount) GROUP BY category)) WHERE row_num <= 2",
                true);

        assertThat(StreamFusionPlannerFactory.nativeTopNBatchCount()).isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void variableRankEndIsRememberedPerPartitionLikeFlink() throws Exception {
        byte[] flink = executeVariable(false);
        byte[] streamFusion = executeVariable(true);

        assertThat(streamFusion).isEqualTo(flink);

        assertThat(StreamFusionPlannerFactory.nativeTopNBatchCount()).isGreaterThan(0);
    }

    @Test
    void globalOrderByLimitOffsetMatchesFlink() throws Exception {
        assertParity(
                "SELECT id, label FROM (VALUES (3, 'c'), (1, 'a'), (4, 'd'), (2, 'b')) "
                        + "AS input(id, label) ORDER BY id LIMIT 2 OFFSET 1",
                true);

        assertThat(StreamFusionPlannerFactory.nativeTopNBatchCount()).isGreaterThan(0);
    }

    @Test
    void insertUpdateBeforeUpdateAfterAndDeleteRestoreDisplacedRows() throws Exception {
        byte[] flink = executeRetractions(false);
        byte[] streamFusion = executeRetractions(true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeTopNBatchCount()).isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void duplicateRowsRetractInFlinksStableSequenceOrder() throws Exception {
        byte[] flink = executeDuplicateRetractions(false);
        byte[] streamFusion = executeDuplicateRetractions(true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeTopNBatchCount()).isGreaterThan(0);
    }

    private static byte[] executeDuplicateRetractions(boolean streamFusionEnabled) throws Exception {
        configure(streamFusionEnabled);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        Table changes = tables.fromChangelogStream(
                environment.fromCollection(
                        List.of(
                                changed(RowKind.INSERT, "a", 20L, "same"),
                                changed(RowKind.INSERT, "a", 20L, "same"),
                                changed(RowKind.INSERT, "a", 20L, "same"),
                                changed(RowKind.DELETE, "a", 20L, "same"),
                                changed(RowKind.DELETE, "a", 20L, "same")),
                        Types.ROW_NAMED(
                                new String[] {"category", "amount", "label"}, Types.STRING, Types.LONG, Types.STRING)),
                Schema.newBuilder()
                        .column("category", "STRING")
                        .column("amount", "BIGINT")
                        .column("label", "STRING")
                        .build());
        tables.createTemporaryView("topn_duplicate_retractions", changes);
        return collect(tables.executeSql("SELECT category, amount, label, row_num FROM ("
                + "SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) AS row_num "
                + "FROM topn_duplicate_retractions) WHERE row_num <= 2"));
    }

    private static byte[] executeRetractions(boolean streamFusionEnabled) throws Exception {
        configure(streamFusionEnabled);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        Table changes = tables.fromChangelogStream(
                environment.fromCollection(
                        List.of(
                                changed(RowKind.INSERT, "a", 10L, "ten"),
                                changed(RowKind.INSERT, "a", 20L, "twenty"),
                                changed(RowKind.INSERT, "a", 30L, "thirty"),
                                changed(RowKind.DELETE, "a", 30L, "thirty"),
                                changed(RowKind.UPDATE_BEFORE, "a", 20L, "twenty"),
                                changed(RowKind.UPDATE_AFTER, "a", 25L, "twenty-five")),
                        Types.ROW_NAMED(
                                new String[] {"category", "amount", "label"}, Types.STRING, Types.LONG, Types.STRING)),
                Schema.newBuilder()
                        .column("category", "STRING")
                        .column("amount", "BIGINT")
                        .column("label", "STRING")
                        .build());
        tables.createTemporaryView("topn_retractions", changes);
        return collect(tables.executeSql("SELECT category, amount, label, row_num FROM ("
                + "SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC, label ASC) AS row_num "
                + "FROM topn_retractions) WHERE row_num <= 2"));
    }

    private static Row changed(RowKind kind, String category, long amount, String label) {
        Row row = Row.of(category, amount, label);
        row.setKind(kind);
        return row;
    }

    private static byte[] executeVariable(boolean streamFusionEnabled) throws Exception {
        configure(streamFusionEnabled);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tables.createTemporaryView(
                "topn_variable_input",
                environment.fromCollection(
                        List.of(
                                Row.of("a", 5, (short) 2),
                                Row.of("a", 9, (short) 3),
                                Row.of("a", 7, (short) 2),
                                Row.of("b", 1, (short) 1),
                                Row.of("b", 4, (short) 1)),
                        Types.ROW_NAMED(
                                new String[] {"category", "amount", "top_size"},
                                Types.STRING,
                                Types.INT,
                                Types.SHORT)));
        return collect(tables.executeSql("SELECT category, amount, top_size FROM ("
                + "SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) AS row_num "
                + "FROM topn_variable_input) WHERE row_num <= top_size"));
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
}

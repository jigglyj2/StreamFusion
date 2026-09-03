/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class ChangelogNormalizeParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @ValueSource(strings = {"SELECT * FROM upsert_input", "SELECT * FROM upsert_input WHERE amount >= 20"})
    void upsertNormalizationAndPushedFilterMatchFlinkByteForByte(String query) throws Exception {
        byte[] flink = execute(false, query);
        byte[] streamFusion = execute(true, query);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeChangelogNormalizeBatchCount())
                .isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @org.junit.jupiter.api.Test
    void generatedEqualiserSemanticsMatchForFloatingPointAndUnorderedMaps() throws Exception {
        byte[] flink = executeEqualiserEdges(false);
        byte[] streamFusion = executeEqualiserEdges(true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeChangelogNormalizeBatchCount())
                .isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    private static byte[] execute(boolean streamFusionEnabled, String query) throws Exception {
        configure(streamFusionEnabled);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        List<Row> changes = List.of(
                Row.ofKind(RowKind.INSERT, 1L, "one", 10),
                Row.ofKind(RowKind.UPDATE_AFTER, 1L, "one-updated", 20),
                Row.ofKind(RowKind.UPDATE_AFTER, 1L, "one-updated", 20),
                Row.ofKind(RowKind.INSERT, 2L, "two", 30),
                Row.ofKind(RowKind.DELETE, 1L, null, null));
        tables.createTemporaryView(
                "upsert_input",
                tables.fromChangelogStream(
                        environment.fromCollection(
                                changes,
                                Types.ROW_NAMED(
                                        new String[] {"id", "name", "amount"}, Types.LONG, Types.STRING, Types.INT)),
                        Schema.newBuilder()
                                .column("id", DataTypes.BIGINT().notNull())
                                .column("name", DataTypes.STRING())
                                .column("amount", DataTypes.INT())
                                .primaryKey("id")
                                .build(),
                        ChangelogMode.upsert()));
        return collect(tables.executeSql(query));
    }

    private static byte[] executeEqualiserEdges(boolean streamFusionEnabled) throws Exception {
        configure(streamFusionEnabled);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        List<Row> changes = List.of(
                Row.ofKind(RowKind.INSERT, 1L, -0.0D, linkedMap("a", 1, "b", 2)),
                Row.ofKind(RowKind.UPDATE_AFTER, 1L, 0.0D, linkedMap("a", 1, "b", 2)),
                Row.ofKind(RowKind.INSERT, 2L, 1.0D, linkedMap("a", 1, "b", 2)),
                Row.ofKind(RowKind.UPDATE_AFTER, 2L, 1.0D, linkedMap("b", 2, "a", 1)),
                Row.ofKind(RowKind.INSERT, 3L, Double.NaN, linkedMap("a", 1, "b", 2)),
                Row.ofKind(RowKind.UPDATE_AFTER, 3L, Double.NaN, linkedMap("a", 1, "b", 2)));
        tables.createTemporaryView(
                "equaliser_input",
                tables.fromChangelogStream(
                        environment.fromCollection(
                                changes,
                                Types.ROW_NAMED(
                                        new String[] {"id", "score", "attributes"},
                                        Types.LONG,
                                        Types.DOUBLE,
                                        Types.MAP(Types.STRING, Types.INT))),
                        Schema.newBuilder()
                                .column("id", DataTypes.BIGINT().notNull())
                                .column("score", DataTypes.DOUBLE())
                                .column(
                                        "attributes",
                                        DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.INT()))
                                .primaryKey("id")
                                .build(),
                        ChangelogMode.upsert()));
        return collect(tables.executeSql("SELECT * FROM equaliser_input"));
    }

    private static Map<String, Integer> linkedMap(Object... entries) {
        Map<String, Integer> result = new LinkedHashMap<>();
        for (int index = 0; index < entries.length; index += 2) {
            result.put((String) entries[index], (Integer) entries[index + 1]);
        }
        return result;
    }

    private static void configure(boolean enabled) {
        if (enabled) {
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        } else {
            System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            StreamFusionPlannerFactory.resetMetrics();
        }
    }
}

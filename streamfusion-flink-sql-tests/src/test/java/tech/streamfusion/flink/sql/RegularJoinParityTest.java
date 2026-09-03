/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
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
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class RegularJoinParityTest extends SqlParityTestSupport {
    @Test
    void innerJoinPreservesNestedPayloadsByteForByte() throws Exception {
        String query = "SELECT l.id, l.payload, l.attributes, r.payload, r.amount "
                + "FROM regular_join_left l JOIN regular_join_right r ON l.id = r.id";

        byte[] flink = execute(query, false);
        byte[] streamFusion = execute(query, true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeRegularJoinBatchCount()).isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void semiJoinPreservesTheLeftArrowRowByteForByte() throws Exception {
        String query = "SELECT l.id, l.payload, l.attributes FROM regular_join_left l WHERE "
                + "EXISTS (SELECT 1 FROM regular_join_right r WHERE l.id = r.id)";

        byte[] flink = execute(query, false);
        byte[] streamFusion = execute(query, true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeRegularJoinBatchCount()).isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    private static byte[] execute(String query, boolean enabled) throws Exception {
        configure(enabled);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tables.getConfig().set(ExecutionConfigOptions.IDLE_STATE_RETENTION, java.time.Duration.ZERO);
        tables.createTemporaryView(
                "regular_join_left",
                tables.fromDataStream(
                        environment.fromCollection(
                                List.of(
                                        Row.of(1L, Row.of("left-1", 1), linkedMap("a", 1, "b", 2)),
                                        Row.of(1L, Row.of("left-2", 2), linkedMap("c", 3)),
                                        Row.of(2L, Row.of("left-3", 3), linkedMap("d", 4))),
                                Types.ROW_NAMED(
                                        new String[] {"id", "payload", "attributes"},
                                        Types.LONG,
                                        Types.ROW_NAMED(new String[] {"label", "code"}, Types.STRING, Types.INT),
                                        Types.MAP(Types.STRING, Types.INT))),
                        Schema.newBuilder()
                                .column("id", DataTypes.BIGINT().notNull())
                                .column(
                                        "payload",
                                        DataTypes.ROW(
                                                DataTypes.FIELD("label", DataTypes.STRING()),
                                                DataTypes.FIELD("code", DataTypes.INT())))
                                .column(
                                        "attributes",
                                        DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.INT()))
                                .build()));
        tables.createTemporaryView(
                "regular_join_right",
                tables.fromDataStream(
                        environment.fromCollection(
                                List.of(
                                        Row.of(1L, Row.of("right-1", 11), new BigDecimal("12.34")),
                                        Row.of(3L, Row.of("right-2", 12), new BigDecimal("56.78"))),
                                Types.ROW_NAMED(
                                        new String[] {"id", "payload", "amount"},
                                        Types.LONG,
                                        Types.ROW_NAMED(new String[] {"label", "code"}, Types.STRING, Types.INT),
                                        Types.BIG_DEC)),
                        Schema.newBuilder()
                                .column("id", DataTypes.BIGINT().notNull())
                                .column(
                                        "payload",
                                        DataTypes.ROW(
                                                DataTypes.FIELD("label", DataTypes.STRING()),
                                                DataTypes.FIELD("code", DataTypes.INT())))
                                .column("amount", DataTypes.DECIMAL(10, 2))
                                .build()));
        return collect(tables.executeSql(query));
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

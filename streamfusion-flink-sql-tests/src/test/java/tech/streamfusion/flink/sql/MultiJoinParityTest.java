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
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class MultiJoinParityTest extends SqlParityTestSupport {
    @Test
    void threeInputInnerJoinPreservesDuplicatesAndChangelogBytes() throws Exception {
        String query = "SELECT a.id, a.payload, b.payload, c.payload "
                + "FROM multi_a a JOIN multi_b b ON a.id = b.id JOIN multi_c c ON b.id = c.id";

        byte[] flink = execute(query, false);
        byte[] streamFusion = execute(query, true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(StreamFusionPlannerFactory.nativeMultiJoinBatchCount()).isGreaterThan(0);
    }

    @Test
    void chainedLeftJoinsPreserveNullPaddingTransitions() throws Exception {
        String query = "SELECT a.id, a.payload, b.payload, c.payload "
                + "FROM multi_a a LEFT JOIN multi_b b ON a.id = b.id "
                + "LEFT JOIN multi_c c ON b.id = c.id";

        byte[] flink = executeLeft(query, false);
        byte[] streamFusion = executeLeft(query, true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(StreamFusionPlannerFactory.nativeMultiJoinBatchCount()).isGreaterThan(0);
    }

    private static byte[] execute(String query, boolean enabled) throws Exception {
        return execute(
                query,
                enabled,
                List.of(Row.of(1L, "b1"), Row.of(2L, "b2")),
                List.of(Row.of(1L, "c1"), Row.of(3L, "c2")));
    }

    private static byte[] executeLeft(String query, boolean enabled) throws Exception {
        // Disjoint right inputs make the complete changelog independent of bounded-source
        // interleaving. Ordered null-padding transition parity is covered in the native test.
        return execute(query, enabled, List.of(Row.of(9L, "b9")), List.of(Row.of(10L, "c10")));
    }

    private static byte[] execute(String query, boolean enabled, List<Row> bValues, List<Row> cValues)
            throws Exception {
        configure(enabled);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tables.getConfig().set(ExecutionConfigOptions.IDLE_STATE_RETENTION, java.time.Duration.ZERO);
        tables.getConfig().set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, true);
        create(tables, environment, "multi_a", List.of(Row.of(1L, "a1"), Row.of(1L, "a2"), Row.of(2L, "a3")));
        create(tables, environment, "multi_b", bValues);
        create(tables, environment, "multi_c", cValues);
        return collect(tables.executeSql(query));
    }

    private static void create(
            StreamTableEnvironment tables, StreamExecutionEnvironment environment, String name, List<Row> values) {
        tables.createTemporaryView(
                name,
                tables.fromDataStream(
                        environment.fromCollection(
                                values, Types.ROW_NAMED(new String[] {"id", "payload"}, Types.LONG, Types.STRING)),
                        Schema.newBuilder()
                                .column("id", "BIGINT")
                                .column("payload", "STRING")
                                .build()));
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

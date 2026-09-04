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

/** Complete Arrow-representable state-row type coverage for the native multi-way join. */
class MultiJoinOpaquePayloadTypeParityTest extends SqlParityTestSupport {
    @Test
    void statePreservesEveryFlinkLogicalPayloadTypeByteForByte() throws Exception {
        byte[] flink = execute(false);
        byte[] streamFusion = execute(true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(StreamFusionPlannerFactory.nativeMultiJoinBatchCount()).isGreaterThan(0);
    }

    private static byte[] execute(boolean enabled) throws Exception {
        configure(enabled);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tables.getConfig().set(ExecutionConfigOptions.IDLE_STATE_RETENTION, java.time.Duration.ZERO);
        tables.getConfig().set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED, true);
        create(tables, environment, "typed_a", "alpha", 1);
        create(tables, environment, "typed_b", "beta", 2);
        create(tables, environment, "typed_c", "gamma", 3);
        return collect(tables.executeSql("SELECT a.payload, b.payload, c.payload "
                + "FROM typed_a a JOIN typed_b b ON a.id = b.id JOIN typed_c c ON b.id = c.id"));
    }

    private static void create(
            StreamTableEnvironment tables,
            StreamExecutionEnvironment environment,
            String name,
            String label,
            int value) {
        tables.createTemporaryView(
                name,
                tables.fromDataStream(
                        environment.fromCollection(
                                List.of(Row.of(7L, TopNOpaquePayloadTypeParityTest.payload(label, value))),
                                Types.ROW_NAMED(
                                        new String[] {"id", "payload"},
                                        Types.LONG,
                                        TopNOpaquePayloadTypeParityTest.payloadTypeInformation())),
                        Schema.newBuilder()
                                .column("id", "BIGINT")
                                .column("payload", TopNOpaquePayloadTypeParityTest.payloadDataType())
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

/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
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
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class TemporalJoinParityTest extends SqlParityTestSupport {
    private static final String QUERY = "SELECT o.amount, o.currency, r.rate "
            + "FROM temporal_orders AS o "
            + "LEFT JOIN temporal_rates FOR SYSTEM_TIME AS OF o.rowtime AS r "
            + "ON o.currency = r.currency AND r.rate >= 100";

    @Test
    void versionedEventTimeJoinWithResidualConditionMatchesFlinkByteForByte() throws Exception {
        byte[] flink = execute(false);
        byte[] streamFusion = execute(true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeTemporalJoinBatchCount()).isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    private static byte[] execute(boolean enabled) throws Exception {
        configure(enabled);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tables.getConfig().set(ExecutionConfigOptions.IDLE_STATE_RETENTION, Duration.ZERO);
        tables.createTemporaryView(
                "temporal_orders",
                tables.fromDataStream(
                        environment.fromCollection(
                                List.of(
                                        Row.of(2L, "Euro", timestamp(42)),
                                        Row.of(1L, "USD", timestamp(43)),
                                        Row.of(50L, "Yen", timestamp(44)),
                                        Row.of(3L, "Euro", timestamp(48))),
                                Types.ROW_NAMED(
                                        new String[] {"amount", "currency", "rowtime"},
                                        Types.LONG,
                                        Types.STRING,
                                        Types.LOCAL_DATE_TIME)),
                        Schema.newBuilder()
                                .column("amount", DataTypes.BIGINT().notNull())
                                .column("currency", DataTypes.STRING().notNull())
                                .column("rowtime", DataTypes.TIMESTAMP(3).notNull())
                                .watermark("rowtime", "rowtime")
                                .build()));
        tables.createTemporaryView(
                "temporal_rates",
                tables.fromDataStream(
                        environment.fromCollection(
                                List.of(
                                        Row.of("USD", 102L, timestamp(41)),
                                        Row.of("Euro", 114L, timestamp(41)),
                                        Row.of("Yen", 1L, timestamp(41)),
                                        Row.of("Euro", 116L, timestamp(45)),
                                        Row.of("Euro", 119L, timestamp(47))),
                                Types.ROW_NAMED(
                                        new String[] {"currency", "rate", "rowtime"},
                                        Types.STRING,
                                        Types.LONG,
                                        Types.LOCAL_DATE_TIME)),
                        Schema.newBuilder()
                                .column("currency", DataTypes.STRING().notNull())
                                .column("rate", DataTypes.BIGINT().notNull())
                                .column("rowtime", DataTypes.TIMESTAMP(3).notNull())
                                .watermark("rowtime", "rowtime")
                                .primaryKey("currency")
                                .build()));
        return collect(tables.executeSql(QUERY));
    }

    private static LocalDateTime timestamp(int second) {
        return LocalDateTime.of(2020, 10, 10, 0, 0, second);
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

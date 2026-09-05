/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.AggregatePhaseStrategy;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class WindowAggregateKeyTypeParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void everyScalarKeyRepresentationMatchesFlink(boolean legacy) throws Exception {
        byte[] flink = executeScalars(false, legacy);
        byte[] streamFusion = executeScalars(true, legacy);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        if (!legacy) {
            assertThat(StreamFusionPlannerFactory.nativeLocalWindowAggregateBatchCount())
                    .isGreaterThan(0);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void arrayAndRowKeysUseCanonicalOpaqueFlinkKeys(boolean legacy) throws Exception {
        byte[] flink = executeNested(false, legacy);
        byte[] streamFusion = executeNested(true, legacy);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        if (!legacy) {
            assertThat(StreamFusionPlannerFactory.nativeLocalWindowAggregateBatchCount())
                    .isGreaterThan(0);
        }
    }

    private static byte[] executeScalars(boolean streamFusion, boolean legacy) throws Exception {
        configure(streamFusion);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        if (!legacy) {
            forceTwoPhase(tables);
        }
        LocalDateTime first = LocalDateTime.of(2026, 8, 29, 12, 0, 1);
        LocalDateTime second = LocalDateTime.of(2026, 8, 29, 12, 0, 2);
        tables.createTemporaryView(
                "scalar_window_keys",
                tables.fromDataStream(
                        environment.fromCollection(
                                List.of(
                                        scalarRow(first, first),
                                        scalarRow(first, second),
                                        Row.of(
                                                null, null, null, null, null, null, null, null, null, null, null, null,
                                                null, null, null, null, second)),
                                Types.ROW_NAMED(
                                        new String[] {
                                            "tiny_value",
                                            "small_value",
                                            "integer_value",
                                            "big_value",
                                            "float_value",
                                            "double_value",
                                            "boolean_value",
                                            "char_value",
                                            "varchar_value",
                                            "binary_value",
                                            "varbinary_value",
                                            "decimal_value",
                                            "date_value",
                                            "time_value",
                                            "timestamp_value",
                                            "timestamp_ltz_value",
                                            "ts"
                                        },
                                        Types.BYTE,
                                        Types.SHORT,
                                        Types.INT,
                                        Types.LONG,
                                        Types.FLOAT,
                                        Types.DOUBLE,
                                        Types.BOOLEAN,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.PRIMITIVE_ARRAY(Types.BYTE),
                                        Types.PRIMITIVE_ARRAY(Types.BYTE),
                                        Types.BIG_DEC,
                                        Types.LOCAL_DATE,
                                        Types.LOCAL_TIME,
                                        Types.LOCAL_DATE_TIME,
                                        Types.INSTANT,
                                        Types.LOCAL_DATE_TIME)),
                        Schema.newBuilder()
                                .column("tiny_value", "TINYINT")
                                .column("small_value", "SMALLINT")
                                .column("integer_value", "INT")
                                .column("big_value", "BIGINT")
                                .column("float_value", "FLOAT")
                                .column("double_value", "DOUBLE")
                                .column("boolean_value", "BOOLEAN")
                                .column("char_value", "CHAR(3)")
                                .column("varchar_value", "VARCHAR(8)")
                                .column("binary_value", "BINARY(2)")
                                .column("varbinary_value", "VARBINARY(4)")
                                .column("decimal_value", "DECIMAL(10, 2)")
                                .column("date_value", "DATE")
                                .column("time_value", "TIME(3)")
                                .column("timestamp_value", "TIMESTAMP(3)")
                                .column("timestamp_ltz_value", "TIMESTAMP_LTZ(3)")
                                .column("ts", "TIMESTAMP(3)")
                                .watermark("ts", "ts - INTERVAL '1' SECOND")
                                .build()));
        String keys = "tiny_value, small_value, integer_value, big_value, float_value, double_value, "
                + "boolean_value, char_value, varchar_value, binary_value, varbinary_value, decimal_value, "
                + "date_value, time_value, timestamp_value, timestamp_ltz_value";
        String sql = legacy
                ? "SELECT "
                        + keys
                        + ", COUNT(*), TUMBLE_START(ts, INTERVAL '10' SECOND), "
                        + "TUMBLE_END(ts, INTERVAL '10' SECOND) FROM scalar_window_keys GROUP BY "
                        + keys
                        + ", TUMBLE(ts, INTERVAL '10' SECOND)"
                : "SELECT "
                        + keys
                        + ", COUNT(*), window_start, window_end FROM TABLE("
                        + "TUMBLE(TABLE scalar_window_keys, DESCRIPTOR(ts), INTERVAL '10' SECOND)) GROUP BY "
                        + keys
                        + ", window_start, window_end";
        return collect(tables.executeSql(sql));
    }

    private static Row scalarRow(LocalDateTime keyTimestamp, LocalDateTime eventTimestamp) {
        return Row.of(
                (byte) 1,
                (short) 2,
                3,
                4L,
                1.5F,
                2.5D,
                true,
                "abc",
                "text",
                new byte[] {1, 2},
                new byte[] {3, 4},
                new BigDecimal("12.34"),
                LocalDate.of(2026, 8, 29),
                LocalTime.of(12, 34, 56, 123_000_000),
                keyTimestamp,
                keyTimestamp.toInstant(java.time.ZoneOffset.UTC),
                eventTimestamp);
    }

    private static byte[] executeNested(boolean streamFusion, boolean legacy) throws Exception {
        configure(streamFusion);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        if (!legacy) {
            forceTwoPhase(tables);
        }
        tables.createTemporaryView(
                "nested_window_keys",
                tables.fromDataStream(
                        environment.fromCollection(
                                List.of(
                                        Row.of(
                                                new Integer[] {1, 2},
                                                Row.of("alpha", 7),
                                                LocalDateTime.of(2026, 8, 29, 12, 0, 1)),
                                        Row.of(
                                                new Integer[] {1, 2},
                                                Row.of("alpha", 7),
                                                LocalDateTime.of(2026, 8, 29, 12, 0, 2))),
                                Types.ROW_NAMED(
                                        new String[] {"array_value", "row_value", "ts"},
                                        Types.OBJECT_ARRAY(Types.INT),
                                        Types.ROW_NAMED(new String[] {"label", "score"}, Types.STRING, Types.INT),
                                        Types.LOCAL_DATE_TIME)),
                        Schema.newBuilder()
                                .column("array_value", DataTypes.ARRAY(DataTypes.INT()))
                                .column(
                                        "row_value",
                                        DataTypes.ROW(
                                                DataTypes.FIELD("label", DataTypes.STRING()),
                                                DataTypes.FIELD("score", DataTypes.INT())))
                                .column("ts", DataTypes.TIMESTAMP(3))
                                .watermark("ts", "ts - INTERVAL '1' SECOND")
                                .build()));
        String sql = legacy
                ? "SELECT array_value, row_value, COUNT(*), TUMBLE_START(ts, INTERVAL '10' SECOND), "
                        + "TUMBLE_END(ts, INTERVAL '10' SECOND) FROM nested_window_keys "
                        + "GROUP BY array_value, row_value, TUMBLE(ts, INTERVAL '10' SECOND)"
                : "SELECT array_value, row_value, COUNT(*), window_start, window_end "
                        + "FROM TABLE(TUMBLE(TABLE nested_window_keys, DESCRIPTOR(ts), INTERVAL '10' SECOND)) "
                        + "GROUP BY array_value, row_value, window_start, window_end";
        return collect(tables.executeSql(sql));
    }

    private static void configure(boolean streamFusion) {
        if (streamFusion) {
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        } else {
            System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            StreamFusionPlannerFactory.resetMetrics();
        }
    }

    private static void forceTwoPhase(StreamTableEnvironment tables) {
        tables.getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, AggregatePhaseStrategy.TWO_PHASE);
    }
}

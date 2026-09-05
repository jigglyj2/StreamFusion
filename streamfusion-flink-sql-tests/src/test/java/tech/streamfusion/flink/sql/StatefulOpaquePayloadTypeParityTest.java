/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class StatefulOpaquePayloadTypeParityTest extends SqlParityTestSupport {
    @Test
    void overStatePreservesEveryFlinkLogicalPartitionAndPayloadTypeByteForByte() throws Exception {
        byte[] flink = executeOver(false);
        byte[] streamFusion = executeOver(true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeOverAggregateBatchCount()).isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void windowStatePreservesEveryFlinkLogicalPayloadTypeByteForByte() throws Exception {
        byte[] flink = execute(false, "order_value", false);
        byte[] streamFusion = execute(true, "order_value", false);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeWindowRankBatchCount()).isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void windowDeduplicateTimerOutputPreservesEveryFlinkLogicalPayloadTypeByteForByte() throws Exception {
        byte[] flink = execute(false, "ts", false);
        byte[] streamFusion = execute(true, "ts", false);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeWindowDeduplicateBatchCount())
                .isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void sessionTimerOutputPreservesEveryFlinkLogicalPayloadTypeByteForByte() throws Exception {
        byte[] flink = execute(false, null, true);
        byte[] streamFusion = execute(true, null, true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void windowJoinStateAndArrowGatherPreserveEveryFlinkLogicalPayloadTypeByteForByte() throws Exception {
        byte[] flink = executeJoin(false);
        byte[] streamFusion = executeJoin(true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeWindowJoinBatchCount()).isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void boundedJoinUsesAnOpaqueAllOrderableTypeRowAsItsEqualityKeyByteForByte() throws Exception {
        byte[] flink = executeBoundedJoin(false);
        byte[] streamFusion = executeBoundedJoin(true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeRegularJoinBatchCount())
                .as(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void boundedSortMergeJoinUsesTheNativeAllTypeJoinContractByteForByte() throws Exception {
        byte[] flink = executeSortMergeJoin(false);
        byte[] streamFusion = executeSortMergeJoin(true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeRegularJoinBatchCount())
                .as(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void changelogNormalizeStatePreservesEveryFlinkLogicalPayloadTypeByteForByte() throws Exception {
        byte[] flink = executeChangelogNormalize(false);
        byte[] streamFusion = executeChangelogNormalize(true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeChangelogNormalizeBatchCount())
                .isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void matchRecognizeStatePreservesEveryFlinkLogicalPartitionAndPayloadTypeByteForByte() throws Exception {
        byte[] flink = executeMatchRecognize(false);
        byte[] streamFusion = executeMatchRecognize(true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeMatchRecognizeBatchCount()).isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    private static byte[] execute(boolean streamFusionEnabled, String orderField, boolean session) throws Exception {
        configure(streamFusionEnabled);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        TypeInformation<Row> payloadType = payloadTypeInformation();
        tables.createTemporaryView(
                "window_all_type_payload",
                tables.fromDataStream(
                        environment.fromCollection(
                                List.of(
                                        Row.of("a", 1L, payload("alpha", 1), LocalDateTime.of(2026, 9, 1, 12, 0, 1)),
                                        Row.of("a", 2L, payload("beta", 2), LocalDateTime.of(2026, 9, 1, 12, 0, 2))),
                                Types.ROW_NAMED(
                                        new String[] {"category", "order_value", "payload", "ts"},
                                        Types.STRING,
                                        Types.LONG,
                                        payloadType,
                                        Types.LOCAL_DATE_TIME)),
                        Schema.newBuilder()
                                .column("category", DataTypes.STRING().notNull())
                                .column("order_value", DataTypes.BIGINT().notNull())
                                .column("payload", payloadDataType())
                                .column("ts", DataTypes.TIMESTAMP(3))
                                .watermark("ts", "ts - INTERVAL '1' SECOND")
                                .build()));
        if (session) {
            return collect(
                    tables.executeSql("SELECT payload, window_start, window_end, window_time FROM TABLE(SESSION(TABLE "
                            + "window_all_type_payload PARTITION BY category, DESCRIPTOR(ts), INTERVAL '5' SECOND))"));
        }
        return collect(tables.executeSql("SELECT payload, window_start, window_end, row_num FROM ("
                + "SELECT *, ROW_NUMBER() OVER (PARTITION BY category, window_start, window_end "
                + "ORDER BY "
                + orderField
                + " DESC) AS row_num FROM TABLE(TUMBLE(TABLE "
                + "window_all_type_payload, DESCRIPTOR(ts), INTERVAL '5' SECOND))) WHERE row_num = 1"));
    }

    private static byte[] executeOver(boolean streamFusionEnabled) throws Exception {
        configure(streamFusionEnabled);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tables.createTemporaryView(
                "over_all_type_payload",
                tables.fromDataStream(
                        environment.fromCollection(
                                List.of(Row.of(2L, payload("alpha", 1)), Row.of(1L, payload("alpha", 1))),
                                Types.ROW_NAMED(
                                        new String[] {"order_value", "payload"}, Types.LONG, payloadTypeInformation())),
                        Schema.newBuilder()
                                .column("order_value", DataTypes.BIGINT().notNull())
                                .column("payload", payloadDataType())
                                .build()));
        return collect(tables.executeSql("SELECT payload, order_value, "
                + "SUM(order_value) OVER (PARTITION BY payload ORDER BY order_value ROWS "
                + "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM over_all_type_payload"));
    }

    private static byte[] executeJoin(boolean streamFusionEnabled) throws Exception {
        configure(streamFusionEnabled);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        createPayloadInput(
                tables,
                environment,
                "window_all_type_left",
                List.of(
                        Row.of("a", payload("alpha", 1), LocalDateTime.of(2026, 9, 1, 12, 0, 1)),
                        Row.of("b", payload("beta", 2), LocalDateTime.of(2026, 9, 1, 12, 0, 7))));
        createPayloadInput(
                tables,
                environment,
                "window_all_type_right",
                List.of(
                        Row.of("a", payload("gamma", 3), LocalDateTime.of(2026, 9, 1, 12, 0, 2)),
                        Row.of("c", payload("delta", 4), LocalDateTime.of(2026, 9, 1, 12, 0, 7))));
        return collect(tables.executeSql("SELECT l.payload, r.payload, l.window_start, l.window_end "
                + "FROM TABLE(TUMBLE(TABLE window_all_type_left, DESCRIPTOR(ts), INTERVAL '5' SECOND)) l "
                + "FULL OUTER JOIN TABLE(TUMBLE(TABLE window_all_type_right, DESCRIPTOR(ts), "
                + "INTERVAL '5' SECOND)) r ON l.category = r.category "
                + "AND l.window_start = r.window_start AND l.window_end = r.window_end"));
    }

    private static byte[] executeBoundedJoin(boolean streamFusionEnabled) throws Exception {
        configure(streamFusionEnabled);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inBatchMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        createBoundedJoinInput(
                tables,
                environment,
                "bounded_all_type_left",
                List.of(
                        Row.of("left-match", comparableKey("alpha", 1)),
                        Row.of("left-only", comparableKey("beta", 2))));
        createBoundedJoinInput(
                tables,
                environment,
                "bounded_all_type_right",
                List.of(
                        Row.of("right-match", comparableKey("alpha", 1)),
                        Row.of("right-only", comparableKey("gamma", 3))));
        return collect(tables.executeSql("SELECT l.label, r.label, l.key_payload "
                + "FROM bounded_all_type_left l JOIN bounded_all_type_right r "
                + "ON l.key_payload = r.key_payload"));
    }

    private static byte[] executeSortMergeJoin(boolean streamFusionEnabled) throws Exception {
        configure(streamFusionEnabled);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inBatchMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,NestedLoopJoin");
        tables.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_ADAPTIVE_BROADCAST_JOIN_STRATEGY,
                        OptimizerConfigOptions.AdaptiveBroadcastJoinStrategy.NONE);
        tables.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_ADAPTIVE_SKEWED_JOIN_OPTIMIZATION_STRATEGY,
                        OptimizerConfigOptions.AdaptiveSkewedJoinOptimizationStrategy.NONE);
        return collect(tables.executeSql("SELECT l.label, r.label FROM "
                + "(VALUES (1, 'alpha', CAST(12.34 AS DECIMAL(20, 2)), "
                + "TIMESTAMP '2026-09-01 12:00:01', 'left-match'), "
                + "(2, 'beta', CAST(23.45 AS DECIMAL(20, 2)), "
                + "TIMESTAMP '2026-09-01 12:00:02', 'left-only')) "
                + "AS l(id, name, amount, event_time, label) JOIN "
                + "(VALUES (1, 'alpha', CAST(12.34 AS DECIMAL(20, 2)), "
                + "TIMESTAMP '2026-09-01 12:00:01', 'right-match'), "
                + "(3, 'gamma', CAST(34.56 AS DECIMAL(20, 2)), "
                + "TIMESTAMP '2026-09-01 12:00:03', 'right-only')) "
                + "AS r(id, name, amount, event_time, label) "
                + "ON l.id = r.id AND l.name = r.name AND l.amount = r.amount "
                + "AND l.event_time = r.event_time"));
    }

    private static void createBoundedJoinInput(
            StreamTableEnvironment tables, StreamExecutionEnvironment environment, String name, List<Row> rows) {
        tables.createTemporaryView(
                name,
                tables.fromDataStream(
                        environment.fromCollection(
                                rows,
                                Types.ROW_NAMED(
                                        new String[] {"label", "key_payload"},
                                        Types.STRING,
                                        comparableKeyTypeInformation())),
                        Schema.newBuilder()
                                .column("label", DataTypes.STRING().notNull())
                                .column("key_payload", comparableKeyDataType())
                                .build()));
    }

    private static byte[] executeChangelogNormalize(boolean streamFusionEnabled) throws Exception {
        configure(streamFusionEnabled);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tables.createTemporaryView(
                "all_type_upsert_input",
                tables.fromChangelogStream(
                        environment.fromCollection(
                                List.of(
                                        Row.ofKind(RowKind.INSERT, 1L, payload("alpha", 1)),
                                        Row.ofKind(RowKind.UPDATE_AFTER, 1L, payload("beta", 2)),
                                        Row.ofKind(RowKind.INSERT, 2L, payload("gamma", 3)),
                                        Row.ofKind(RowKind.DELETE, 1L, null)),
                                Types.ROW_NAMED(new String[] {"id", "payload"}, Types.LONG, payloadTypeInformation())),
                        Schema.newBuilder()
                                .column("id", DataTypes.BIGINT().notNull())
                                .column("payload", payloadDataType())
                                .primaryKey("id")
                                .build(),
                        ChangelogMode.upsert()));
        return collect(tables.executeSql("SELECT * FROM all_type_upsert_input"));
    }

    private static byte[] executeMatchRecognize(boolean streamFusionEnabled) throws Exception {
        configure(streamFusionEnabled);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        Row allTypes = payload("alpha", 1);
        tables.createTemporaryView(
                "match_all_type_payload",
                tables.fromDataStream(
                        environment.fromCollection(
                                List.of(Row.of("a", allTypes), Row.of("b", allTypes), Row.of("c", allTypes)),
                                Types.ROW_NAMED(
                                        new String[] {"label", "payload"}, Types.STRING, payloadTypeInformation())),
                        Schema.newBuilder()
                                .column("label", DataTypes.STRING().notNull())
                                .column("payload", payloadDataType())
                                .columnByExpression("pt", "PROCTIME()")
                                .build()));
        return collect(tables.executeSql("SELECT payload, matched_payload FROM match_all_type_payload "
                + "MATCH_RECOGNIZE (PARTITION BY payload ORDER BY pt "
                + "MEASURES C.payload AS matched_payload ONE ROW PER MATCH "
                + "AFTER MATCH SKIP PAST LAST ROW PATTERN (A B C) "
                + "DEFINE A AS label = 'a', B AS label = 'b', C AS label = 'c')"));
    }

    private static void createPayloadInput(
            StreamTableEnvironment tables, StreamExecutionEnvironment environment, String name, List<Row> rows) {
        tables.createTemporaryView(
                name,
                tables.fromDataStream(
                        environment.fromCollection(
                                rows,
                                Types.ROW_NAMED(
                                        new String[] {"category", "payload", "ts"},
                                        Types.STRING,
                                        payloadTypeInformation(),
                                        Types.LOCAL_DATE_TIME)),
                        Schema.newBuilder()
                                .column("category", DataTypes.STRING().notNull())
                                .column("payload", payloadDataType())
                                .column("ts", DataTypes.TIMESTAMP(3))
                                .watermark("ts", "ts - INTERVAL '1' SECOND")
                                .build()));
    }

    private static TypeInformation<Row> payloadTypeInformation() {
        return Types.ROW_NAMED(
                new String[] {
                    "boolean_value",
                    "tiny_value",
                    "small_value",
                    "integer_value",
                    "big_value",
                    "float_value",
                    "double_value",
                    "char_value",
                    "varchar_value",
                    "binary_value",
                    "varbinary_value",
                    "decimal_value",
                    "date_value",
                    "time_value",
                    "timestamp_value",
                    "timestamp_ltz_value",
                    "year_month_value",
                    "day_time_value",
                    "array_value",
                    "map_value",
                    "multiset_value",
                    "row_value"
                },
                Types.BOOLEAN,
                Types.BYTE,
                Types.SHORT,
                Types.INT,
                Types.LONG,
                Types.FLOAT,
                Types.DOUBLE,
                Types.STRING,
                Types.STRING,
                Types.PRIMITIVE_ARRAY(Types.BYTE),
                Types.PRIMITIVE_ARRAY(Types.BYTE),
                Types.BIG_DEC,
                Types.LOCAL_DATE,
                Types.LOCAL_TIME,
                Types.LOCAL_DATE_TIME,
                Types.INSTANT,
                Types.GENERIC(Period.class),
                Types.GENERIC(Duration.class),
                Types.OBJECT_ARRAY(Types.INT),
                Types.MAP(Types.STRING, Types.INT),
                Types.MAP(Types.STRING, Types.INT),
                Types.ROW_NAMED(new String[] {"id", "label"}, Types.LONG, Types.STRING));
    }

    private static TypeInformation<Row> comparableKeyTypeInformation() {
        return Types.ROW_NAMED(
                new String[] {
                    "boolean_value",
                    "tiny_value",
                    "small_value",
                    "integer_value",
                    "big_value",
                    "float_value",
                    "double_value",
                    "char_value",
                    "varchar_value",
                    "binary_value",
                    "varbinary_value",
                    "decimal_value",
                    "date_value",
                    "time_value",
                    "timestamp_value",
                    "timestamp_ltz_value",
                    "year_month_value",
                    "day_time_value",
                    "row_value"
                },
                Types.BOOLEAN,
                Types.BYTE,
                Types.SHORT,
                Types.INT,
                Types.LONG,
                Types.FLOAT,
                Types.DOUBLE,
                Types.STRING,
                Types.STRING,
                Types.PRIMITIVE_ARRAY(Types.BYTE),
                Types.PRIMITIVE_ARRAY(Types.BYTE),
                Types.BIG_DEC,
                Types.LOCAL_DATE,
                Types.LOCAL_TIME,
                Types.LOCAL_DATE_TIME,
                Types.INSTANT,
                Types.GENERIC(Period.class),
                Types.GENERIC(Duration.class),
                Types.ROW_NAMED(new String[] {"id", "label"}, Types.LONG, Types.STRING));
    }

    private static DataType payloadDataType() {
        return DataTypes.ROW(
                DataTypes.FIELD("boolean_value", DataTypes.BOOLEAN()),
                DataTypes.FIELD("tiny_value", DataTypes.TINYINT()),
                DataTypes.FIELD("small_value", DataTypes.SMALLINT()),
                DataTypes.FIELD("integer_value", DataTypes.INT()),
                DataTypes.FIELD("big_value", DataTypes.BIGINT()),
                DataTypes.FIELD("float_value", DataTypes.FLOAT()),
                DataTypes.FIELD("double_value", DataTypes.DOUBLE()),
                DataTypes.FIELD("char_value", DataTypes.CHAR(5)),
                DataTypes.FIELD("varchar_value", DataTypes.VARCHAR(20)),
                DataTypes.FIELD("binary_value", DataTypes.BINARY(3)),
                DataTypes.FIELD("varbinary_value", DataTypes.VARBINARY(20)),
                DataTypes.FIELD("decimal_value", DataTypes.DECIMAL(25, 2)),
                DataTypes.FIELD("date_value", DataTypes.DATE()),
                DataTypes.FIELD("time_value", DataTypes.TIME(3)),
                DataTypes.FIELD("timestamp_value", DataTypes.TIMESTAMP(6)),
                DataTypes.FIELD("timestamp_ltz_value", DataTypes.TIMESTAMP_LTZ(6)),
                DataTypes.FIELD("year_month_value", DataTypes.INTERVAL(DataTypes.YEAR(), DataTypes.MONTH())),
                DataTypes.FIELD("day_time_value", DataTypes.INTERVAL(DataTypes.DAY(), DataTypes.SECOND(3))),
                DataTypes.FIELD("array_value", DataTypes.ARRAY(DataTypes.INT())),
                DataTypes.FIELD("map_value", DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.INT())),
                DataTypes.FIELD(
                        "multiset_value", DataTypes.MULTISET(DataTypes.STRING().notNull())),
                DataTypes.FIELD(
                        "row_value",
                        DataTypes.ROW(
                                DataTypes.FIELD("id", DataTypes.BIGINT()),
                                DataTypes.FIELD("label", DataTypes.STRING()))));
    }

    private static DataType comparableKeyDataType() {
        return DataTypes.ROW(
                DataTypes.FIELD("boolean_value", DataTypes.BOOLEAN()),
                DataTypes.FIELD("tiny_value", DataTypes.TINYINT()),
                DataTypes.FIELD("small_value", DataTypes.SMALLINT()),
                DataTypes.FIELD("integer_value", DataTypes.INT()),
                DataTypes.FIELD("big_value", DataTypes.BIGINT()),
                DataTypes.FIELD("float_value", DataTypes.FLOAT()),
                DataTypes.FIELD("double_value", DataTypes.DOUBLE()),
                DataTypes.FIELD("char_value", DataTypes.CHAR(5)),
                DataTypes.FIELD("varchar_value", DataTypes.VARCHAR(20)),
                DataTypes.FIELD("binary_value", DataTypes.BINARY(3)),
                DataTypes.FIELD("varbinary_value", DataTypes.VARBINARY(20)),
                DataTypes.FIELD("decimal_value", DataTypes.DECIMAL(25, 2)),
                DataTypes.FIELD("date_value", DataTypes.DATE()),
                DataTypes.FIELD("time_value", DataTypes.TIME(3)),
                DataTypes.FIELD("timestamp_value", DataTypes.TIMESTAMP(6)),
                DataTypes.FIELD("timestamp_ltz_value", DataTypes.TIMESTAMP_LTZ(6)),
                DataTypes.FIELD("year_month_value", DataTypes.INTERVAL(DataTypes.YEAR(), DataTypes.MONTH())),
                DataTypes.FIELD("day_time_value", DataTypes.INTERVAL(DataTypes.DAY(), DataTypes.SECOND(3))),
                DataTypes.FIELD(
                        "row_value",
                        DataTypes.ROW(
                                DataTypes.FIELD("id", DataTypes.BIGINT()),
                                DataTypes.FIELD("label", DataTypes.STRING()))));
    }

    private static Row payload(String label, int value) {
        return Row.of(
                value % 2 == 0,
                (byte) value,
                (short) (value * 10),
                value * 100,
                value * 1_000L,
                value + 0.25F,
                value + 0.5D,
                label,
                label + "-varchar",
                new byte[] {(byte) value, 2, 3},
                new byte[] {4, (byte) value, 6},
                new BigDecimal("123456789012345678901.25").add(BigDecimal.valueOf(value)),
                LocalDate.of(2026, 9, value),
                LocalTime.of(1, 2, value, 4_000_000),
                LocalDateTime.of(2026, 9, value, 3, 4, 5, 6_007_000),
                Instant.parse("2026-09-01T12:13:14.015016Z").plusSeconds(value),
                Period.ofMonths(value * 13),
                Duration.ofMillis(value * 90_061_007L),
                new Integer[] {value, null, -value},
                linkedMap(label, value, "nullable", null),
                linkedMap(label, value + 1),
                Row.of((long) value, label));
    }

    private static Row comparableKey(String label, int value) {
        Row payload = payload(label, value);
        return Row.of(
                payload.getField(0),
                payload.getField(1),
                payload.getField(2),
                payload.getField(3),
                payload.getField(4),
                payload.getField(5),
                payload.getField(6),
                payload.getField(7),
                payload.getField(8),
                payload.getField(9),
                payload.getField(10),
                payload.getField(11),
                payload.getField(12),
                payload.getField(13),
                payload.getField(14),
                payload.getField(15),
                payload.getField(16),
                payload.getField(17),
                payload.getField(21));
    }

    private static Map<String, Integer> linkedMap(Object... entries) {
        Map<String, Integer> values = new LinkedHashMap<>();
        for (int index = 0; index < entries.length; index += 2) {
            values.put((String) entries[index], (Integer) entries[index + 1]);
        }
        return values;
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

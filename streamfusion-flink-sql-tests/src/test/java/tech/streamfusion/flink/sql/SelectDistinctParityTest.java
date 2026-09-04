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
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class SelectDistinctParityTest extends SqlParityTestSupport {
    @ParameterizedTest(name = "{0}")
    @MethodSource("distinctTypes")
    void everyFlinkDistinctKeyTypeMatchesByteForByte(
            String description, TypeInformation<?> type, DataType dataType, Object first, Object second)
            throws Exception {
        assertDataStreamParity(
                "SELECT DISTINCT metric FROM distinct_input",
                type,
                dataType,
                List.of(Row.of(first), Row.of(first), Row.of(second), Row.of((Object) null)),
                "distinct_input");
        assertThat(StreamFusionPlanningDiagnostics.explain())
                .contains("Accelerated: yes")
                .doesNotContain("StreamExecGroupAggregate [INTERNAL]");
    }

    @Test
    void insertUpdateAndDeleteChangelogMatchesFlinkByteForByte() throws Exception {
        byte[] flink = executeRetractions(false);
        byte[] streamFusion = executeRetractions(true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    static Stream<Arguments> distinctTypes() {
        return Stream.of(
                Arguments.of("BOOLEAN", Types.BOOLEAN, DataTypes.BOOLEAN(), true, false),
                Arguments.of("TINYINT", Types.BYTE, DataTypes.TINYINT(), (byte) -7, (byte) 8),
                Arguments.of("SMALLINT", Types.SHORT, DataTypes.SMALLINT(), (short) -700, (short) 800),
                Arguments.of("INTEGER", Types.INT, DataTypes.INT(), -70_000, 80_000),
                Arguments.of("BIGINT", Types.LONG, DataTypes.BIGINT(), -7_000_000_000L, 8_000_000_000L),
                Arguments.of("FLOAT", Types.FLOAT, DataTypes.FLOAT(), -1.25F, 2.5F),
                Arguments.of("DOUBLE", Types.DOUBLE, DataTypes.DOUBLE(), -1.25D, 2.5D),
                Arguments.of("CHAR", Types.STRING, DataTypes.CHAR(5), "abc", "xyz"),
                Arguments.of("VARCHAR", Types.STRING, DataTypes.VARCHAR(20), "alpha", "beta"),
                Arguments.of(
                        "BINARY",
                        Types.PRIMITIVE_ARRAY(Types.BYTE),
                        DataTypes.BINARY(3),
                        new byte[] {1, 2, 3},
                        new byte[] {4, 5, 6}),
                Arguments.of(
                        "VARBINARY",
                        Types.PRIMITIVE_ARRAY(Types.BYTE),
                        DataTypes.VARBINARY(20),
                        new byte[] {1, 2, 3},
                        new byte[] {4, 5, 6}),
                Arguments.of(
                        "DECIMAL",
                        Types.BIG_DEC,
                        DataTypes.DECIMAL(25, 2),
                        new BigDecimal("-123456789012345678901.25"),
                        new BigDecimal("2.50")),
                Arguments.of(
                        "DATE", Types.LOCAL_DATE, DataTypes.DATE(), LocalDate.of(1960, 1, 2), LocalDate.of(2026, 9, 1)),
                Arguments.of(
                        "TIME",
                        Types.LOCAL_TIME,
                        DataTypes.TIME(3),
                        LocalTime.of(1, 2, 3, 4_000_000),
                        LocalTime.of(12, 13, 14, 15_000_000)),
                Arguments.of(
                        "TIMESTAMP",
                        Types.LOCAL_DATE_TIME,
                        DataTypes.TIMESTAMP(6),
                        LocalDateTime.of(1960, 1, 2, 3, 4, 5, 6_007_000),
                        LocalDateTime.of(2026, 9, 1, 12, 13, 14, 15_016_000)),
                Arguments.of(
                        "TIMESTAMP_LTZ",
                        Types.INSTANT,
                        DataTypes.TIMESTAMP_LTZ(6),
                        Instant.ofEpochSecond(-1, 999_000),
                        Instant.parse("2026-09-01T12:13:14.015016Z")),
                Arguments.of(
                        "INTERVAL YEAR TO MONTH",
                        Types.GENERIC(Period.class),
                        DataTypes.INTERVAL(DataTypes.YEAR(), DataTypes.MONTH()),
                        Period.ofMonths(-25),
                        Period.ofMonths(38)),
                Arguments.of(
                        "INTERVAL DAY TO SECOND",
                        Types.GENERIC(Duration.class),
                        DataTypes.INTERVAL(DataTypes.DAY(), DataTypes.SECOND(3)),
                        Duration.ofMillis(-90_061_007L),
                        Duration.ofMillis(183_845_006L)),
                Arguments.of(
                        "ARRAY",
                        Types.OBJECT_ARRAY(Types.INT),
                        DataTypes.ARRAY(DataTypes.INT()),
                        new Integer[] {1, null, -2},
                        new Integer[] {3, 4}),
                Arguments.of(
                        "MAP",
                        Types.MAP(Types.STRING, Types.INT),
                        DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.INT()),
                        linkedMap("a", 1, "b", null),
                        linkedMap("c", 3)),
                Arguments.of(
                        "MULTISET",
                        Types.MAP(Types.STRING, Types.INT),
                        DataTypes.MULTISET(DataTypes.STRING().notNull()),
                        linkedMap("a", 2, "b", 1),
                        linkedMap("c", 1)),
                Arguments.of(
                        "ROW",
                        Types.ROW_NAMED(new String[] {"id", "label"}, Types.LONG, Types.STRING),
                        DataTypes.ROW(
                                DataTypes.FIELD("id", DataTypes.BIGINT()),
                                DataTypes.FIELD("label", DataTypes.STRING())),
                        Row.of(42L, "nested"),
                        Row.of(43L, "other")));
    }

    private static Map<String, Integer> linkedMap(Object... entries) {
        Map<String, Integer> values = new LinkedHashMap<>();
        for (int index = 0; index < entries.length; index += 2) {
            values.put((String) entries[index], (Integer) entries[index + 1]);
        }
        return values;
    }

    private static byte[] executeRetractions(boolean streamFusion) throws Exception {
        if (streamFusion) {
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        } else {
            System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            StreamFusionPlannerFactory.resetMetrics();
        }
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        DataStream<Row> changes = environment.fromCollection(
                List.of(
                        row(RowKind.INSERT, "a", 1L),
                        row(RowKind.INSERT, "a", 1L),
                        row(RowKind.UPDATE_BEFORE, "a", 1L),
                        row(RowKind.UPDATE_AFTER, "b", 2L),
                        row(RowKind.DELETE, "a", 1L),
                        row(RowKind.DELETE, "b", 2L),
                        row(RowKind.DELETE, "missing", 9L)),
                Types.ROW_NAMED(new String[] {"label", "amount"}, Types.STRING, Types.LONG));
        Table input = tables.fromChangelogStream(
                changes,
                Schema.newBuilder()
                        .column("label", "STRING")
                        .column("amount", "BIGINT")
                        .build());
        tables.createTemporaryView("distinct_changes", input);
        return collect(tables.executeSql("SELECT DISTINCT label, amount FROM distinct_changes"));
    }

    private static Row row(RowKind kind, String label, long value) {
        Row row = Row.of(label, value);
        row.setKind(kind);
        return row;
    }
}

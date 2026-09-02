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
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class TopNOpaquePayloadTypeParityTest extends SqlParityTestSupport {
    @Test
    void topNStatePreservesEveryFlinkLogicalPayloadTypeByteForByte() throws Exception {
        byte[] flink = execute(false);
        byte[] streamFusion = execute(true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeTopNBatchCount()).isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    private static byte[] execute(boolean streamFusion) throws Exception {
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
        tables.createTemporaryView(
                "topn_all_type_payload",
                tables.fromChangelogStream(
                        environment.fromCollection(
                                List.of(
                                        changed(RowKind.INSERT, 1L),
                                        changed(RowKind.INSERT, 3L),
                                        changed(RowKind.INSERT, 2L),
                                        changed(RowKind.DELETE, 3L)),
                                Types.ROW_NAMED(
                                        new String[] {"category", "order_value", "payload"},
                                        Types.STRING,
                                        Types.LONG,
                                        payloadTypeInformation())),
                        Schema.newBuilder()
                                .column("category", DataTypes.STRING().notNull())
                                .column("order_value", DataTypes.BIGINT().notNull())
                                .column("payload", payloadDataType())
                                .build()));
        return collect(tables.executeSql("SELECT payload, row_num FROM ("
                + "SELECT *, ROW_NUMBER() OVER (PARTITION BY payload ORDER BY order_value DESC) AS row_num "
                + "FROM topn_all_type_payload) WHERE row_num <= 2"));
    }

    private static Row changed(RowKind kind, long orderValue) {
        Row row = Row.of("a", orderValue, payload("alpha", 1));
        row.setKind(kind);
        return row;
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

    private static Map<String, Integer> linkedMap(Object... entries) {
        Map<String, Integer> values = new LinkedHashMap<>();
        for (int index = 0; index < entries.length; index += 2) {
            values.put((String) entries[index], (Integer) entries[index + 1]);
        }
        return values;
    }
}

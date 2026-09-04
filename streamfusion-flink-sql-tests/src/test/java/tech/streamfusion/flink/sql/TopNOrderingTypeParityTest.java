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
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class TopNOrderingTypeParityTest extends SqlParityTestSupport {
    @ParameterizedTest(name = "{0}")
    @MethodSource("orderableTypes")
    void everyFlinkOrderableTypeUsesGeneratedComparatorParity(
            String description, TypeInformation<?> type, DataType dataType, Object low, Object high) throws Exception {
        assertDataStreamParity(
                "SELECT metric FROM (SELECT metric, ROW_NUMBER() OVER "
                        + "(ORDER BY metric ASC NULLS LAST) AS row_num FROM topn_order_input) WHERE row_num <= 3",
                type,
                dataType,
                List.of(Row.of(high), Row.of(low), Row.of(high), Row.of((Object) null)),
                "topn_order_input");
        assertThat(StreamFusionPlannerFactory.nativeTopNBatchCount()).isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    static Stream<Arguments> orderableTypes() {
        return Stream.of(
                Arguments.of("BOOLEAN", Types.BOOLEAN, DataTypes.BOOLEAN(), false, true),
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
                        "ARRAY<INTEGER>",
                        Types.OBJECT_ARRAY(Types.INT),
                        DataTypes.ARRAY(DataTypes.INT()),
                        new Integer[] {1, 2},
                        new Integer[] {1, 3}),
                Arguments.of(
                        "ROW<BIGINT, VARCHAR>",
                        Types.ROW_NAMED(new String[] {"id", "label"}, Types.LONG, Types.STRING),
                        DataTypes.ROW(
                                DataTypes.FIELD("id", DataTypes.BIGINT()),
                                DataTypes.FIELD("label", DataTypes.STRING())),
                        Row.of(1L, "a"),
                        Row.of(1L, "b")));
    }

    @Test
    void floatingPointNanAndSignedZeroFollowFlinksGeneratedComparator() throws Exception {
        assertDataStreamParity(
                "SELECT metric FROM (SELECT metric, ROW_NUMBER() OVER "
                        + "(ORDER BY metric ASC NULLS LAST) AS row_num FROM topn_float_edges) WHERE row_num <= 5",
                Types.FLOAT,
                DataTypes.FLOAT(),
                List.of(
                        Row.of(Float.NaN),
                        Row.of(-0.0F),
                        Row.of(+0.0F),
                        Row.of(Float.NEGATIVE_INFINITY),
                        Row.of(Float.POSITIVE_INFINITY)),
                "topn_float_edges");
        assertDataStreamParity(
                "SELECT metric FROM (SELECT metric, ROW_NUMBER() OVER "
                        + "(ORDER BY metric DESC NULLS FIRST) AS row_num FROM topn_double_edges) WHERE row_num <= 5",
                Types.DOUBLE,
                DataTypes.DOUBLE(),
                List.of(
                        Row.of(Double.NaN),
                        Row.of(-0.0D),
                        Row.of(+0.0D),
                        Row.of(Double.NEGATIVE_INFINITY),
                        Row.of(Double.POSITIVE_INFINITY)),
                "topn_double_edges");
    }
}

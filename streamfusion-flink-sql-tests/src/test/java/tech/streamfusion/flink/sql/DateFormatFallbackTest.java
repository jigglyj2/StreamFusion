/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class DateFormatFallbackTest extends SqlParityTestSupport {
    @ParameterizedTest
    @MethodSource("cases")
    void unsupportedFormattingExplainsWholePlanFallback(String expression, String reason) {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        var environment = StreamExecutionEnvironment.getExecutionEnvironment();
        var tables = StreamTableEnvironment.create(environment);
        var timestamp = LocalDateTime.parse("2024-03-10T02:30:00");
        tables.createTemporaryView(
                "input",
                tables.fromDataStream(
                        environment
                                .fromData(Row.of(timestamp, timestamp, Instant.EPOCH, "yyyy-MM-dd"))
                                .returns(Types.ROW_NAMED(
                                        new String[] {"ts", "precise", "zoned", "fmt"},
                                        Types.LOCAL_DATE_TIME,
                                        Types.LOCAL_DATE_TIME,
                                        Types.INSTANT,
                                        Types.STRING)),
                        Schema.newBuilder()
                                .column("ts", DataTypes.TIMESTAMP(3))
                                .column("precise", DataTypes.TIMESTAMP(6))
                                .column("zoned", DataTypes.TIMESTAMP_LTZ(3))
                                .column("fmt", DataTypes.STRING())
                                .build()));
        assertThat(tables.explainSql("SELECT " + expression + " FROM input"))
                .contains("Accelerated: no")
                .contains(reason)
                .contains("the entire plan will use Flink");
    }

    private static Stream<Arguments> cases() {
        return Stream.of(
                Arguments.of("DATE_FORMAT(ts, fmt)", "numeric literal patterns"),
                Arguments.of("DATE_FORMAT(ts, 'MMMM')", "numeric literal patterns"),
                Arguments.of("DATE_FORMAT(ts, 'YYYY-ww')", "numeric literal patterns"),
                Arguments.of("DATE_FORMAT(ts, 'yyyy-MM-dd Z')", "numeric literal patterns"),
                Arguments.of("DATE_FORMAT(ts, 'yyyy[MM]')", "numeric literal patterns"),
                Arguments.of("DATE_FORMAT(precise, 'yyyy-MM-dd')", "timezone-free TIMESTAMP(3)"),
                Arguments.of("DATE_FORMAT(zoned, 'yyyy-MM-dd')", "timezone-free TIMESTAMP(3)"),
                Arguments.of("DATE_FORMAT(fmt, 'yyyy-MM-dd')", "timezone-free TIMESTAMP(3)"));
    }
}

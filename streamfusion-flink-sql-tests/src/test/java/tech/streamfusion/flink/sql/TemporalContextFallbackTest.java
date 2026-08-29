/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class TemporalContextFallbackTest extends SqlParityTestSupport {
    @ParameterizedTest(name = "{0}")
    @MethodSource("queries")
    void explainNamesTemporalRuntimeContract(String ignoredName, String sql, String reason) {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        tableEnvironment.createTemporaryView(
                "temporal_input",
                environment
                        .fromData(Row.of(LocalDateTime.parse("2024-03-10T02:30:00")))
                        .returns(Types.ROW(Types.LOCAL_DATE_TIME)));

        assertThat(tableEnvironment.explainSql(sql))
                .contains("Accelerated: no")
                .contains(reason)
                .contains("the entire plan will use Flink");
    }

    private static Stream<Arguments> queries() {
        return Stream.of(
                Arguments.of(
                        "clock lifecycle",
                        "SELECT f0, CURRENT_ROW_TIMESTAMP() FROM temporal_input",
                        "job, row, and session clock lifecycle"),
                Arguments.of(
                        "format and timezone",
                        "SELECT DATE_FORMAT(f0, 'yyyy-MM-dd HH:mm:ss') FROM temporal_input",
                        "Java pattern parsing, locale, session-zone, DST gap/overlap"));
    }
}

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

import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class NondeterministicFunctionFallbackTest {
    @ParameterizedTest
    @MethodSource("functions")
    void executionHistoryDependencyHasAnExplainReason(String expression, String detail) {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        tableEnvironment.createTemporaryView(
                "random_explain_input", environment.fromData(Row.of(1)).returns(Types.ROW(Types.INT)));

        assertThat(tableEnvironment.explainSql("SELECT " + expression + " FROM random_explain_input"))
                .contains("Accelerated: no")
                .contains(detail)
                .contains("the entire plan will use Flink");
    }

    private static Stream<Arguments> functions() {
        return Stream.of(
                Arguments.of("UUID()", "secure random UUID generator"),
                Arguments.of("RAND()", "state advances per row"),
                Arguments.of("RAND(42)", "including seeded calls"),
                Arguments.of("RAND_INTEGER(10)", "state advances per row"),
                Arguments.of("RAND_INTEGER(42, 10)", "including seeded calls"));
    }
}

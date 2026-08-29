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

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class CharsetFunctionFallbackTest {
    @ParameterizedTest
    @ValueSource(strings = {"ENCODE(f0, 'UTF-8')", "DECODE(ENCODE(f0, 'UTF-16LE'), 'UTF-16LE')"})
    void jvmCharsetDependencyHasAnExplainReason(String expression) {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        tableEnvironment.createTemporaryView(
                "charset_explain_input", environment.fromData(Row.of("你好")).returns(Types.ROW(Types.STRING)));

        assertThat(tableEnvironment.explainSql("SELECT " + expression + " FROM charset_explain_input"))
                .contains("Accelerated: no")
                .contains("installed providers")
                .contains("malformed-input replacement")
                .contains("the entire plan will use Flink");
    }
}

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

class RegexFunctionFallbackTest {
    @ParameterizedTest
    @ValueSource(
            strings = {
                "REGEXP(f0, 'a(?=b)')",
                "REGEXP_COUNT(f0, 'a')",
                "REGEXP_EXTRACT(f0, '(a)(b)', 1)",
                "REGEXP_EXTRACT_ALL(f0, '(a)', 1)",
                "REGEXP_INSTR(f0, 'b')",
                "REGEXP_SUBSTR(f0, 'a.')",
                "REGEXP_REPLACE(f0, '(a)', '$1x')",
                "f0 SIMILAR TO 'a%'",
                "f0 NOT SIMILAR TO 'z%'"
            })
    void javaRegexDependencyHasAnExplainReason(String expression) {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        tableEnvironment.createTemporaryView(
                "regex_explain_input", environment.fromData(Row.of("ababa")).returns(Types.ROW(Types.STRING)));

        assertThat(tableEnvironment.explainSql("SELECT " + expression + " FROM regex_explain_input"))
                .contains("Accelerated: no")
                .contains("Java Pattern syntax")
                .contains("look-around and backreferences")
                .contains("the entire plan will use Flink");
    }
}

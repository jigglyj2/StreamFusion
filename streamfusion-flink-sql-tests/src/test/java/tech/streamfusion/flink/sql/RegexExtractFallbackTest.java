/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class RegexExtractFallbackTest extends SqlParityTestSupport {
    @ParameterizedTest
    @ValueSource(
            strings = {
                "REGEXP_EXTRACT(f0, f1, 1)",
                "REGEXP_EXTRACT(f0, '(a)', f2)",
                "REGEXP_EXTRACT(f0, '(a)+', 1)",
                "REGEXP_EXTRACT(f0, '(?i)(a)', 1)",
                "REGEXP_EXTRACT(f0, '(a$)', 1)",
                "REGEXP_EXTRACT(f0, '(.)', 1)",
                "REGEXP_EXTRACT(f0, '(a)(?=b)', 1)",
                "REGEXP_EXTRACT(f0, '([a-z&&[^b]])', 1)",
                "REGEXP_EXTRACT(f0, '(é)', 1)",
                "REGEXP_EXTRACT(CAST(f0 AS CHAR(10)), '(a)', 1)"
            })
    void unverifiedRegexSemanticsRetainWholePlanFallback(String expression) {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        var environment = StreamExecutionEnvironment.getExecutionEnvironment();
        var tables = StreamTableEnvironment.create(environment);
        tables.createTemporaryView(
                "regex_input",
                environment.fromData(Row.of("ab", "(a)", 1)).returns(Types.ROW(Types.STRING, Types.STRING, Types.INT)));
        assertThat(tables.explainSql("SELECT " + expression + " FROM regex_input"))
                .contains("Accelerated: no", "REGEXP_EXTRACT", "stays on Flink", "the entire plan will use Flink");
    }
}

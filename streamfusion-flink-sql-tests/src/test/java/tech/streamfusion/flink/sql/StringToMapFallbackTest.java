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
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class StringToMapFallbackTest {
    @Test
    void regexDelimiterDifferenceHasAnExplainReason() {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        tableEnvironment.createTemporaryView(
                "str_to_map_explain_input",
                environment.fromData(Row.of("k1$$v1|k2$$v2")).returns(Types.ROW(Types.STRING)));

        assertThat(tableEnvironment.explainSql(
                        "SELECT STR_TO_MAP(f0, '\\\\|', '\\\\$\\\\$') " + "FROM str_to_map_explain_input"))
                .contains("Accelerated: no")
                .contains("Java regular expressions")
                .contains("literal delimiter matching")
                .contains("the entire plan will use Flink");
    }
}

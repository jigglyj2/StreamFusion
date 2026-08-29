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

class StructuredObjectFallbackTest extends SqlParityTestSupport {
    @Test
    void objectOfStaysOnFlinkWithClassIdentityReason() {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        tableEnvironment.createTemporaryView(
                "object_input", environment.fromData(Row.of(42, "value")).returns(Types.ROW(Types.INT, Types.STRING)));

        String className = TestObject.class.getName();
        assertThat(tableEnvironment.explainSql(
                        "SELECT OBJECT_OF('" + className + "', 'number', f0, 'text', f1) FROM object_input"))
                .contains("Accelerated: no")
                .contains("classloader identity")
                .contains("object materialization semantics")
                .contains("the entire plan will use Flink");
    }

    /** Public fields define the structured type used by the planner test. */
    public static class TestObject {
        public Integer number;
        public String text;
    }
}

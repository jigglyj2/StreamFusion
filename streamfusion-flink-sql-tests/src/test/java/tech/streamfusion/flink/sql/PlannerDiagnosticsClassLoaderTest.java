/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class PlannerDiagnosticsClassLoaderTest extends SqlParityTestSupport {
    @Test
    void explainUsesThePlannerLoaderWhenApplicationCannotSeeDiagnostics() {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        var environment = StreamExecutionEnvironment.getExecutionEnvironment();
        var tables = StreamTableEnvironment.create(environment);
        tables.createTemporaryView(
                "diagnostic_input", environment.fromData(Row.of("abc123")).returns(Types.ROW(Types.STRING)));
        var thread = Thread.currentThread();
        var original = thread.getContextClassLoader();
        thread.setContextClassLoader(new ClassLoader(original) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (name.equals("tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics"))
                    throw new ClassNotFoundException("Diagnostics belong to the isolated planner loader");
                return super.loadClass(name, resolve);
            }
        });
        try {
            assertThat(tables.explainSql("SELECT f0 FROM diagnostic_input WHERE f0 = 'abc123'"))
                    .contains("Accelerated: yes")
                    .doesNotContain("diagnostics are not installed");
            assertThat(tables.explainSql("SELECT IS_ALPHA(f0) FROM diagnostic_input"))
                    .contains("Accelerated: no", "the entire plan will use Flink", "UTF-16 code units")
                    .doesNotContain("diagnostics are not installed");
        } finally {
            thread.setContextClassLoader(original);
        }
    }
}

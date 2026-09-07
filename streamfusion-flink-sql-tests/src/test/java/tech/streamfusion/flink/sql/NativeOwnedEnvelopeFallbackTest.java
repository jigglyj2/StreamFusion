/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class NativeOwnedEnvelopeFallbackTest extends SqlParityTestSupport {
    @Test
    void sqlNamesCannotMasqueradeAsAnOwnedRecordEnvelope() throws Exception {
        for (String version : List.of("v1", "v99")) {
            String name = "__streamfusion_owned_timestamp_" + version;
            var flink = executeNamed(name, false);
            assertThat(executeNamed(name, true)).isEqualTo(flink);
            assertThat(StreamFusionPlanningDiagnostics.explain())
                    .contains("Accelerated: no", "reserved native owned-envelope namespace");
            assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
        }
    }

    private static byte[] executeNamed(String field, boolean nativePlanner) throws Exception {
        StreamFusionPlannerFactory.resetMetrics();
        if (nativePlanner)
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        else System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        try {
            var environment = StreamExecutionEnvironment.getExecutionEnvironment();
            environment.setParallelism(1);
            var tables = StreamTableEnvironment.create(environment);
            var input = environment.fromCollection(
                    List.of(Row.of(1L), Row.of((Object) null), Row.of(-7L)),
                    Types.ROW_NAMED(new String[] {field}, Types.LONG));
            tables.createTemporaryView("conflict_input", tables.fromDataStream(input));
            return collect(tables.executeSql("SELECT `" + field + "` FROM conflict_input"));
        } finally {
            System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        }
    }
}

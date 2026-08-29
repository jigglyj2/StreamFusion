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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class NumericTruncateFallbackTest {
    @Test
    void narrowIntegerCodegenRestrictionHasAnExplainReason() {
        assertFallback(Byte.valueOf((byte) 12), Types.BYTE, "incompatible assignments");
    }

    @Test
    void floatingPointRestrictionHasAnExplainReason() {
        assertFallback(12.34d, Types.DOUBLE, "non-finite error parity");
    }

    private static <T> void assertFallback(T value, TypeInformation<T> type, String detail) {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        tableEnvironment.createTemporaryView(
                "truncate_explain_input", environment.fromData(Row.of(value)).returns(Types.ROW(type)));

        assertThat(tableEnvironment.explainSql("SELECT TRUNCATE(f0, -1) FROM truncate_explain_input"))
                .contains("Accelerated: no")
                .contains(detail)
                .contains("the entire plan will use Flink");
    }
}

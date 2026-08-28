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

import java.util.Arrays;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class StringAsciiParityTest extends SqlParityTestSupport {
    private static final java.util.List<Row> ROWS =
            Arrays.asList(Row.of("abc"), Row.of("äbc"), Row.of("😀"), Row.of(""), Row.of((Object) null));

    @Test
    void returnsFirstUnicodeCodePoint() throws Exception {
        assertDataStreamParity(
                "SELECT ASCII(metric) FROM string_input", Types.STRING, DataTypes.STRING(), ROWS, "string_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    @Test
    void filtersWithAscii() throws Exception {
        assertDataStreamParity(
                "SELECT metric FROM string_input WHERE ASCII(metric) > 127",
                Types.STRING,
                DataTypes.STRING(),
                ROWS,
                "string_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }
}

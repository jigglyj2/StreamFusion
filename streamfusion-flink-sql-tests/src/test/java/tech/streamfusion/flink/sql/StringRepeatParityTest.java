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

class StringRepeatParityTest extends SqlParityTestSupport {
    @Test
    void generatedNullableUnicodeAndNestedRepeatsStayNative() throws Exception {
        var rows = new java.util.ArrayList<Row>();
        for (int seed = 0; seed < 4; seed++) {
            var random = new java.util.Random(seed);
            for (int index = 0; index < 32; index++) {
                rows.add(Row.of(index % 7 == 0 ? null : (index % 2 == 0 ? "é" : "界").repeat(random.nextInt(129))));
            }
        }
        assertDataStreamParity(
                "SELECT REPEAT(REPEAT(metric, 2), 3), REPEAT(metric, CHAR_LENGTH(metric)), "
                        + "REPEAT(metric, -1), REPEAT(metric, 0) FROM string_input",
                Types.STRING,
                DataTypes.STRING(),
                rows,
                "string_input");
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    @Test
    void repeatsStringsWithLiteralAndComputedCounts() throws Exception {
        assertDataStreamParity(
                "SELECT REPEAT(metric, 2), REPEAT(metric, 0), REPEAT(metric, -1), "
                        + "REPEAT(metric, CHAR_LENGTH(metric)) FROM string_input",
                Types.STRING,
                DataTypes.STRING(),
                Arrays.asList(Row.of("ab"), Row.of("ä"), Row.of(""), Row.of((Object) null)),
                "string_input");

        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }
}

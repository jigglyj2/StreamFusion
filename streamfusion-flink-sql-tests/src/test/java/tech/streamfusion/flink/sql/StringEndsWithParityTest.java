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

class StringEndsWithParityTest extends SqlParityTestSupport {
    private static final java.util.List<Row> ROWS =
            Arrays.asList(Row.of("www.apache.org"), Row.of("in中文"), Row.of("😀"), Row.of(""), Row.of((Object) null));

    @Test
    void projectsLiteralAndComputedSuffixes() throws Exception {
        assertDataStreamParity(
                "SELECT ENDSWITH(metric, 'org'), ENDSWITH(metric, ''), "
                        + "ENDSWITH(metric, LOWER(metric)) FROM string_input",
                Types.STRING,
                DataTypes.STRING(),
                ROWS,
                "string_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    @Test
    void filtersWithEndsWith() throws Exception {
        assertDataStreamParity(
                "SELECT metric FROM string_input WHERE ENDSWITH(metric, 'org')",
                Types.STRING,
                DataTypes.STRING(),
                ROWS,
                "string_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }
}

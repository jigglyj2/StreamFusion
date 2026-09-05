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

class CollectionSplitParityTest extends SqlParityTestSupport {
    @Test
    void splitMatchesFlinkForNonemptyLiteralDelimiter() throws Exception {
        assertDataStreamParity(
                "SELECT SPLIT(metric, ',') FROM string_input",
                Types.STRING,
                DataTypes.STRING(),
                Arrays.asList(
                        Row.of("123,123,23"),
                        Row.of(",123,123"),
                        Row.of(",123,123,"),
                        Row.of(",123,,,123,"),
                        Row.of("12345"),
                        Row.of((Object) null)),
                "string_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    @Test
    void emptyDelimiterFallsBackBecauseFlinkSplitsCharacters() throws Exception {
        assertFallbackDataStreamParity(
                "SELECT SPLIT(metric, '') FROM string_input",
                Types.STRING,
                DataTypes.STRING(),
                Arrays.asList(Row.of("123,ä"), Row.of(""), Row.of((Object) null)),
                "string_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isZero();
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Flink splits into Unicode characters");
    }
}

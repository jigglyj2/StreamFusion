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

class CollectionJoinParityTest extends SqlParityTestSupport {
    @Test
    void arrayJoinMatchesFlinkWithAndWithoutNullReplacement() throws Exception {
        assertDataStreamParity(
                "SELECT ARRAY_JOIN(metric, '+'), ARRAY_JOIN(metric, '+', '<null>'), ARRAY_JOIN(metric, '') FROM array_input",
                Types.OBJECT_ARRAY(Types.STRING),
                DataTypes.ARRAY(DataTypes.STRING()),
                Arrays.asList(
                        Row.of((Object) new String[] {"abv", "bbb", "cb"}),
                        Row.of((Object) new String[] {"a", null, "b"}),
                        Row.of((Object) new String[] {}),
                        Row.of((Object) null)),
                "array_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    @Test
    void dynamicArrayJoinDelimiterFallsBackWithSemanticReason() throws Exception {
        assertFallbackDataStreamParity(
                "SELECT ARRAY_JOIN(metric, metric[1]) FROM array_input",
                Types.OBJECT_ARRAY(Types.STRING),
                DataTypes.ARRAY(DataTypes.STRING()),
                Arrays.asList(
                        Row.of((Object) new String[] {"+", "a", "b"}),
                        Row.of((Object) new String[] {null, "a"}),
                        Row.of((Object) new String[] {}),
                        Row.of((Object) null)),
                "array_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isZero();
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("delimiter is a non-null literal");
    }
}

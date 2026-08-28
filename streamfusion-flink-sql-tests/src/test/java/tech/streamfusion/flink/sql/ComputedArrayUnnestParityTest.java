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

class ComputedArrayUnnestParityTest extends SqlParityTestSupport {
    @Test
    void nativeUnnestEvaluatesArrayConstructorInsideTheSamePlan() throws Exception {
        assertDataStreamParity(
                "SELECT item, ord_idx FROM computed_array_unnest_input "
                        + "CROSS JOIN UNNEST(ARRAY[metric, metric + 1, CAST(NULL AS INT)]) "
                        + "WITH ORDINALITY AS expanded(item, ord_idx)",
                Types.INT,
                DataTypes.INT(),
                Arrays.asList(Row.of(1), Row.of(-2), Row.of((Object) null)),
                "computed_array_unnest_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isEqualTo(1);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void unsupportedComputedArrayExpressionFallsBackWithReason() throws Exception {
        assertDataStreamParity(
                "SELECT item FROM computed_array_fallback_input "
                        + "CROSS JOIN UNNEST(ARRAY[TRIM(metric)]) AS expanded(item)",
                Types.STRING,
                DataTypes.STRING(),
                Arrays.asList(Row.of(" a "), Row.of("b"), Row.of((Object) null)),
                "computed_array_fallback_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isZero();
        assertThat(StreamFusionPlanningDiagnostics.explain())
                .contains("computed ARRAY operand")
                .contains("has an expression that StreamFusion Calc cannot translate exactly")
                .contains("Accelerated: no");
    }

    @Test
    void nativeLeftUnnestEvaluatesNullableArrayFunctionOperand() throws Exception {
        assertDataStreamParity(
                "SELECT item, ord_idx FROM computed_array_function_unnest_input "
                        + "LEFT JOIN UNNEST(ARRAY_REVERSE(metric)) WITH ORDINALITY "
                        + "AS expanded(item, ord_idx) ON TRUE",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                Arrays.asList(
                        Row.of((Object) new Integer[] {1, null, 3}), Row.of((Object) new Integer[] {}), Row.of((Object)
                                null)),
                "computed_array_function_unnest_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isEqualTo(1);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }
}

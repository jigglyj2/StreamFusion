/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class HyperbolicParityTest extends SqlParityTestSupport {
    private static final List<Row> INPUTS = Arrays.asList(
            Row.of(Double.NEGATIVE_INFINITY),
            Row.of(-710.0d),
            Row.of(-1.0d),
            Row.of(-0.0d),
            Row.of(0.0d),
            Row.of(1.0d),
            Row.of(710.0d),
            Row.of(Double.POSITIVE_INFINITY),
            Row.of(Double.NaN),
            Row.of((Object) null));

    @ParameterizedTest
    @ValueSource(strings = {"SINH", "TANH"})
    void nativeHyperbolicFunctionMatchesFlinkByteForByte(String function) throws Exception {
        assertDataStreamParity(
                "SELECT " + function + "(metric) FROM hyperbolic_input", Types.DOUBLE, INPUTS, "hyperbolic_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @Test
    void hyperbolicCosineFallsBackWithSemanticReason() throws Exception {
        assertFallbackDataStreamParity(
                "SELECT COSH(metric) FROM hyperbolic_cosh_input", Types.DOUBLE, INPUTS, "hyperbolic_cosh_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isZero();
        assertThat(tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics.explain())
                .contains("differs from Flink by one ULP");
    }

    @Test
    void nativeHyperbolicFunctionComposesWithComparisonFilter() throws Exception {
        assertDataStreamParity(
                "SELECT metric FROM hyperbolic_filter_input WHERE TANH(metric) > 0",
                Types.DOUBLE,
                INPUTS,
                "hyperbolic_filter_input");

        assertThat(tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics.explain())
                .contains("Accelerated: yes");
        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }
}

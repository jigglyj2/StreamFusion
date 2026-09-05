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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class PowerParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @ValueSource(
            strings = {
                "CAST(0.0 AS DOUBLE)",
                "CAST(1.0 AS DOUBLE)",
                "CAST(2.0 AS DOUBLE)",
                "CAST(3.0 AS DOUBLE)",
                "CAST(1.5 AS DOUBLE)"
            })
    void nativePowerMatchesFlinkByteForByte(String exponent) throws Exception {
        assertDataStreamParity(
                "SELECT POWER(metric, " + exponent + ") FROM power_input",
                Types.DOUBLE,
                Arrays.asList(
                        Row.of(Double.NEGATIVE_INFINITY),
                        Row.of(-2.0d),
                        Row.of(-1.0d),
                        Row.of(-0.0d),
                        Row.of(0.0d),
                        Row.of(Double.MIN_VALUE),
                        Row.of(0.5d),
                        Row.of(1.0d),
                        Row.of(2.0d),
                        Row.of(Double.MAX_VALUE),
                        Row.of(Double.POSITIVE_INFINITY),
                        Row.of(Double.NaN),
                        Row.of((Object) null)),
                "power_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @Test
    void negativePowerFallsBackWithSemanticReason() throws Exception {
        assertFallbackDataStreamParity(
                "SELECT POWER(metric, CAST(-1.0 AS DOUBLE)) FROM negative_power_input",
                Types.DOUBLE,
                Arrays.asList(
                        Row.of(-0.0d), Row.of(0.0d), Row.of(-2.0d), Row.of(2.0d), Row.of(Double.NaN), Row.of((Object)
                                null)),
                "negative_power_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isZero();
        assertThat(tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics.explain())
                .contains("DataFusion errors on zero raised to a negative power");
    }
}

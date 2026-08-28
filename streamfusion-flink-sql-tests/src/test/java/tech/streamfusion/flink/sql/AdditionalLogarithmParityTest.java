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

class AdditionalLogarithmParityTest extends SqlParityTestSupport {
    @Test
    void nativeUnaryLogarithmMatchesFlinkByteForByte() throws Exception {
        assertDataStreamParity(
                "SELECT LOG(metric) FROM additional_logarithm_input",
                Types.DOUBLE,
                logarithmInputs(),
                "additional_logarithm_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @Test
    void binaryLogarithmFallsBackWithSemanticReason() throws Exception {
        assertDataStreamParity(
                "SELECT LOG2(metric) FROM binary_logarithm_input",
                Types.DOUBLE,
                logarithmInputs(),
                "binary_logarithm_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isZero();
        assertThat(tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics.explain())
                .contains("differs from Flink by one ULP");
    }

    @ParameterizedTest
    @ValueSource(strings = {"CAST(2.0 AS DOUBLE)", "CAST(10.0 AS DOUBLE)", "CAST(1.0 AS DOUBLE)"})
    void nativeArbitraryBaseLogarithmMatchesFlinkByteForByte(String base) throws Exception {
        assertDataStreamParity(
                "SELECT LOG(" + base + ", metric) FROM arbitrary_logarithm_input",
                Types.DOUBLE,
                logarithmInputs(),
                "arbitrary_logarithm_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static List<Row> logarithmInputs() {
        return Arrays.asList(
                Row.of(Double.NEGATIVE_INFINITY),
                Row.of(-1.0d),
                Row.of(-0.0d),
                Row.of(0.0d),
                Row.of(Double.MIN_VALUE),
                Row.of(0.5d),
                Row.of(1.0d),
                Row.of(10.0d),
                Row.of(Double.MAX_VALUE),
                Row.of(Double.POSITIVE_INFINITY),
                Row.of(Double.NaN),
                Row.of((Object) null));
    }
}

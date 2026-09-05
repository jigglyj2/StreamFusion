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
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class RoundParityTest extends SqlParityTestSupport {
    @Test
    void floatingRoundFallsBackWithSemanticReason() throws Exception {
        assertFallbackDataStreamParity(
                "SELECT ROUND(metric, 2) FROM round_input",
                Types.DOUBLE,
                Arrays.asList(
                        Row.of(-125.315d),
                        Row.of(-2.5d),
                        Row.of(-1.5d),
                        Row.of(-0.0d),
                        Row.of(0.0d),
                        Row.of(1.4d),
                        Row.of(1.5d),
                        Row.of(2.5d),
                        Row.of(125.315d),
                        Row.of((Object) null)),
                "round_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isZero();
        assertThat(tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics.explain())
                .contains("data-dependent error semantics for non-finite values");
    }
}

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

class CharacterFromCodeParityTest extends SqlParityTestSupport {
    private static final java.util.List<Row> INPUTS = Arrays.asList(
            Row.of(Long.MIN_VALUE),
            Row.of(-1L),
            Row.of(0L),
            Row.of(1L),
            Row.of(65L),
            Row.of(127L),
            Row.of(128L),
            Row.of(255L),
            Row.of(256L),
            Row.of(321L),
            Row.of(Long.MAX_VALUE),
            Row.of((Object) null));

    @Test
    void nativeChrProjectionMatchesFlinkLowByteSemantics() throws Exception {
        assertDataStreamParity(
                "SELECT CHR(metric) FROM chr_projection_input", Types.LONG, INPUTS, "chr_projection_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @Test
    void nativeChrFilterMatchesFlink() throws Exception {
        assertDataStreamParity(
                "SELECT metric FROM chr_filter_input WHERE CHR(metric) = CHR(65)",
                Types.LONG,
                INPUTS,
                "chr_filter_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }
}

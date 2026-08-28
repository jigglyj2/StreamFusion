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

class StringReverseParityTest extends SqlParityTestSupport {
    private static final java.util.List<Row> INPUTS = Arrays.asList(
            Row.of(""),
            Row.of("abc"),
            Row.of("你好"),
            Row.of("😀x"),
            Row.of("e\u0301x"),
            Row.of("a\u0000b"),
            Row.of((Object) null));

    @Test
    void nativeReverseProjectionMatchesFlinkCharacters() throws Exception {
        assertDataStreamParity(
                "SELECT REVERSE(metric) FROM reverse_projection_input",
                Types.STRING,
                INPUTS,
                "reverse_projection_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @Test
    void nativeReverseFilterMatchesFlink() throws Exception {
        assertDataStreamParity(
                "SELECT metric FROM reverse_filter_input WHERE REVERSE(metric) = 'cba'",
                Types.STRING,
                INPUTS,
                "reverse_filter_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }
}

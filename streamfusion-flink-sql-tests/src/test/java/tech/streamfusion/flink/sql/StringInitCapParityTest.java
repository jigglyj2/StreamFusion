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

class StringInitCapParityTest extends SqlParityTestSupport {
    private static final java.util.List<Row> INPUTS = Arrays.asList(
            Row.of(""),
            Row.of("hello WORLD"),
            Row.of("42SQL_data"),
            Row.of("éFLINK rocks"),
            Row.of("你好FLINK"),
            Row.of("a-b.c"),
            Row.of((Object) null));

    @Test
    void nativeInitCapProjectionMatchesFlinkAsciiWordRules() throws Exception {
        assertDataStreamParity(
                "SELECT INITCAP(metric) FROM init_cap_projection_input",
                Types.STRING,
                INPUTS,
                "init_cap_projection_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @Test
    void nativeInitCapFilterMatchesFlink() throws Exception {
        assertDataStreamParity(
                "SELECT metric FROM init_cap_filter_input WHERE INITCAP(metric) = 'Hello World'",
                Types.STRING,
                INPUTS,
                "init_cap_filter_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }
}

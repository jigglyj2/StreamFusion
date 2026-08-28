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

class HexadecimalParityTest extends SqlParityTestSupport {
    @Test
    void nativeIntegerHexadecimalMatchesFlinkByteForByte() throws Exception {
        assertDataStreamParity(
                "SELECT HEX(metric) FROM integer_hex_input",
                Types.LONG,
                Arrays.asList(
                        Row.of(Long.MIN_VALUE),
                        Row.of(-255L),
                        Row.of(-1L),
                        Row.of(0L),
                        Row.of(10L),
                        Row.of(255L),
                        Row.of(Long.MAX_VALUE),
                        Row.of((Object) null)),
                "integer_hex_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @Test
    void nativeStringHexadecimalMatchesFlinkUtf8Bytes() throws Exception {
        assertDataStreamParity(
                "SELECT HEX(metric) FROM string_hex_input",
                Types.STRING,
                Arrays.asList(
                        Row.of(""),
                        Row.of("Flink"),
                        Row.of("ö"),
                        Row.of("你好"),
                        Row.of("😀"),
                        Row.of("a\u0000b"),
                        Row.of((Object) null)),
                "string_hex_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }
}

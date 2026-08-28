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

class Md5ParityTest extends SqlParityTestSupport {
    @Test
    void nativeStringMd5MatchesFlinkUtf8Bytes() throws Exception {
        assertDataStreamParity(
                "SELECT MD5(metric) FROM string_md5_input",
                Types.STRING,
                Arrays.asList(Row.of(""), Row.of("abc"), Row.of("你好😀"), Row.of("a\u0000b"), Row.of((Object) null)),
                "string_md5_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @Test
    void nativeBinaryMd5MatchesFlinkBytes() throws Exception {
        assertDataStreamParity(
                "SELECT MD5(metric) FROM binary_md5_input",
                Types.PRIMITIVE_ARRAY(Types.BYTE),
                Arrays.asList(Row.of(new byte[0]), Row.of(new byte[] {0, 1, 2, 127, -128, -1}), Row.of((Object) null)),
                "binary_md5_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }
}

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

class Sha1ParityTest extends SqlParityTestSupport {
    @Test
    void nativeSha1ProjectionMatchesFlinkUtf8Bytes() throws Exception {
        assertDataStreamParity(
                "SELECT SHA1(metric) FROM sha1_projection_input",
                Types.STRING,
                Arrays.asList(Row.of(""), Row.of("abc"), Row.of("你好😀"), Row.of("a\u0000b"), Row.of((Object) null)),
                "sha1_projection_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @Test
    void nativeSha1FilterMatchesFlink() throws Exception {
        assertDataStreamParity(
                "SELECT metric FROM sha1_filter_input WHERE SHA1(metric) = SHA1('abc')",
                Types.STRING,
                Arrays.asList(Row.of("abc"), Row.of("def"), Row.of("你好"), Row.of((Object) null)),
                "sha1_filter_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }
}

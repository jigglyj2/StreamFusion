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
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class ShaDigestParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @MethodSource("fixedShaFunctions")
    void nativeFixedShaProjectionMatchesFlinkUtf8Bytes(String function) throws Exception {
        assertDataStreamParity(
                "SELECT " + function + "(metric) FROM sha_projection_input",
                Types.STRING,
                Arrays.asList(Row.of(""), Row.of("abc"), Row.of("你好😀"), Row.of("a\u0000b"), Row.of((Object) null)),
                "sha_projection_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @ParameterizedTest
    @MethodSource("fixedShaFunctions")
    void nativeFixedShaFilterMatchesFlink(String function) throws Exception {
        assertDataStreamParity(
                "SELECT metric FROM sha_filter_input WHERE " + function + "(metric) = " + function + "('abc')",
                Types.STRING,
                Arrays.asList(Row.of("abc"), Row.of("def"), Row.of("你好"), Row.of((Object) null)),
                "sha_filter_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<String> fixedShaFunctions() {
        return Stream.of("SHA224", "SHA256", "SHA384", "SHA512");
    }
}

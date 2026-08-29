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

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class ComposedCastParityTest extends SqlParityTestSupport {
    @ParameterizedTest(name = "{0}")
    @MethodSource("composedCastCases")
    void composedNumericCastsMatchFlinkByteForByte(String ignoredName, String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @Test
    void narrowingComputedIntegersRetainFlinkWrappingSemantics() throws Exception {
        assertParity(
                "SELECT CAST(metric + 256 AS TINYINT) FROM "
                        + "(VALUES (-129), (-128), (-1), (0), (127), (128), (CAST(NULL AS INT))) input(metric)",
                true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> composedCastCases() {
        return Stream.of(
                Arguments.of(
                        "computed INT to BIGINT",
                        "SELECT CAST(metric + 1 AS BIGINT) FROM "
                                + "(VALUES (-2147483648), (-1), (0), (2147483646), (CAST(NULL AS INT))) input(metric)"),
                Arguments.of(
                        "computed INT to DOUBLE",
                        "SELECT CAST(metric * 3 AS DOUBLE) FROM "
                                + "(VALUES (-10000001), (-1), (0), (10000001), (CAST(NULL AS INT))) input(metric)"),
                Arguments.of(
                        "computed FLOAT to DOUBLE",
                        "SELECT CAST(metric + CAST(0.5 AS FLOAT) AS DOUBLE) FROM "
                                + "(VALUES (CAST(-3.25 AS FLOAT)), (CAST(-0.0 AS FLOAT)), "
                                + "(CAST(3.25 AS FLOAT)), (CAST(NULL AS FLOAT))) input(metric)"));
    }
}

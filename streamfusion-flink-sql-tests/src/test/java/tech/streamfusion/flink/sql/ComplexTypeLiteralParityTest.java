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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class ComplexTypeLiteralParityTest extends SqlParityTestSupport {
    @ParameterizedTest(name = "{0} typed null")
    @MethodSource("complexNullCases")
    void nativeComplexTypedNullsMatchFlinkByteForByte(String ignoredName, String type) throws Exception {
        String sql = "SELECT CAST(NULL AS " + type + ") FROM (VALUES (1), (2)) AS input(id) WHERE id >= 1";

        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> complexNullCases() {
        return Stream.of(
                Arguments.of("array", "ARRAY<INT>"),
                Arguments.of("nested-array", "ARRAY<ARRAY<STRING>>"),
                Arguments.of("map", "MAP<STRING, INT>"),
                Arguments.of("row", "ROW<label STRING, amount INT>"),
                Arguments.of("nested-row", "ROW<label STRING, entries ARRAY<INT>>"));
    }
}

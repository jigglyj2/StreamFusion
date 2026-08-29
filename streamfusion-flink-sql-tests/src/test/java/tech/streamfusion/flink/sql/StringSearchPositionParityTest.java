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
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class StringSearchPositionParityTest extends SqlParityTestSupport {
    private static final String INPUT = "(VALUES "
            + "('streamfusion', 'fusion'), ('banana', 'na'), ('😀文😀', '文'), "
            + "('', ''), ('abc', ''), ('abc', 'z'), "
            + "(CAST(NULL AS STRING), 'a'), ('abc', CAST(NULL AS STRING))) "
            + "input(text_value, search_value)";

    @ParameterizedTest
    @MethodSource("queries")
    void twoArgumentSearchesMatchFlinkByteForByte(String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    private static Stream<String> queries() {
        return Stream.of(
                "SELECT INSTR(text_value, search_value), LOCATE(search_value, text_value) FROM " + INPUT,
                "SELECT text_value FROM " + INPUT + " WHERE INSTR(text_value, search_value) > 1",
                "SELECT LOCATE(UPPER(search_value), UPPER(text_value)) FROM " + INPUT);
    }
}

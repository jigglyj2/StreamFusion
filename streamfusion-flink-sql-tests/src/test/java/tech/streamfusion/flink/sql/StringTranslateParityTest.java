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

class StringTranslateParityTest extends SqlParityTestSupport {
    private static final String INPUT = "(VALUES "
            + "('abcabc', 'abc', 'xy'), ('banana', 'an', '12'), "
            + "('😀a😀', '😀a', '界'), ('duplicate', 'pp', 'xy'), "
            + "('', 'abc', 'xyz'), ('unchanged', '', 'xyz'), "
            + "('delete', 'el', CAST(NULL AS STRING)), "
            + "(CAST(NULL AS STRING), 'abc', 'xyz'), "
            + "('value', CAST(NULL AS STRING), 'xyz')) "
            + "input(text_value, source_characters, target_characters)";

    @ParameterizedTest
    @MethodSource("queries")
    void dynamicAlphabetsMatchFlinkByteForByte(String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    private static Stream<String> queries() {
        return Stream.of(
                "SELECT TRANSLATE(text_value, source_characters, target_characters) FROM " + INPUT,
                "SELECT text_value FROM " + INPUT
                        + " WHERE TRANSLATE(text_value, source_characters, target_characters) = 'xyxy'",
                "SELECT TRANSLATE(UPPER(text_value), source_characters, target_characters) FROM " + INPUT);
    }
}

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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class StringTrimParityTest extends SqlParityTestSupport {
    private static final String DYNAMIC_INPUT = "(VALUES "
            + "('xyxstreamxy', 'xy'), ('  padded  ', ' '), ('😀text😀', '😀'), "
            + "('', 'x'), (CAST(NULL AS STRING), 'x'), ('x', CAST(NULL AS STRING))) "
            + "input(text_value, trim_characters)";

    @ParameterizedTest(name = "default {0}")
    @MethodSource("defaultTrimCases")
    void defaultSpaceTrimMatchesFlinkByteForByte(String ignoredName, String expression) throws Exception {
        assertDataStreamParity(
                "SELECT " + expression + " FROM trim_input",
                Types.STRING,
                Arrays.asList(
                        Row.of(""),
                        Row.of("   "),
                        Row.of("  padded  "),
                        Row.of("\tpadded\t"),
                        Row.of("\u00a0padded\u00a0"),
                        Row.of("😀 text 😀"),
                        Row.of((Object) null)),
                "trim_input");

        assertNativeCalcRan();
    }

    @ParameterizedTest(name = "dynamic {0}")
    @MethodSource("dynamicTrimCases")
    void dynamicTrimCharactersMatchFlinkByteForByte(String ignoredName, String expression) throws Exception {
        // These three TRIM fixtures fold their two null cases to the same shared Calc.
        // LTRIM/RTRIM/BTRIM retain distinct branches. Runtime arguments below cover all six natively.
        String sql = "SELECT " + expression + " FROM " + DYNAMIC_INPUT;
        if (expression.startsWith("TRIM(")) {
            assertFallbackParity(sql, true);
            SqlFallbackAssertions.nativeBatchesAreZero(StreamFusionPlannerFactory.nativePlanBatchCount());
        } else {
            assertParity(sql, true);
            SqlArchitectureAssertions.nativeBatchesAtLeast(StreamFusionPlannerFactory.nativePlanBatchCount(), 1);
        }
    }

    @ParameterizedTest(name = "runtime dynamic {0}")
    @MethodSource("dynamicTrimCases")
    void dynamicTrimExecutesNativelyWithoutConstantFolding(String ignoredName, String expression) throws Exception {
        assertDataStreamParity(
                "SELECT "
                        + expression
                                .replace("trim_characters", "metric.trim_characters")
                                .replace("text_value", "metric.text_value")
                        + " FROM dynamic_trim_input",
                Types.ROW_NAMED(new String[] {"text_value", "trim_characters"}, Types.STRING, Types.STRING),
                DataTypes.ROW(
                        DataTypes.FIELD("text_value", DataTypes.STRING()),
                        DataTypes.FIELD("trim_characters", DataTypes.STRING())),
                Arrays.asList(
                        Row.of(Row.of("xyxstreamxy", "xy")),
                        Row.of(Row.of("  padded  ", " ")),
                        Row.of(Row.of("😀text😀", "😀")),
                        Row.of(Row.of("", "x")),
                        Row.of(Row.of(null, "x")),
                        Row.of(Row.of("x", null)),
                        Row.of((Object) null)),
                "dynamic_trim_input");
        assertNativeCalcRan();
    }

    private static Stream<Arguments> defaultTrimCases() {
        return Stream.of(
                Arguments.of("both", "TRIM(metric)"),
                Arguments.of("btrim", "BTRIM(metric)"),
                Arguments.of("leading", "LTRIM(metric)"),
                Arguments.of("trailing", "RTRIM(metric)"));
    }

    private static Stream<Arguments> dynamicTrimCases() {
        return Stream.of(
                Arguments.of("both", "TRIM(BOTH trim_characters FROM text_value)"),
                Arguments.of("leading", "TRIM(LEADING trim_characters FROM text_value)"),
                Arguments.of("trailing", "TRIM(TRAILING trim_characters FROM text_value)"),
                Arguments.of("ltrim", "LTRIM(text_value, trim_characters)"),
                Arguments.of("rtrim", "RTRIM(text_value, trim_characters)"),
                Arguments.of("btrim", "BTRIM(text_value, trim_characters)"));
    }

    private static void assertNativeCalcRan() {
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }
}

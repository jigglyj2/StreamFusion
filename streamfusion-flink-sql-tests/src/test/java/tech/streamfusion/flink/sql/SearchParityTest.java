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
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class SearchParityTest extends SqlParityTestSupport {
    @ParameterizedTest(name = "fixed-width search {0}")
    @MethodSource("nativeFixedWidthSearchCases")
    void nativeFixedWidthSearchMatchesFlinkByteForByte(
            String ignoredName,
            TypeInformation<?> type,
            DataType logicalType,
            List<Row> rows,
            String tableName,
            String predicate)
            throws Exception {
        assertDataStreamParity(
                "SELECT metric FROM " + tableName + " WHERE " + predicate, type, logicalType, rows, tableName);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> nativeFixedWidthSearchCases() {
        return Stream.of(
                Arguments.of(
                        "CHAR BETWEEN",
                        Types.STRING,
                        DataTypes.CHAR(3),
                        Arrays.asList(Row.of("a  "), Row.of("b  "), Row.of("é  "), Row.of((Object) null)),
                        "fixed_char_input",
                        "metric BETWEEN 'a  ' AND 'é  '"),
                Arguments.of(
                        "CHAR IN",
                        Types.STRING,
                        DataTypes.CHAR(3),
                        Arrays.asList(Row.of("a  "), Row.of("b  "), Row.of("é  "), Row.of((Object) null)),
                        "fixed_char_input",
                        "metric IN ('a  ', 'é  ')"),
                Arguments.of(
                        "BINARY BETWEEN",
                        Types.PRIMITIVE_ARRAY(Types.BYTE),
                        DataTypes.BINARY(2),
                        fixedBinaryRows(),
                        "fixed_binary_input",
                        "metric BETWEEN X'0000' AND X'80FF'"));
    }

    private static List<Row> fixedBinaryRows() {
        return Arrays.asList(
                Row.of(new byte[] {0x00, 0x00}),
                Row.of(new byte[] {0x01, 0x02}),
                Row.of(new byte[] {(byte) 0x80, (byte) 0xff}),
                Row.of((Object) null));
    }

    @Test
    void fixedBinaryPointSearchFallsBackToFlink() throws Exception {
        assertFallbackDataStreamParity(
                "SELECT metric FROM fixed_binary_fallback_input WHERE metric IN (X'0000', X'80FF')",
                Types.PRIMITIVE_ARRAY(Types.BYTE),
                DataTypes.BINARY(2),
                fixedBinaryRows(),
                "fixed_binary_fallback_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isZero();
    }

    @ParameterizedTest(name = "null-containing IN {0}")
    @ValueSource(
            strings = {
                "metric IN (1, 3, NULL)",
                "metric IN (1, 3, NULL) AND metric > 0",
                "metric IN (1, 3, NULL) OR metric = 2"
            })
    void nullContainingInSearchMatchesFlinkByteForByte(String predicate) throws Exception {
        assertIntegerDataStreamParity("SELECT metric FROM integer_input WHERE " + predicate);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @ParameterizedTest(name = "null-aware search {0}")
    @ValueSource(
            strings = {
                "metric NOT IN (1, 3, NULL)",
                "NOT (metric IN (1, 3, NULL))",
                "(metric IN (1, 3, NULL)) IS NOT FALSE"
            })
    void nullAwareNegatedOrTruthTestSearchMatchesFlinkByteForByte(String predicate) throws Exception {
        assertIntegerDataStreamParity("SELECT metric FROM integer_input WHERE " + predicate);
    }

    @ParameterizedTest(name = "VARBINARY {0}")
    @MethodSource("nativeVarbinaryDataStreamRangeCases")
    void nativeVarbinaryDataStreamRangesMatchFlinkByteForByte(String ignoredName, String predicate) throws Exception {
        assertDataStreamParity(
                "SELECT metric FROM varbinary_input WHERE " + predicate,
                Types.PRIMITIVE_ARRAY(Types.BYTE),
                Arrays.asList(
                        Row.of(new byte[] {0x00}),
                        Row.of(new byte[] {0x01, 0x02}),
                        Row.of(new byte[] {0x03}),
                        Row.of(new byte[] {(byte) 0x80}),
                        Row.of((Object) null)),
                "varbinary_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> nativeVarbinaryDataStreamRangeCases() {
        return Stream.of(Arguments.of("between", "metric BETWEEN X'0102' AND X'03'"));
    }

    @Test
    void varbinaryPointSearchFallsBackAndMatchesFlinkByteForByte() throws Exception {
        assertFallbackDataStreamParity(
                "SELECT metric FROM varbinary_input WHERE metric IN (X'00', X'03', X'80')",
                Types.PRIMITIVE_ARRAY(Types.BYTE),
                Arrays.asList(Row.of(new byte[] {0x00}), Row.of(new byte[] {0x03}), Row.of(new byte[] {(byte) 0x80})),
                "varbinary_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isZero();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nativeBooleanColumnCases")
    void nativeBooleanColumnsMatchFlinkByteForByte(String ignoredName, String predicate) throws Exception {
        String sql =
                "SELECT id FROM (VALUES (1, TRUE), (2, FALSE), (3, TRUE)) AS input(id, enabled) WHERE " + predicate;
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> nativeBooleanColumnCases() {
        return Stream.of(
                Arguments.of("boolean-column", "enabled"),
                Arguments.of("negated-boolean-column", "NOT enabled"),
                Arguments.of("boolean-column-and-comparison", "enabled AND id >= 2"),
                Arguments.of("equal-true", "enabled = TRUE"),
                Arguments.of("true-equal", "TRUE = enabled"),
                Arguments.of("not-equal-false", "enabled <> FALSE"),
                Arguments.of("false-not-equal", "FALSE <> enabled"));
    }
}

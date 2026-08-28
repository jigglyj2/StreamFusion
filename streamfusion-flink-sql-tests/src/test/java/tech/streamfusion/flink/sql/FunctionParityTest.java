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
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class FunctionParityTest extends SqlParityTestSupport {
    @ParameterizedTest(name = "COALESCE {0}")
    @MethodSource("nativeCoalesceDataStreamCases")
    void nativeCoalesceMatchesFlinkByteForByte(
            String ignoredName, String sql, TypeInformation<?> type, List<Row> rows, String tableName)
            throws Exception {
        assertDataStreamParity(sql, type, rows, tableName);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> nativeCoalesceDataStreamCases() {
        return Stream.of(
                Arguments.of(
                        "integer",
                        "SELECT COALESCE(metric, 99) FROM coalesce_int_input",
                        Types.INT,
                        Arrays.asList(Row.of(1), Row.of((Object) null), Row.of(3)),
                        "coalesce_int_input"),
                Arguments.of(
                        "nested-null-integer",
                        "SELECT COALESCE(CAST(NULL AS INT), metric, 99) FROM coalesce_int_input",
                        Types.INT,
                        Arrays.asList(Row.of(1), Row.of((Object) null), Row.of(3)),
                        "coalesce_int_input"),
                Arguments.of(
                        "varchar",
                        "SELECT COALESCE(metric, 'fallback') FROM coalesce_string_input",
                        Types.STRING,
                        Arrays.asList(Row.of("alpha"), Row.of((Object) null), Row.of("zeta")),
                        "coalesce_string_input"));
    }

    @ParameterizedTest(name = "conditional {0}")
    @MethodSource("nativeConditionalDataStreamCases")
    void nativeConditionalProjectionsMatchFlinkByteForByte(String ignoredName, String expression) throws Exception {
        assertDataStreamParity(
                "SELECT " + expression + " FROM conditional_input",
                Types.INT,
                Arrays.asList(Row.of(-3), Row.of(0), Row.of(1), Row.of(2), Row.of((Object) null)),
                "conditional_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> nativeConditionalDataStreamCases() {
        return Stream.of(
                Arguments.of(
                        "searched-case",
                        "CASE WHEN metric IS NULL THEN 99 WHEN metric < 0 THEN -metric ELSE metric + 10 END"),
                Arguments.of("simple-case", "CASE metric WHEN 1 THEN 10 WHEN 2 THEN 20 ELSE 30 END"),
                Arguments.of("if", "IF(metric IS NULL, 99, metric + 1)"));
    }

    @ParameterizedTest(name = "ABS {0}")
    @MethodSource("nativeAbsoluteValueDataStreamCases")
    void nativeAbsoluteValueMatchesFlinkByteForByte(
            String ignoredName, TypeInformation<?> type, List<Row> rows, String tableName) throws Exception {
        assertDataStreamParity("SELECT ABS(metric) FROM " + tableName, type, rows, tableName);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> nativeAbsoluteValueDataStreamCases() {
        return Stream.of(
                Arguments.of(
                        "tinyint",
                        Types.BYTE,
                        Arrays.asList(
                                Row.of(Byte.MIN_VALUE),
                                Row.of((byte) -7),
                                Row.of((byte) 0),
                                Row.of((byte) 7),
                                Row.of((Object) null)),
                        "abs_tinyint_input"),
                Arguments.of(
                        "smallint",
                        Types.SHORT,
                        Arrays.asList(
                                Row.of(Short.MIN_VALUE),
                                Row.of((short) -7),
                                Row.of((short) 0),
                                Row.of((short) 7),
                                Row.of((Object) null)),
                        "abs_smallint_input"),
                Arguments.of(
                        "integer",
                        Types.INT,
                        Arrays.asList(
                                Row.of(Integer.MIN_VALUE), Row.of(-7), Row.of(0), Row.of(7), Row.of((Object) null)),
                        "abs_int_input"),
                Arguments.of(
                        "bigint",
                        Types.LONG,
                        Arrays.asList(
                                Row.of(Long.MIN_VALUE), Row.of(-7L), Row.of(0L), Row.of(7L), Row.of((Object) null)),
                        "abs_bigint_input"),
                Arguments.of(
                        "float",
                        Types.FLOAT,
                        Arrays.asList(
                                Row.of(Float.NEGATIVE_INFINITY),
                                Row.of(-7.5f),
                                Row.of(-0.0f),
                                Row.of(0.0f),
                                Row.of(Float.NaN),
                                Row.of((Object) null)),
                        "abs_float_input"),
                Arguments.of(
                        "double",
                        Types.DOUBLE,
                        Arrays.asList(
                                Row.of(Double.NEGATIVE_INFINITY),
                                Row.of(-7.5d),
                                Row.of(-0.0d),
                                Row.of(0.0d),
                                Row.of(Double.NaN),
                                Row.of((Object) null)),
                        "abs_double_input"),
                Arguments.of(
                        "decimal",
                        Types.BIG_DEC,
                        Arrays.asList(
                                Row.of(new java.math.BigDecimal("-12345678901234567890.123456789012345678")),
                                Row.of(new java.math.BigDecimal("-0.000000000000000000")),
                                Row.of(new java.math.BigDecimal("7.500000000000000000")),
                                Row.of((Object) null)),
                        "abs_decimal_input"));
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("nativeRoundingDataStreamCases")
    void nativeRoundingMatchesFlinkByteForByte(
            String function, String ignoredType, TypeInformation<?> type, List<Row> rows, String tableName)
            throws Exception {
        assertDataStreamParity("SELECT " + function + "(metric) FROM " + tableName, type, rows, tableName);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> nativeRoundingDataStreamCases() {
        return Stream.of("CEIL", "FLOOR")
                .flatMap(function -> Stream.of(
                        Arguments.of(
                                function,
                                "tinyint",
                                Types.BYTE,
                                Arrays.asList(
                                        Row.of(Byte.MIN_VALUE),
                                        Row.of((byte) 0),
                                        Row.of(Byte.MAX_VALUE),
                                        Row.of((Object) null)),
                                function.toLowerCase() + "_tinyint_input"),
                        Arguments.of(
                                function,
                                "smallint",
                                Types.SHORT,
                                Arrays.asList(
                                        Row.of(Short.MIN_VALUE),
                                        Row.of((short) 0),
                                        Row.of(Short.MAX_VALUE),
                                        Row.of((Object) null)),
                                function.toLowerCase() + "_smallint_input"),
                        Arguments.of(
                                function,
                                "integer",
                                Types.INT,
                                Arrays.asList(
                                        Row.of(Integer.MIN_VALUE), Row.of(0), Row.of(Integer.MAX_VALUE), Row.of((Object)
                                                null)),
                                function.toLowerCase() + "_integer_input"),
                        Arguments.of(
                                function,
                                "bigint",
                                Types.LONG,
                                Arrays.asList(
                                        Row.of(Long.MIN_VALUE), Row.of(0L), Row.of(Long.MAX_VALUE), Row.of((Object)
                                                null)),
                                function.toLowerCase() + "_bigint_input"),
                        Arguments.of(
                                function,
                                "float",
                                Types.FLOAT,
                                Arrays.asList(
                                        Row.of(Float.NEGATIVE_INFINITY),
                                        Row.of(-7.5f),
                                        Row.of(-0.0f),
                                        Row.of(7.5f),
                                        Row.of(Float.POSITIVE_INFINITY),
                                        Row.of(Float.NaN),
                                        Row.of((Object) null)),
                                function.toLowerCase() + "_float_input"),
                        Arguments.of(
                                function,
                                "double",
                                Types.DOUBLE,
                                Arrays.asList(
                                        Row.of(Double.NEGATIVE_INFINITY),
                                        Row.of(-7.5d),
                                        Row.of(-0.0d),
                                        Row.of(7.5d),
                                        Row.of(Double.POSITIVE_INFINITY),
                                        Row.of(Double.NaN),
                                        Row.of((Object) null)),
                                function.toLowerCase() + "_double_input")));
    }

    @ParameterizedTest(name = "SIGN {0}")
    @MethodSource("nativeSignDataStreamCases")
    void nativeSignMatchesFlinkByteForByte(
            String ignoredName, TypeInformation<?> type, List<Row> rows, String tableName) throws Exception {
        assertDataStreamParity("SELECT SIGN(metric) FROM " + tableName, type, rows, tableName);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> nativeSignDataStreamCases() {
        return nativeAbsoluteValueDataStreamCases()
                .filter(arguments -> !"tinyint".equals(arguments.get()[0])
                        && !"smallint".equals(arguments.get()[0]));
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"CHAR_LENGTH", "CHARACTER_LENGTH"})
    void nativeCharacterLengthMatchesFlinkByteForByte(String function) throws Exception {
        assertDataStreamParity(
                "SELECT " + function + "(metric) FROM character_length_input",
                Types.STRING,
                Arrays.asList(
                        Row.of(""),
                        Row.of("StreamFusion"),
                        Row.of("你好"),
                        Row.of("😀"),
                        Row.of("e\u0301"),
                        Row.of("a\u0000b"),
                        Row.of((Object) null)),
                "character_length_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"LOWER", "UPPER"})
    void nativeCaseMappingMatchesFlinkByteForByte(String function) throws Exception {
        assertDataStreamParity(
                "SELECT " + function + "(metric) FROM case_mapping_input",
                Types.STRING,
                Arrays.asList(
                        Row.of(""),
                        Row.of("StreamFusion 123"),
                        Row.of("Straße"),
                        Row.of("İı"),
                        Row.of("ΟΣ Σσς"),
                        Row.of("ﬃ"),
                        Row.of("你好😀"),
                        Row.of("e\u0301"),
                        Row.of((Object) null)),
                "case_mapping_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @ParameterizedTest(name = "CONCAT {0}")
    @MethodSource("nativeConcatCases")
    void nativeConcatMatchesFlinkByteForByte(String ignoredName, String expression) throws Exception {
        assertDataStreamParity(
                "SELECT " + expression + " FROM concat_input",
                Types.STRING,
                Arrays.asList(Row.of(""), Row.of("AbC"), Row.of("你好😀"), Row.of((Object) null)),
                "concat_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> nativeConcatCases() {
        return Stream.of(
                Arguments.of("binary", "CONCAT(metric, '/', metric)"),
                Arguments.of("variadic", "CONCAT('[', metric, ']', '')"),
                Arguments.of("null argument", "CONCAT(metric, CAST(NULL AS STRING))"),
                Arguments.of("nested", "CONCAT(LOWER(metric), UPPER(metric))"));
    }

    @ParameterizedTest(name = "recursive filter {0}")
    @MethodSource("nativeRecursiveStringFilterCases")
    void nativeRecursiveStringFiltersMatchFlinkByteForByte(String ignoredName, String predicate) throws Exception {
        assertDataStreamParity(
                "SELECT metric FROM recursive_string_filter_input WHERE " + predicate,
                Types.STRING,
                Arrays.asList(
                        Row.of("Alpha"), Row.of("ALPINE"), Row.of("beta"), Row.of("你好世界"), Row.of(""), Row.of((Object)
                                null)),
                "recursive_string_filter_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> nativeRecursiveStringFilterCases() {
        return Stream.of(
                Arguments.of("function comparison", "LOWER(metric) = 'alpha'"),
                Arguments.of("numeric result comparison", "CHAR_LENGTH(metric) > 4"),
                Arguments.of("nested LIKE", "CONCAT(LOWER(metric), '!') LIKE 'alp%'"),
                Arguments.of("nested null check", "LOWER(metric) IS NOT NULL"));
    }

    @Test
    void nativeRecursiveNumericFilterMatchesFlinkByteForByte() throws Exception {
        assertDataStreamParity(
                "SELECT metric FROM recursive_numeric_filter_input WHERE ABS(metric + 1) BETWEEN 2 AND 5",
                Types.INT,
                Arrays.asList(
                        Row.of(Integer.MIN_VALUE),
                        Row.of(-6),
                        Row.of(-3),
                        Row.of(-1),
                        Row.of(1),
                        Row.of(4),
                        Row.of(5),
                        Row.of((Object) null)),
                "recursive_numeric_filter_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @ParameterizedTest(name = "LIKE {0}")
    @MethodSource("nativeLikeCases")
    void nativeLikeMatchesFlinkByteForByte(String ignoredName, String predicate) throws Exception {
        assertDataStreamParity(
                "SELECT metric FROM like_input WHERE " + predicate,
                Types.STRING,
                Arrays.asList(
                        Row.of(""),
                        Row.of("ab"),
                        Row.of("abc"),
                        Row.of("z😀x"),
                        Row.of("a_c"),
                        Row.of("a.c"),
                        Row.of("你好"),
                        Row.of((Object) null)),
                "like_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> nativeLikeCases() {
        return Stream.of(
                Arguments.of("prefix", "metric LIKE 'ab%'"),
                Arguments.of("Unicode wildcard", "metric LIKE '%😀_'"),
                Arguments.of("single wildcard", "metric LIKE 'a_c'"),
                Arguments.of("literal regex punctuation", "metric LIKE 'a.c'"),
                Arguments.of("negated", "metric NOT LIKE '%x%'"));
    }

    @ParameterizedTest(name = "STARTS_WITH {0}")
    @ValueSource(strings = {"", "ab", "😀", "%", "_", "你好"})
    void nativeStartsWithMatchesFlinkByteForByte(String prefix) throws Exception {
        String escapedPrefix = prefix.replace("'", "''");
        assertDataStreamParity(
                "SELECT metric FROM starts_with_input WHERE STARTSWITH(metric, '" + escapedPrefix + "')",
                Types.STRING,
                Arrays.asList(
                        Row.of(""),
                        Row.of("ab"),
                        Row.of("abc"),
                        Row.of("😀x"),
                        Row.of("%literal"),
                        Row.of("_literal"),
                        Row.of("你好世界"),
                        Row.of((Object) null)),
                "starts_with_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @ParameterizedTest(name = "SUBSTRING {0}")
    @MethodSource("nativeSubstringCases")
    void nativeSubstringMatchesFlinkByteForByte(String ignoredName, String expression) throws Exception {
        assertDataStreamParity(
                "SELECT " + expression + " FROM substring_input",
                Types.STRING,
                Arrays.asList(Row.of(""), Row.of("abcdef"), Row.of("你好世界"), Row.of("a😀bc"), Row.of((Object) null)),
                "substring_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> nativeSubstringCases() {
        return Stream.of(
                Arguments.of("standard", "SUBSTRING(metric FROM 1 FOR 2)"),
                Arguments.of("remainder", "SUBSTRING(metric FROM 2)"),
                Arguments.of("alias", "SUBSTR(metric, 2, 3)"),
                Arguments.of("zero length", "SUBSTRING(metric FROM 2 FOR 0)"),
                Arguments.of("past end", "SUBSTRING(metric FROM 99 FOR 4)"));
    }

    @Test
    void negativeSubstringStartFallsBackToFlink() throws Exception {
        assertDataStreamParity(
                "SELECT SUBSTRING(metric FROM -2 FOR 2) FROM substring_fallback_input",
                Types.STRING,
                Arrays.asList(Row.of("abcdef"), Row.of("你好世界"), Row.of((Object) null)),
                "substring_fallback_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isZero();
    }

    @ParameterizedTest(name = "SUBSTRING filter {0}")
    @MethodSource("nativeSubstringFilterCases")
    void nativeSubstringFiltersMatchFlinkByteForByte(String ignoredName, String predicate) throws Exception {
        assertDataStreamParity(
                "SELECT metric FROM substring_filter_input WHERE " + predicate,
                Types.STRING,
                Arrays.asList(Row.of(""), Row.of("abcd"), Row.of("ab😀d"), Row.of("你好世界"), Row.of((Object) null)),
                "substring_filter_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> nativeSubstringFilterCases() {
        return Stream.of(
                Arguments.of("equal", "SUBSTRING(metric FROM 1 FOR 2) = 'ab'"),
                Arguments.of("not equal", "SUBSTR(metric, 2, 2) <> 'bc'"),
                Arguments.of("reverse ordered", "'bc' < SUBSTRING(metric FROM 2 FOR 2)"),
                Arguments.of("remainder", "SUBSTRING(metric FROM 2) >= 'b'"),
                Arguments.of("null safe", "SUBSTRING(metric FROM 1 FOR 2) IS DISTINCT FROM 'ab'"));
    }
}

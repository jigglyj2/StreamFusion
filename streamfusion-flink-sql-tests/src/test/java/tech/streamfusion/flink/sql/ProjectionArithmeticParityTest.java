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
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class ProjectionArithmeticParityTest extends SqlParityTestSupport {
    @Test
    void acceleratedExecutionMatchesFlinkByteForByte() throws Exception {
        assertParity(BATCH_SQL, false);
    }

    @Test
    void boundedIdentityCalcRunsNativelyAndMatchesFlinkByteForByte() throws Exception {
        assertParity(IDENTITY_CALC_SQL, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nativeProjectionCases")
    void nativeInputReferenceProjectionsMatchFlinkByteForByte(
            String ignoredName, String sql, boolean nativeExecutionExpected) throws Exception {
        assertParity(sql, true);

        if (nativeExecutionExpected) {
            assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
        }
    }

    private static Stream<Arguments> nativeProjectionCases() {
        return Stream.of(
                Arguments.of("multi-column-reordered-types", MULTI_COLUMN_PROJECTION_SQL, true),
                Arguments.of("filter-on-unprojected-int", FILTER_ON_UNPROJECTED_COLUMN_SQL, true),
                Arguments.of("casts-force-whole-plan-fallback", SCALAR_TYPE_PROJECTION_SQL, false));
    }

    @Test
    void nativeIntegerArithmeticMatchesFlinkByteForByte() throws Exception {
        assertParity(INTEGER_ARITHMETIC_SQL, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @Test
    void nativeIntegerToBigintCastMatchesFlinkByteForByte() throws Exception {
        String sql =
                "SELECT CAST(id AS BIGINT) FROM " + "(VALUES (-2147483648), (-1), (0), (1), (2147483647)) AS input(id)";
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @Test
    void nativeIntegerToDoubleCastMatchesFlinkByteForByte() throws Exception {
        String sql =
                "SELECT CAST(id AS DOUBLE) FROM " + "(VALUES (-2147483648), (-1), (0), (1), (2147483647)) AS input(id)";
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @Test
    void nativeBooleanLiteralProjectionsMatchFlinkByteForByte() throws Exception {
        String sql = "SELECT enabled, TRUE, FALSE FROM "
                + "(VALUES (1, TRUE), (2, FALSE)) AS input(id, enabled) WHERE id >= 1";
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @ParameterizedTest(name = "{0} literal projections")
    @MethodSource("narrowIntegerLiteralProjectionCases")
    void nativeNarrowIntegerLiteralProjectionsMatchFlinkByteForByte(String ignoredName, String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> narrowIntegerLiteralProjectionCases() {
        return Stream.of(
                Arguments.of(
                        "tinyint",
                        "SELECT CAST(-128 AS TINYINT), CAST(0 AS TINYINT), CAST(127 AS TINYINT) "
                                + "FROM (VALUES (1), (2)) AS input(id) WHERE id >= 1"),
                Arguments.of(
                        "smallint",
                        "SELECT CAST(-32768 AS SMALLINT), CAST(0 AS SMALLINT), CAST(32767 AS SMALLINT) "
                                + "FROM (VALUES (1), (2)) AS input(id) WHERE id >= 1"));
    }

    @ParameterizedTest(name = "NULL AS {0}")
    @MethodSource("typedNullProjectionTypes")
    void nativeTypedNullProjectionsMatchFlinkByteForByte(String type) throws Exception {
        String sql = "SELECT CAST(NULL AS " + type + ") FROM (VALUES (1), (2)) AS input(id) WHERE id >= 1";
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<String> typedNullProjectionTypes() {
        return Stream.of(
                "TINYINT",
                "SMALLINT",
                "INT",
                "BIGINT",
                "FLOAT",
                "DOUBLE",
                "BOOLEAN",
                "CHAR(5)",
                "VARCHAR(12)",
                "BINARY(8)",
                "VARBINARY(8)",
                "DECIMAL(20, 4)",
                "DATE",
                "TIME(6)",
                "TIMESTAMP(6)");
    }

    @Test
    void nativeBooleanExpressionProjectionsMatchFlinkByteForByte() throws Exception {
        String sql = "SELECT NOT left_flag, left_flag AND right_flag, left_flag OR right_flag, "
                + "NOT (left_flag AND right_flag), (left_flag OR FALSE) AND TRUE "
                + "FROM (VALUES (TRUE, TRUE), (TRUE, FALSE), (FALSE, TRUE), (FALSE, FALSE)) "
                + "AS input(left_flag, right_flag)";
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @Test
    void nativeComparisonProjectionsMatchFlinkByteForByte() throws Exception {
        String sql = "SELECT id = 2, id <> 2, id < 2, id <= 2, id > 2, id >= 2, "
                + "(id >= 2 AND enabled), NOT (id = 2 OR enabled) "
                + "FROM (VALUES (1, TRUE), (2, FALSE), (3, TRUE)) AS input(id, enabled)";
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @Test
    void nativeBooleanTruthTestsMatchFlinkByteForByte() throws Exception {
        String sql = "SELECT flag IS TRUE, flag IS FALSE, flag IS NOT TRUE, flag IS NOT FALSE "
                + "FROM (VALUES (TRUE), (FALSE)) AS input(flag)";
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @Test
    void nativeNullSafeComparisonProjectionsMatchFlinkByteForByte() throws Exception {
        String sql = "SELECT id IS DISTINCT FROM 2, id IS NOT DISTINCT FROM 2, "
                + "left_id IS DISTINCT FROM right_id, left_id IS NOT DISTINCT FROM right_id, "
                + "left_flag IS DISTINCT FROM right_flag, left_flag IS NOT DISTINCT FROM right_flag "
                + "FROM (VALUES (1, 1, 2, TRUE, FALSE), (2, 2, 2, TRUE, TRUE), "
                + "(3, 3, 2, FALSE, FALSE)) "
                + "AS input(id, left_id, right_id, left_flag, right_flag)";
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @Test
    void nativeDateLiteralProjectionsMatchFlinkByteForByte() throws Exception {
        String sql = "SELECT DATE '1969-12-31', DATE '1970-01-01', DATE '2026-08-27' "
                + "FROM (VALUES (1), (2)) AS input(id) WHERE id >= 1";
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @Test
    void nativeTimeLiteralProjectionsMatchFlinkByteForByte() throws Exception {
        String sql = "SELECT TIME '00:00:00', TIME '12:34:56.123', TIME '23:59:59.999' "
                + "FROM (VALUES (1), (2)) AS input(id) WHERE id >= 1";
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @Test
    void nativeTimestampLiteralProjectionsMatchFlinkByteForByte() throws Exception {
        String sql = "SELECT CAST('1969-12-31 23:59:59.999999' AS TIMESTAMP(6)), "
                + "CAST('1970-01-01 00:00:00' AS TIMESTAMP(6)), "
                + "CAST('2026-08-27 12:34:56.123456' AS TIMESTAMP(6)) "
                + "FROM (VALUES (1), (2)) AS input(id) WHERE id >= 1";
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @Test
    void nativeCharacterLiteralProjectionsMatchFlinkByteForByte() throws Exception {
        String sql = "SELECT '', 'alpha', 'élan', '東京' " + "FROM (VALUES (1), (2)) AS input(id) WHERE id >= 1";
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @Test
    void nativeBinaryLiteralProjectionsMatchFlinkByteForByte() throws Exception {
        String sql = "SELECT X'00', X'0102', X'80FF' " + "FROM (VALUES (1), (2)) AS input(id) WHERE id >= 1";
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @ParameterizedTest(name = "{0} unary minus")
    @MethodSource("nativeUnaryMinusCases")
    void nativeUnaryMinusMatchesFlinkByteForByte(String ignoredName, String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> nativeUnaryMinusCases() {
        return Stream.of(
                Arguments.of("integer", "SELECT -metric FROM (VALUES (-2147483648), (-1), (0), (7)) AS input(metric)"),
                Arguments.of(
                        "bigint", "SELECT -metric FROM (VALUES (-2147483649), (0), (2147483649)) AS input(metric)"),
                Arguments.of("double", "SELECT -metric FROM (VALUES (-2.5E0), (-0.0E0), (3.25E0)) AS input(metric)"),
                Arguments.of("decimal", "SELECT -metric FROM (VALUES (-12.34), (0.00), (99.99)) AS input(metric)"));
    }

    @ParameterizedTest(name = "{0} division")
    @MethodSource("nativeFloatingDivisionCases")
    void nativeFloatingDivisionMatchesFlinkByteForByte(String ignoredName, String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> nativeFloatingDivisionCases() {
        return Stream.of(
                Arguments.of(
                        "float",
                        "SELECT numerator / denominator FROM (VALUES "
                                + "(CAST(7.0 AS FLOAT), CAST(2.0 AS FLOAT)), "
                                + "(CAST(1.0 AS FLOAT), CAST(0.0 AS FLOAT)), "
                                + "(CAST(-1.0 AS FLOAT), CAST(-0.0 AS FLOAT))) "
                                + "AS input(numerator, denominator)"),
                Arguments.of(
                        "double",
                        "SELECT numerator / denominator FROM (VALUES "
                                + "(7.0E0, 2.0E0), (1.0E0, 0.0E0), (-1.0E0, -0.0E0), (0.0E0, 0.0E0)) "
                                + "AS input(numerator, denominator)"));
    }

    @Test
    void nativeBigintArithmeticMatchesFlinkByteForByte() throws Exception {
        assertParity(BIGINT_ARITHMETIC_SQL, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @Test
    void nativeBigintArithmeticWrapsOverflowLikeFlink() throws Exception {
        String sql = "SELECT id + 2147483648 FROM "
                + "(VALUES (9223372034707292159), (9223372034707292160)) AS input(id) "
                + "WHERE id >= 9223372034707292159";
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @ParameterizedTest(name = "{0} decimal arithmetic")
    @MethodSource("nativeDecimalArithmeticCases")
    void nativeDecimalArithmeticMatchesFlinkByteForByte(String ignoredName, String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    @ParameterizedTest(name = "{0} column comparison")
    @MethodSource("nativeColumnComparisonCases")
    void nativeColumnComparisonsMatchFlinkByteForByte(String ignoredName, String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> nativeColumnComparisonCases() {
        return Stream.of(
                Arguments.of(
                        "integer",
                        "SELECT left_value, right_value FROM "
                                + "(VALUES (1, 2), (3, 2), (4, 4)) AS input(left_value, right_value) "
                                + "WHERE left_value <= right_value"),
                Arguments.of("less than", columnComparisonSql("<")),
                Arguments.of("equal", columnComparisonSql("=")),
                Arguments.of("not equal", columnComparisonSql("<>")),
                Arguments.of("greater than", columnComparisonSql(">")),
                Arguments.of("greater than or equal", columnComparisonSql(">=")),
                Arguments.of(
                        "bigint",
                        "SELECT left_value, right_value FROM (VALUES "
                                + "(2147483648, 2147483649), (2147483650, 2147483649), "
                                + "(2147483651, 2147483651)) AS input(left_value, right_value) "
                                + "WHERE left_value <= right_value"),
                Arguments.of(
                        "decimal",
                        "SELECT left_value, right_value FROM "
                                + "(VALUES (1.25, 2.50), (3.75, 2.50), (4.00, 4.00)) "
                                + "AS input(left_value, right_value) WHERE left_value <= right_value"),
                Arguments.of(
                        "date",
                        "SELECT left_value, right_value FROM (VALUES "
                                + "(DATE '1969-12-31', DATE '1970-01-01'), "
                                + "(DATE '2026-01-03', DATE '2026-01-02'), "
                                + "(DATE '2026-01-04', DATE '2026-01-04')) "
                                + "AS input(left_value, right_value) WHERE left_value <= right_value"),
                Arguments.of(
                        "time",
                        "SELECT left_value, right_value FROM (VALUES "
                                + "(TIME '01:02:03.123', TIME '01:02:03.124'), "
                                + "(TIME '23:00:00.000', TIME '22:00:00.000'), "
                                + "(TIME '12:00:00.000', TIME '12:00:00.000')) "
                                + "AS input(left_value, right_value) WHERE left_value <= right_value"),
                Arguments.of(
                        "timestamp",
                        "SELECT left_value, right_value FROM (VALUES "
                                + "(TIMESTAMP '1969-12-31 23:59:59.999', TIMESTAMP '1970-01-01 00:00:00.000'), "
                                + "(TIMESTAMP '2026-01-03 00:00:00.000', TIMESTAMP '2026-01-02 00:00:00.000'), "
                                + "(TIMESTAMP '2026-01-04 00:00:00.000', TIMESTAMP '2026-01-04 00:00:00.000')) "
                                + "AS input(left_value, right_value) WHERE left_value <= right_value"),
                Arguments.of(
                        "boolean equality",
                        "SELECT left_value, right_value FROM "
                                + "(VALUES (TRUE, FALSE), (TRUE, TRUE), (FALSE, FALSE)) "
                                + "AS input(left_value, right_value) WHERE left_value = right_value"),
                Arguments.of(
                        "boolean inequality",
                        "SELECT left_value, right_value FROM "
                                + "(VALUES (TRUE, FALSE), (TRUE, TRUE), (FALSE, FALSE)) "
                                + "AS input(left_value, right_value) WHERE left_value <> right_value"));
    }

    private static String columnComparisonSql(String operator) {
        return "SELECT left_value, right_value FROM "
                + "(VALUES (1, 2), (3, 2), (4, 4)) AS input(left_value, right_value) WHERE left_value "
                + operator
                + " right_value";
    }

    private static Stream<Arguments> nativeDecimalArithmeticCases() {
        return Stream.of(
                Arguments.of(
                        "compact",
                        "SELECT amount + 1.25, amount - 1.25, amount * 1.25, "
                                + "(amount + 1.25) * (amount - 1.25) "
                                + "FROM (VALUES (-12.34), (0.00), (99.99)) AS input(amount)"),
                Arguments.of(
                        "wide",
                        "SELECT amount + 1.25, amount - 1.25, amount * 1.25 "
                                + "FROM (VALUES (-1234567890123456.78), (0.00), (1234567890123456.78)) "
                                + "AS input(amount)"),
                Arguments.of(
                        "division",
                        "SELECT numerator / denominator FROM (VALUES "
                                + "(CAST(1 AS DECIMAL(38, 18)), CAST(3 AS DECIMAL(20, 4))), "
                                + "(CAST(-2 AS DECIMAL(38, 18)), CAST(3 AS DECIMAL(20, 4))), "
                                + "(1234567890123456.78, CAST(7 AS DECIMAL(20, 2)))) "
                                + "AS input(numerator, denominator)"),
                Arguments.of(
                        "rescale and half-up rounding",
                        "SELECT CAST(amount AS DECIMAL(12, 6)), CAST(amount AS DECIMAL(3, 1)) "
                                + "FROM (VALUES (-12.345), (0.005), (9.999)) AS input(amount)"),
                Arguments.of(
                        "signed integer casts",
                        "SELECT CAST(tiny_value AS DECIMAL(5, 2)), "
                                + "CAST(small_value AS DECIMAL(8, 3)), "
                                + "CAST(integer_value AS DECIMAL(12, 2)), "
                                + "CAST(bigint_value AS DECIMAL(20, 2)) FROM (VALUES "
                                + "(CAST(-12 AS TINYINT), CAST(1234 AS SMALLINT), 123456, 123456789012), "
                                + "(CAST(7 AS TINYINT), CAST(-321 AS SMALLINT), -456789, -987654321098)) "
                                + "AS input(tiny_value, small_value, integer_value, bigint_value)"));
    }

    @ParameterizedTest(name = "{0} arithmetic")
    @MethodSource("nativeFloatingPointArithmeticCases")
    void floatingPointArithmeticMatchesFlinkByteForByte(String ignoredName, String sql, boolean nativeExecutionExpected)
            throws Exception {
        assertParity(sql, true);

        if (nativeExecutionExpected) {
            assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
        }
    }

    private static Stream<Arguments> nativeFloatingPointArithmeticCases() {
        return Stream.of(
                Arguments.of("FLOAT", floatingPointArithmeticSql("FLOAT"), true),
                Arguments.of("DOUBLE", nativeDoubleArithmeticSql(), true));
    }

    private static String floatingPointArithmeticSql(String type) {
        String literal = "CAST(1.5 AS " + type + ")";
        return "SELECT metric + "
                + literal
                + ", metric - "
                + literal
                + ", metric * "
                + literal
                + ", (metric + "
                + literal
                + ") * (metric - "
                + literal
                + "), "
                + literal
                + " FROM (VALUES (CAST(-2.5 AS "
                + type
                + ")), (CAST(0.0 AS "
                + type
                + ")), (CAST(3.25 AS "
                + type
                + "))) AS input(metric)";
    }

    private static String nativeDoubleArithmeticSql() {
        return "SELECT metric + 1.5E0, metric - 1.5E0, metric * 1.5E0, "
                + "(metric + 1.5E0) * (metric - 1.5E0), 1.5E0 "
                + "FROM (VALUES (-2.5E0), (0.0E0), (3.25E0)) AS input(metric)";
    }

    @Test
    void castDoubleSpecialValuesFallBackAndMatchFlinkByteForByte() throws Exception {
        String sql = "SELECT metric + 1.5E0, metric * -1.0E0 FROM (VALUES "
                + "(CAST('NaN' AS DOUBLE)), (CAST('Infinity' AS DOUBLE)), "
                + "(CAST('-Infinity' AS DOUBLE)), (-0.0E0)) AS input(metric)";
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isZero();
    }

    @Test
    void nativeIntegerDivisionByNonzeroLiteralMatchesFlinkByteForByte() throws Exception {
        assertParity("SELECT id / 2, id / -2 FROM (VALUES (-7), (-1), (0), (1), (7)) AS input(id)", true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @Test
    void nativeBigintDivisionByNonzeroLiteralMatchesFlinkByteForByte() throws Exception {
        assertParity(
                "SELECT id / 2147483648, id / -2147483648 "
                        + "FROM (VALUES (-9223372036854775807), (-2147483649), (0), "
                        + "(2147483649), (9223372036854775807)) AS input(id)",
                true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @Test
    void nativeIntegerDivisionByColumnMatchesFlinkByteForByte() throws Exception {
        assertParity(
                "SELECT numerator / denominator FROM (VALUES (4, 2), (9, 3)) " + "AS input(numerator, denominator)",
                true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @Test
    void nativeNarrowIntegerDivisionByColumnMatchesFlinkByteForByte() throws Exception {
        assertParity(
                "SELECT tiny_n / tiny_d, small_n / small_d FROM (VALUES "
                        + "(CAST(-7 AS TINYINT), CAST(2 AS TINYINT), CAST(-32767 AS SMALLINT), CAST(3 AS SMALLINT)), "
                        + "(CAST(7 AS TINYINT), CAST(-2 AS TINYINT), CAST(32767 AS SMALLINT), CAST(-3 AS SMALLINT))) "
                        + "AS input(tiny_n, tiny_d, small_n, small_d)",
                true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @Test
    void nativeIntegerRemainderByNonzeroLiteralMatchesFlinkByteForByte() throws Exception {
        assertParity("SELECT MOD(id, 3), MOD(id, -3) " + "FROM (VALUES (-7), (-1), (0), (1), (7)) AS input(id)", true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @Test
    void nativeBigintRemainderByNonzeroLiteralMatchesFlinkByteForByte() throws Exception {
        assertParity(
                "SELECT MOD(id, 2147483648) "
                        + "FROM (VALUES (-9223372036854775807), (-2147483649), (0), "
                        + "(2147483649), (9223372036854775807)) AS input(id)",
                true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }
}

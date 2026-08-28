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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class SqlParityTest {
    private static final String BATCH_SQL = "SELECT id, UPPER(name), amount * 2 "
            + "FROM (VALUES (2, 'beta', 2.50), (1, 'alpha', 1.25), "
            + "(3, CAST(NULL AS STRING), CAST(NULL AS DECIMAL(10, 2)))) "
            + "AS orders(id, name, amount) WHERE id >= 1 ORDER BY id";
    private static final String STREAMING_CALC_SQL = "SELECT id, UPPER(name), amount * 2 "
            + "FROM (VALUES (1, 'alpha', 1.25), (2, 'beta', 2.50), "
            + "(3, CAST(NULL AS STRING), CAST(NULL AS DECIMAL(10, 2)))) "
            + "AS orders(id, name, amount) WHERE id >= 2";
    private static final String STREAMING_AGGREGATE_SQL = "SELECT category, COUNT(*), SUM(amount) "
            + "FROM (VALUES ('a', 1), ('b', 2), ('a', 3)) AS orders(category, amount) "
            + "GROUP BY category";
    private static final String IDENTITY_CALC_SQL = "SELECT id FROM (VALUES (1), (2), (3)) AS input(id) WHERE id >= 2";
    private static final String MULTI_COLUMN_PROJECTION_SQL = "SELECT name, enabled, id "
            + "FROM (VALUES (1, 'one', TRUE), (2, 'two', FALSE)) "
            + "AS input(id, name, enabled) WHERE id >= 1";
    private static final String FILTER_ON_UNPROJECTED_COLUMN_SQL =
            "SELECT name, id " + "FROM (VALUES (1, 'one'), (2, 'two')) AS input(id, name) WHERE id >= 2";
    private static final String SCALAR_TYPE_PROJECTION_SQL = "SELECT tiny_value, small_value, big_value, "
            + "float_value, double_value, boolean_value, char_value, varchar_value, binary_value, "
            + "varbinary_value, decimal_value, date_value, time_value, timestamp_value FROM (VALUES (1, "
            + "CAST(2 AS TINYINT), CAST(3 AS SMALLINT), CAST(4 AS BIGINT), CAST(1.5 AS FLOAT), "
            + "CAST(2.5 AS DOUBLE), TRUE, CAST('abc' AS CHAR(3)), CAST('text' AS VARCHAR(8)), "
            + "CAST(X'0102' AS BINARY(2)), CAST(X'0304' AS VARBINARY(4)), CAST(12.34 AS DECIMAL(10, 2)), "
            + "DATE '2026-08-27', TIME '12:34:56.123', TIMESTAMP '2026-08-27 12:34:56.123')) "
            + "AS input(id, tiny_value, small_value, big_value, float_value, double_value, boolean_value, "
            + "char_value, varchar_value, binary_value, varbinary_value, decimal_value, date_value, "
            + "time_value, timestamp_value) WHERE id >= 1";
    private static final String INTEGER_ARITHMETIC_SQL = "SELECT id + 10, id - 1, id * 3, "
            + "(id + 2) * (id - 1), 7 FROM (VALUES (1), (2), (3)) AS input(id) WHERE id >= 1";
    private static final String BIGINT_ARITHMETIC_SQL = "SELECT id + 2147483648, id - 2147483648, "
            + "id * 2147483648, (id + 2147483648) * (id - 2147483648), 2147483648 "
            + "FROM (VALUES (2147483648), (2147483649), (2147483650)) AS input(id) "
            + "WHERE id >= 2147483648";

    @AfterEach
    void clearPlannerOverride() {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
    }

    @Test
    void acceleratedExecutionMatchesFlinkByteForByte() throws Exception {
        assertParity(BATCH_SQL, false);
    }

    @Test
    void boundedIdentityCalcRunsNativelyAndMatchesFlinkByteForByte() throws Exception {
        assertParity(IDENTITY_CALC_SQL, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
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
                "TIMESTAMP(9)");
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
        String sql = "SELECT TIMESTAMP '1969-12-31 23:59:59.999999999', "
                + "TIMESTAMP '1970-01-01 00:00:00', "
                + "TIMESTAMP '2026-08-27 12:34:56.123456789' "
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
        return Stream.of(Arguments.of(
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

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
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
                                + "AS input(amount)"));
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
                Arguments.of("FLOAT cast shape falls back", floatingPointArithmeticSql("FLOAT"), false),
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
    void divisionByColumnFallsBackAndMatchesFlinkByteForByte() throws Exception {
        assertParity(
                "SELECT numerator / denominator FROM (VALUES (4, 2), (9, 3)) " + "AS input(numerator, denominator)",
                true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isZero();
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

    @ParameterizedTest(name = "{0}")
    @MethodSource("nativeComparisonCases")
    void nativeIntegerComparisonsMatchFlinkByteForByte(
            String ignoredName, String predicate, boolean nativeExecutionExpected) throws Exception {
        String sql = "SELECT id FROM (VALUES (1), (2), (3), (4), (5)) AS input(id) WHERE " + predicate;
        assertParity(sql, true);

        if (nativeExecutionExpected) {
            assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
        }
    }

    private static Stream<Arguments> nativeComparisonCases() {
        return Stream.of(
                Arguments.of("equal-constant-folded-by-flink", "id = 2", false),
                Arguments.of("not-equal", "id <> 2", true),
                Arguments.of("less-than", "id < 2", true),
                Arguments.of("less-than-or-equal", "id <= 2", true),
                Arguments.of("greater-than", "id > 2", true),
                Arguments.of("greater-than-or-equal", "id >= 2", true),
                Arguments.of("literal-on-left", "2 < id", true));
    }

    @ParameterizedTest(name = "BIGINT {0}")
    @MethodSource("nativeBigintComparisonCases")
    void nativeBigintComparisonsMatchFlinkByteForByte(String ignoredName, String predicate) throws Exception {
        String sql = "SELECT payload FROM (VALUES "
                + "(2147483648, 10), (2147483649, 20), (2147483649, 21), (2147483650, 30)) "
                + "AS input(id, payload) WHERE "
                + predicate;
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> nativeBigintComparisonCases() {
        return Stream.of(
                Arguments.of("equal", "id = 2147483649"),
                Arguments.of("not-equal", "id <> 2147483649"),
                Arguments.of("less-than", "id < 2147483649"),
                Arguments.of("less-than-or-equal", "id <= 2147483649"),
                Arguments.of("greater-than", "id > 2147483649"),
                Arguments.of("greater-than-or-equal", "id >= 2147483649"),
                Arguments.of("literal-on-left", "2147483649 < id"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nativeNarrowIntegerComparisonCases")
    void nativeNarrowIntegerComparisonsMatchFlinkByteForByte(
            String ignoredName, String valueType, String predicate, boolean nativeExecutionExpected) throws Exception {
        String sql = "SELECT payload FROM (VALUES "
                + "(CAST(-2 AS "
                + valueType
                + "), 10), (CAST(0 AS "
                + valueType
                + "), 20), (CAST(2 AS "
                + valueType
                + "), 30)) AS input(id, payload) WHERE "
                + predicate;
        assertParity(sql, true);

        if (nativeExecutionExpected) {
            assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
        }
    }

    private static Stream<Arguments> nativeNarrowIntegerComparisonCases() {
        return Stream.of(
                Arguments.of("TINYINT coerced input falls back", "TINYINT", "id >= 0", false),
                Arguments.of("TINYINT native comparison", "TINYINT", "0 < id", true),
                Arguments.of("SMALLINT coerced input falls back", "SMALLINT", "id >= 0", false),
                Arguments.of("SMALLINT native comparison", "SMALLINT", "0 < id", true));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nativeFloatingPointComparisonCases")
    void nativeFloatingPointComparisonsMatchFlinkByteForByte(
            String ignoredName, String valueType, String predicate, boolean nativeExecutionExpected) throws Exception {
        String sql = "SELECT payload FROM (VALUES "
                + "(CAST(-1.5 AS "
                + valueType
                + "), 10), (CAST(0.5 AS "
                + valueType
                + "), 20), (CAST(2.5 AS "
                + valueType
                + "), 30)) AS input(metric, payload) WHERE "
                + predicate;
        assertParity(sql, true);

        if (nativeExecutionExpected) {
            assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
        }
    }

    private static Stream<Arguments> nativeFloatingPointComparisonCases() {
        return Stream.of(
                Arguments.of("FLOAT coerced input falls back", "FLOAT", "metric >= 0.5", false),
                Arguments.of("FLOAT native comparison", "FLOAT", "CAST(0.5 AS FLOAT) < metric", true),
                Arguments.of("DOUBLE coerced input falls back", "DOUBLE", "metric >= 0.5", false),
                Arguments.of("DOUBLE native comparison", "DOUBLE", "CAST(0.5 AS DOUBLE) < metric", true));
    }

    @ParameterizedTest(name = "DATE {0}")
    @MethodSource("nativeDateComparisonCases")
    void nativeDateComparisonsMatchFlinkByteForByte(String ignoredName, String predicate) throws Exception {
        String sql = "SELECT payload FROM (VALUES "
                + "(DATE '1969-12-31', 10), (DATE '2026-08-27', 20), "
                + "(DATE '2026-08-27', 21), (DATE '2030-01-01', 30)) "
                + "AS input(event_date, payload) WHERE "
                + predicate;
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> nativeDateComparisonCases() {
        return Stream.of(
                Arguments.of("equal", "event_date = DATE '2026-08-27'"),
                Arguments.of("pre-epoch", "event_date < DATE '1970-01-01'"),
                Arguments.of("greater-than-or-equal", "event_date >= DATE '2026-08-27'"),
                Arguments.of("literal-on-left", "DATE '2026-08-27' < event_date"));
    }

    @ParameterizedTest(name = "TIME {0}")
    @MethodSource("nativeTimeComparisonCases")
    void nativeTimeComparisonsMatchFlinkByteForByte(String ignoredName, String predicate) throws Exception {
        String sql = "SELECT payload FROM (VALUES "
                + "(TIME '00:00:00.000', 10), (TIME '12:34:56.123', 20), "
                + "(TIME '12:34:56.123', 21), (TIME '23:59:59.999', 30)) "
                + "AS input(event_time, payload) WHERE "
                + predicate;
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> nativeTimeComparisonCases() {
        return Stream.of(
                Arguments.of("equal at millisecond precision", "event_time = TIME '12:34:56.123'"),
                Arguments.of("midnight lower bound", "event_time >= TIME '00:00:00.000'"),
                Arguments.of("end-of-day range", "event_time < TIME '23:59:59.999'"),
                Arguments.of("literal-on-left", "TIME '12:34:56.123' < event_time"));
    }

    @ParameterizedTest(name = "TIME({0})")
    @MethodSource("nativeTimePrecisionCases")
    void castTimePrecisionFallsBackAndMatchesFlinkByteForByte(int precision) throws Exception {
        String type = "TIME(" + precision + ")";
        String sql = "SELECT payload FROM (VALUES "
                + "(CAST(TIME '00:00:00.000' AS "
                + type
                + "), 10), (CAST(TIME '12:34:56.123' AS "
                + type
                + "), 20), (CAST(TIME '23:59:59.999' AS "
                + type
                + "), 30)) AS input(event_time, payload) WHERE "
                + "CAST(TIME '12:34:56.123' AS "
                + type
                + ") <= event_time";
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isZero();
    }

    private static Stream<Integer> nativeTimePrecisionCases() {
        return Stream.of(0, 6, 9);
    }

    @ParameterizedTest(name = "TIMESTAMP {0}")
    @MethodSource("nativeTimestampComparisonCases")
    void nativeTimestampComparisonsMatchFlinkByteForByte(String ignoredName, String predicate) throws Exception {
        String sql = "SELECT payload FROM (VALUES "
                + "(TIMESTAMP '1969-12-31 23:59:59.123', 10), "
                + "(TIMESTAMP '2026-08-27 12:34:56.123', 20), "
                + "(TIMESTAMP '2026-08-27 12:34:56.123', 21), "
                + "(TIMESTAMP '2030-01-01 00:00:00.000', 30)) "
                + "AS input(event_timestamp, payload) WHERE "
                + predicate;
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> nativeTimestampComparisonCases() {
        return Stream.of(
                Arguments.of("equal", "event_timestamp = TIMESTAMP '2026-08-27 12:34:56.123'"),
                Arguments.of("pre-epoch", "event_timestamp < TIMESTAMP '1970-01-01 00:00:00.000'"),
                Arguments.of("range", "event_timestamp >= TIMESTAMP '2026-08-27 12:34:56.123'"),
                Arguments.of("literal-on-left", "TIMESTAMP '2026-08-27 12:34:56.123' < event_timestamp"));
    }

    @ParameterizedTest(name = "DECIMAL {0}")
    @MethodSource("nativeDecimalComparisonCases")
    void nativeDecimalComparisonsMatchFlinkByteForByte(String ignoredName, String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> nativeDecimalComparisonCases() {
        String compactValues =
                "(VALUES (-12.34, 10), (12.34, 20), (12.34, 21), (99.99, 30)) " + "AS input(amount, payload)";
        String wideValues = "(VALUES "
                + "(-1234567890.123456789, 10), "
                + "(1234567890.123456789, 20), "
                + "(1234567890.123456789, 21), "
                + "(2234567890.123456789, 30)) AS input(amount, payload)";
        return Stream.of(
                Arguments.of(
                        "compact input on left", "SELECT payload FROM " + compactValues + " WHERE amount >= 12.34"),
                Arguments.of(
                        "compact literal on left", "SELECT payload FROM " + compactValues + " WHERE 12.34 <= amount"),
                Arguments.of(
                        "wide input on left",
                        "SELECT payload FROM " + wideValues + " WHERE amount >= 1234567890.123456789"),
                Arguments.of(
                        "wide literal on left",
                        "SELECT payload FROM " + wideValues + " WHERE 1234567890.123456789 <= amount"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nativeNullPredicateCases")
    void nativeNullPredicatesMatchFlinkByteForByte(String ignoredName, String predicate) throws Exception {
        String sql = "SELECT id FROM (VALUES (1), (CAST(NULL AS INT)), (3)) AS input(id) WHERE " + predicate;
        assertParity(sql, true);
    }

    private static Stream<Arguments> nativeNullPredicateCases() {
        return Stream.of(Arguments.of("is-null", "id IS NULL"), Arguments.of("is-not-null", "id IS NOT NULL"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nativeUnknownPredicateCases")
    void nativeUnknownPredicatesMatchFlinkByteForByte(String ignoredName, String predicate) throws Exception {
        String sql = "SELECT id FROM (VALUES (1, TRUE), (2, FALSE), (3, CAST(NULL AS BOOLEAN))) "
                + "AS input(id, flag) WHERE "
                + predicate;
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> nativeUnknownPredicateCases() {
        return Stream.of(Arguments.of("is-unknown", "flag IS UNKNOWN"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nativeBooleanPredicateCases")
    void nativeBooleanPredicatesMatchFlinkByteForByte(String ignoredName, String predicate) throws Exception {
        String sql = "SELECT id FROM (VALUES (1, 5), (2, 4), (3, 3), (4, 2), (5, 1)) "
                + "AS input(id, payload) WHERE "
                + predicate;
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> nativeBooleanPredicateCases() {
        return Stream.of(
                Arguments.of("and", "id >= 2 AND payload < 5"),
                Arguments.of("or", "id < 2 OR id > 2"),
                Arguments.of("not", "NOT (id >= 2 AND id <= 2)"),
                Arguments.of("nested", "(id >= 2 AND payload <= 4) OR id = 1"));
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
                Arguments.of("boolean-column-and-comparison", "enabled AND id >= 2"));
    }

    @Test
    void explainStatesWhyCurrentPlanFallsBack() {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        assertThat(tableEnvironment.explainSql(STREAMING_CALC_SQL))
                .contains("== StreamFusion Acceleration ==")
                .contains("Accelerated: no")
                .contains("Flink plan conversion to StreamFusion physical operators is not implemented")
                .contains("Flink RowData Arrow batch views and native materialization are TODO");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("streamingSqlCases")
    void acceleratedStreamingExecutionMatchesFlinkByteForByte(String ignoredName, String sql) throws Exception {
        assertParity(sql, true);
    }

    private static Stream<Arguments> streamingSqlCases() {
        return Stream.of(
                Arguments.of("calc", STREAMING_CALC_SQL),
                Arguments.of("group-aggregate-changelog", STREAMING_AGGREGATE_SQL));
    }

    private static void assertParity(String sql, boolean streaming) throws Exception {
        byte[] flinkResult = execute(sql, streaming, false);
        byte[] streamFusionResult = execute(sql, streaming, true);

        assertThat(StreamFusionPlannerFactory.createdPlannerCount()).isEqualTo(1);
        assertThat(StreamFusionPlannerFactory.translatedPlanCount()).isGreaterThan(0);
        assertThat(streamFusionResult).isEqualTo(flinkResult);
    }

    private static byte[] execute(String sql, boolean streaming, boolean streamFusionEnabled) throws Exception {
        if (streamFusionEnabled) {
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        } else {
            System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            StreamFusionPlannerFactory.resetMetrics();
        }

        EnvironmentSettings settings = streaming
                ? EnvironmentSettings.newInstance().inStreamingMode().build()
                : EnvironmentSettings.newInstance().inBatchMode().build();
        RuntimeExecutionMode runtimeMode = streaming ? RuntimeExecutionMode.STREAMING : RuntimeExecutionMode.BATCH;
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);
        tableEnvironment.getConfig().getConfiguration().set(ExecutionOptions.RUNTIME_MODE, runtimeMode);
        tableEnvironment.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);

        TableResult result = tableEnvironment.executeSql(sql);
        try (CloseableIterator<Row> rows = result.collect();
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream output = new DataOutputStream(bytes)) {
            List<byte[]> encodedRows = new ArrayList<>();
            while (rows.hasNext()) {
                Row resultRow = rows.next();
                byte[] row = (resultRow.getKind().shortString() + resultRow).getBytes(StandardCharsets.UTF_8);
                encodedRows.add(row);
            }
            encodedRows.sort(SqlParityTest::compareUnsigned);
            for (byte[] row : encodedRows) {
                output.writeInt(row.length);
                output.write(row);
            }
            output.flush();
            return bytes.toByteArray();
        }
    }

    private static int compareUnsigned(byte[] left, byte[] right) {
        return Arrays.compareUnsigned(left, right);
    }
}

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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class ComparisonParityTest extends SqlParityTestSupport {
    @ParameterizedTest(name = "{0}")
    @MethodSource("nativeComparisonCases")
    void nativeIntegerComparisonsMatchFlinkByteForByte(
            String ignoredName, String predicate, boolean nativeExecutionExpected) throws Exception {
        String sql = "SELECT id FROM (VALUES (1), (2), (3), (4), (5)) AS input(id) WHERE " + predicate;
        assertParity(sql, true, nativeExecutionExpected || ignoredName.contains("constant-folded"));

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
        assertParity(sql, true, nativeExecutionExpected);

        if (nativeExecutionExpected) {
            assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
        }
    }

    private static Stream<Arguments> nativeNarrowIntegerComparisonCases() {
        return Stream.of(
                Arguments.of("TINYINT coerced input", "TINYINT", "id >= 0", true),
                Arguments.of("TINYINT native comparison", "TINYINT", "0 < id", true),
                Arguments.of("SMALLINT coerced input", "SMALLINT", "id >= 0", true),
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
        assertParity(sql, true, nativeExecutionExpected);

        if (nativeExecutionExpected) {
            assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
        }
    }

    private static Stream<Arguments> nativeFloatingPointComparisonCases() {
        return Stream.of(
                Arguments.of("FLOAT coerced literal", "FLOAT", "metric >= 0.5", true),
                Arguments.of("FLOAT native comparison", "FLOAT", "CAST(0.5 AS FLOAT) < metric", true),
                Arguments.of("DOUBLE coerced literal", "DOUBLE", "metric >= 0.5", true),
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
    void castTimePrecisionAcceleratesAndMatchesFlinkByteForByte(int precision) throws Exception {
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

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
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
        return Stream.of(
                Arguments.of("is-unknown", "flag IS UNKNOWN"), Arguments.of("is-not-unknown", "flag IS NOT UNKNOWN"));
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
    @MethodSource("nativeDataStreamRangePredicateCases")
    void nativeDataStreamRangePredicatesMatchFlinkByteForByte(String ignoredName, String predicate) throws Exception {
        assertIntegerDataStreamParity("SELECT metric FROM integer_input WHERE " + predicate);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> nativeDataStreamRangePredicateCases() {
        return Stream.of(
                Arguments.of("between", "metric BETWEEN 2 AND 3"),
                Arguments.of("not-between", "metric NOT BETWEEN 2 AND 3"),
                Arguments.of("in", "metric IN (1, 3, 4)"),
                Arguments.of("not-in", "metric NOT IN (1, 3, 4)"));
    }

    @ParameterizedTest(name = "{0} BETWEEN")
    @MethodSource("nativeIntegralDataStreamRangeCases")
    void nativeIntegralDataStreamRangesMatchFlinkByteForByte(
            String ignoredName, TypeInformation<?> type, List<Row> rows, String predicate) throws Exception {
        assertDataStreamParity("SELECT metric FROM integral_input WHERE " + predicate, type, rows, "integral_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> nativeIntegralDataStreamRangeCases() {
        return Stream.of(
                Arguments.of(
                        "tinyint",
                        Types.BYTE,
                        Arrays.asList(Row.of((byte) -3), Row.of((byte) -2), Row.of((byte) 2), Row.of((byte) 3)),
                        "metric BETWEEN CAST(-2 AS TINYINT) AND CAST(2 AS TINYINT)"),
                Arguments.of(
                        "smallint",
                        Types.SHORT,
                        Arrays.asList(
                                Row.of((short) -300), Row.of((short) -200), Row.of((short) 200), Row.of((short) 300)),
                        "metric BETWEEN CAST(-200 AS SMALLINT) AND CAST(200 AS SMALLINT)"),
                Arguments.of(
                        "bigint",
                        Types.LONG,
                        Arrays.asList(
                                Row.of(-2147483649L), Row.of(-2147483648L), Row.of(2147483648L), Row.of(2147483649L)),
                        "metric BETWEEN -2147483648 AND 2147483648"));
    }

    @ParameterizedTest(name = "DECIMAL {0}")
    @MethodSource("nativeDecimalDataStreamRangeCases")
    void nativeDecimalDataStreamRangesMatchFlinkByteForByte(String ignoredName, String predicate) throws Exception {
        assertDataStreamParity(
                "SELECT metric FROM decimal_input WHERE " + predicate,
                Types.BIG_DEC,
                Arrays.asList(
                        Row.of(new java.math.BigDecimal("-2.500000000000000000")),
                        Row.of(new java.math.BigDecimal("-2.000000000000000000")),
                        Row.of(new java.math.BigDecimal("2.000000000000000000")),
                        Row.of(new java.math.BigDecimal("2.500000000000000000"))),
                "decimal_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> nativeDecimalDataStreamRangeCases() {
        return Stream.of(
                Arguments.of(
                        "between",
                        "metric BETWEEN CAST(-2.000000000000000000 AS DECIMAL(38, 18)) AND "
                                + "CAST(2.000000000000000000 AS DECIMAL(38, 18))"),
                Arguments.of(
                        "in",
                        "metric IN (CAST(-2.000000000000000000 AS DECIMAL(38, 18)), "
                                + "CAST(2.000000000000000000 AS DECIMAL(38, 18)))"));
    }

    @ParameterizedTest(name = "DATE {0}")
    @MethodSource("nativeDateDataStreamRangeCases")
    void nativeDateDataStreamRangesMatchFlinkByteForByte(String ignoredName, String predicate) throws Exception {
        assertDataStreamParity(
                "SELECT metric FROM date_input WHERE " + predicate,
                Types.SQL_DATE,
                Arrays.asList(
                        Row.of(java.sql.Date.valueOf("1969-12-31")),
                        Row.of(java.sql.Date.valueOf("1970-01-01")),
                        Row.of(java.sql.Date.valueOf("2026-08-28")),
                        Row.of(java.sql.Date.valueOf("2030-01-01"))),
                "date_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> nativeDateDataStreamRangeCases() {
        return Stream.of(
                Arguments.of("between", "metric BETWEEN DATE '1970-01-01' AND DATE '2026-08-28'"),
                Arguments.of("in", "metric IN (DATE '1969-12-31', DATE '2026-08-28')"));
    }

    @ParameterizedTest(name = "TIME {0}")
    @MethodSource("nativeTimeDataStreamRangeCases")
    void nativeTimeDataStreamRangesMatchFlinkByteForByte(String ignoredName, String predicate) throws Exception {
        assertDataStreamParity(
                "SELECT metric FROM time_input WHERE " + predicate,
                Types.SQL_TIME,
                Arrays.asList(
                        Row.of(java.sql.Time.valueOf("00:00:00")),
                        Row.of(java.sql.Time.valueOf("08:30:00")),
                        Row.of(java.sql.Time.valueOf("17:45:00")),
                        Row.of(java.sql.Time.valueOf("23:59:59"))),
                "time_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> nativeTimeDataStreamRangeCases() {
        return Stream.of(
                Arguments.of("between", "metric BETWEEN TIME '08:30:00' AND TIME '17:45:00'"),
                Arguments.of("in", "metric IN (TIME '00:00:00', TIME '23:59:59')"));
    }

    @ParameterizedTest(name = "TIMESTAMP {0}")
    @MethodSource("nativeTimestampDataStreamRangeCases")
    void nanosecondTimestampDataStreamRangesFallBackAndMatchFlinkByteForByte(String ignoredName, String predicate)
            throws Exception {
        assertFallbackDataStreamParity(
                "SELECT metric FROM timestamp_input WHERE " + predicate,
                Types.SQL_TIMESTAMP,
                Arrays.asList(
                        Row.of(java.sql.Timestamp.valueOf("1969-12-31 23:59:59.999")),
                        Row.of(java.sql.Timestamp.valueOf("1970-01-01 00:00:00.000")),
                        Row.of(java.sql.Timestamp.valueOf("2026-08-28 12:34:56.123")),
                        Row.of(java.sql.Timestamp.valueOf("2030-01-01 00:00:00.000"))),
                "timestamp_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isZero();
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("TIMESTAMP precision 9 stays on Flink");
    }

    private static Stream<Arguments> nativeTimestampDataStreamRangeCases() {
        return Stream.of(
                Arguments.of(
                        "between",
                        "metric BETWEEN TIMESTAMP '1970-01-01 00:00:00.000' AND "
                                + "TIMESTAMP '2026-08-28 12:34:56.123'"),
                Arguments.of(
                        "in",
                        "metric IN (TIMESTAMP '1969-12-31 23:59:59.999', " + "TIMESTAMP '2030-01-01 00:00:00.000')"));
    }

    @ParameterizedTest(name = "VARCHAR {0}")
    @MethodSource("nativeVarcharDataStreamRangeCases")
    void nativeVarcharDataStreamRangesMatchFlinkByteForByte(String ignoredName, String predicate) throws Exception {
        assertDataStreamParity(
                "SELECT metric FROM varchar_input WHERE " + predicate,
                Types.STRING,
                Arrays.asList(Row.of("alpha"), Row.of("beta"), Row.of("delta"), Row.of("zeta"), Row.of((Object) null)),
                "varchar_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    private static Stream<Arguments> nativeVarcharDataStreamRangeCases() {
        return Stream.of(
                Arguments.of("between", "metric BETWEEN 'beta' AND 'delta'"),
                Arguments.of("in", "metric IN ('alpha', 'delta', 'zeta')"));
    }
}

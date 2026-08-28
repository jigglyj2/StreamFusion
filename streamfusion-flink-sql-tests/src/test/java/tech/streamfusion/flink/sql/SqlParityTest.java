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
    void unsupportedIntegerDivisionFallsBackAndMatchesFlinkByteForByte() throws Exception {
        assertParity("SELECT id / 2 FROM (VALUES (1), (2), (3)) AS input(id) WHERE id >= 1", true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isZero();
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

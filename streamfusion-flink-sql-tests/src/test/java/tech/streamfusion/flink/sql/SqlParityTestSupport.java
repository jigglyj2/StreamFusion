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
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

@ExtendWith(SharedMiniClusterExtension.class)
abstract class SqlParityTestSupport {
    protected static final String BATCH_SQL = "SELECT id, UPPER(name), amount * 2 "
            + "FROM (VALUES (2, 'beta', 2.50), (1, 'alpha', 1.25), "
            + "(3, CAST(NULL AS STRING), CAST(NULL AS DECIMAL(10, 2)))) "
            + "AS orders(id, name, amount) WHERE id >= 1 ORDER BY id";
    protected static final String STREAMING_CALC_SQL = "SELECT id, UPPER(name), amount * 2 "
            + "FROM (VALUES (1, 'alpha', 1.25), (2, 'beta', 2.50), "
            + "(3, CAST(NULL AS STRING), CAST(NULL AS DECIMAL(10, 2)))) "
            + "AS orders(id, name, amount) WHERE id >= 2";
    protected static final String STREAMING_AGGREGATE_SQL = "SELECT category, COUNT(*), SUM(amount) "
            + "FROM (VALUES ('a', 1), ('b', 2), ('a', 3)) AS orders(category, amount) "
            + "GROUP BY category";
    protected static final String IDENTITY_CALC_SQL =
            "SELECT id FROM (VALUES (1), (2), (3)) AS input(id) WHERE id >= 2";
    protected static final String MULTI_COLUMN_PROJECTION_SQL = "SELECT name, enabled, id "
            + "FROM (VALUES (1, 'one', TRUE), (2, 'two', FALSE)) "
            + "AS input(id, name, enabled) WHERE id >= 1";
    protected static final String FILTER_ON_UNPROJECTED_COLUMN_SQL =
            "SELECT name, id " + "FROM (VALUES (1, 'one'), (2, 'two')) AS input(id, name) WHERE id >= 2";
    protected static final String SCALAR_TYPE_PROJECTION_SQL = "SELECT tiny_value, small_value, big_value, "
            + "float_value, double_value, boolean_value, char_value, varchar_value, binary_value, "
            + "varbinary_value, decimal_value, date_value, time_value, timestamp_value FROM (VALUES (1, "
            + "CAST(2 AS TINYINT), CAST(3 AS SMALLINT), CAST(4 AS BIGINT), CAST(1.5 AS FLOAT), "
            + "CAST(2.5 AS DOUBLE), TRUE, CAST('abc' AS CHAR(3)), CAST('text' AS VARCHAR(8)), "
            + "CAST(X'0102' AS BINARY(2)), CAST(X'0304' AS VARBINARY(4)), CAST(12.34 AS DECIMAL(10, 2)), "
            + "DATE '2026-08-27', TIME '12:34:56.123', TIMESTAMP '2026-08-27 12:34:56.123')) "
            + "AS input(id, tiny_value, small_value, big_value, float_value, double_value, boolean_value, "
            + "char_value, varchar_value, binary_value, varbinary_value, decimal_value, date_value, "
            + "time_value, timestamp_value) WHERE id >= 1";
    protected static final String INTEGER_ARITHMETIC_SQL = "SELECT id + 10, id - 1, id * 3, "
            + "(id + 2) * (id - 1), 7 FROM (VALUES (1), (2), (3)) AS input(id) WHERE id >= 1";
    protected static final String BIGINT_ARITHMETIC_SQL = "SELECT id + 2147483648, id - 2147483648, "
            + "id * 2147483648, (id + 2147483648) * (id - 2147483648), 2147483648 "
            + "FROM (VALUES (2147483648), (2147483649), (2147483650)) AS input(id) "
            + "WHERE id >= 2147483648";

    @AfterEach
    protected void clearPlannerOverride() {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
    }

    protected static void assertParity(String sql, boolean streaming) throws Exception {
        assertParity(sql, streaming, true);
    }

    protected static void assertParity(String sql, boolean streaming, boolean accelerationExpected) throws Exception {
        byte[] flinkResult = execute(sql, streaming, false);
        byte[] streamFusionResult = execute(sql, streaming, true);

        assertThat(StreamFusionPlannerFactory.createdPlannerCount()).isEqualTo(1);
        assertThat(StreamFusionPlannerFactory.translatedPlanCount()).isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain())
                .withFailMessage(
                        "Unexpected StreamFusion acceleration outcome for SQL:%n%s%n%s",
                        sql, StreamFusionPlanningDiagnostics.explain())
                .contains(accelerationExpected ? "Accelerated: yes" : "Accelerated: no");
        assertThat(streamFusionResult).isEqualTo(flinkResult);
    }

    protected static void assertIntegerDataStreamParity(String sql) throws Exception {
        assertDataStreamParity(
                sql,
                Types.INT,
                Arrays.asList(Row.of(1), Row.of(2), Row.of(3), Row.of(4), Row.of((Object) null)),
                "integer_input");
    }

    protected static void assertDataStreamParity(String sql, TypeInformation<?> type, List<Row> rows, String tableName)
            throws Exception {
        assertDataStreamParity(sql, type, null, rows, tableName);
    }

    protected static void assertDataStreamParity(
            String sql, TypeInformation<?> type, DataType logicalType, List<Row> rows, String tableName)
            throws Exception {
        assertDataStreamParity(sql, type, logicalType, rows, tableName, true);
    }

    protected static void assertBatchDataStreamParity(
            String sql, TypeInformation<?> type, DataType logicalType, List<Row> rows, String tableName)
            throws Exception {
        assertDataStreamParity(sql, type, logicalType, rows, tableName, false, true);
    }

    protected static void assertFallbackDataStreamParity(
            String sql, TypeInformation<?> type, DataType logicalType, List<Row> rows, String tableName)
            throws Exception {
        assertDataStreamParity(sql, type, logicalType, rows, tableName, true, false);
    }

    protected static void assertFallbackDataStreamParity(
            String sql, TypeInformation<?> type, List<Row> rows, String tableName) throws Exception {
        assertFallbackDataStreamParity(sql, type, null, rows, tableName);
    }

    private static void assertDataStreamParity(
            String sql,
            TypeInformation<?> type,
            DataType logicalType,
            List<Row> rows,
            String tableName,
            boolean streaming)
            throws Exception {
        assertDataStreamParity(sql, type, logicalType, rows, tableName, streaming, true);
    }

    private static void assertDataStreamParity(
            String sql,
            TypeInformation<?> type,
            DataType logicalType,
            List<Row> rows,
            String tableName,
            boolean streaming,
            boolean accelerationExpected)
            throws Exception {
        byte[] flinkResult = executeDataStream(sql, type, logicalType, rows, tableName, false, streaming);
        byte[] streamFusionResult = executeDataStream(sql, type, logicalType, rows, tableName, true, streaming);

        assertThat(StreamFusionPlannerFactory.createdPlannerCount()).isEqualTo(1);
        assertThat(StreamFusionPlannerFactory.translatedPlanCount()).isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain())
                .withFailMessage(
                        "Unexpected StreamFusion acceleration outcome for SQL:%n%s%n%s",
                        sql, StreamFusionPlanningDiagnostics.explain())
                .contains(accelerationExpected ? "Accelerated: yes" : "Accelerated: no");
        assertThat(streamFusionResult).isEqualTo(flinkResult);
    }

    protected static byte[] executeDataStream(
            String sql, TypeInformation<?> type, List<Row> rows, String tableName, boolean streamFusionEnabled)
            throws Exception {
        return executeDataStream(sql, type, null, rows, tableName, streamFusionEnabled);
    }

    protected static byte[] executeDataStream(
            String sql,
            TypeInformation<?> type,
            DataType logicalType,
            List<Row> rows,
            String tableName,
            boolean streamFusionEnabled)
            throws Exception {
        return executeDataStream(sql, type, logicalType, rows, tableName, streamFusionEnabled, true);
    }

    private static byte[] executeDataStream(
            String sql,
            TypeInformation<?> type,
            DataType logicalType,
            List<Row> rows,
            String tableName,
            boolean streamFusionEnabled,
            boolean streaming)
            throws Exception {
        if (streamFusionEnabled) {
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        } else {
            System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            StreamFusionPlannerFactory.resetMetrics();
        }

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment.setRuntimeMode(streaming ? RuntimeExecutionMode.STREAMING : RuntimeExecutionMode.BATCH);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(
                executionEnvironment,
                streaming
                        ? EnvironmentSettings.newInstance().inStreamingMode().build()
                        : EnvironmentSettings.newInstance().inBatchMode().build());
        tableEnvironment.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        DataStream<Row> input =
                executionEnvironment.fromCollection(rows, Types.ROW_NAMED(new String[] {"metric"}, type));
        Table inputTable = logicalType == null
                ? tableEnvironment.fromDataStream(input)
                : tableEnvironment.fromDataStream(
                        input, Schema.newBuilder().column("metric", logicalType).build());
        tableEnvironment.createTemporaryView(tableName, inputTable);
        return collect(tableEnvironment.executeSql(sql));
    }

    protected static byte[] execute(String sql, boolean streaming, boolean streamFusionEnabled) throws Exception {
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

        return collect(tableEnvironment.executeSql(sql));
    }

    protected static byte[] collect(TableResult result) throws Exception {
        try (CloseableIterator<Row> rows = result.collect();
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream output = new DataOutputStream(bytes)) {
            List<byte[]> encodedRows = new ArrayList<>();
            while (rows.hasNext()) {
                Row resultRow = rows.next();
                byte[] row = (resultRow.getKind().shortString() + resultRow).getBytes(StandardCharsets.UTF_8);
                encodedRows.add(row);
            }
            encodedRows.sort(SqlParityTestSupport::compareUnsigned);
            for (byte[] row : encodedRows) {
                output.writeInt(row.length);
                output.write(row);
            }
            output.flush();
            return bytes.toByteArray();
        }
    }

    protected static int compareUnsigned(byte[] left, byte[] right) {
        return Arrays.compareUnsigned(left, right);
    }
}

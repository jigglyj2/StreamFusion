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

    @AfterEach
    void clearPlannerOverride() {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
    }

    @Test
    void acceleratedExecutionMatchesFlinkByteForByte() throws Exception {
        assertParity(BATCH_SQL, false);
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

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
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class SqlParityTest {
    private static final String SQL = "SELECT id, UPPER(name), amount * 2 "
            + "FROM (VALUES (2, 'beta', 2.50), (1, 'alpha', 1.25), "
            + "(3, CAST(NULL AS STRING), CAST(NULL AS DECIMAL(10, 2)))) "
            + "AS orders(id, name, amount) WHERE id >= 1 ORDER BY id";

    @AfterEach
    void clearPlannerOverride() {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
    }

    @Test
    void acceleratedExecutionMatchesFlinkByteForByte() throws Exception {
        byte[] flinkResult = execute(false);
        byte[] streamFusionResult = execute(true);

        assertThat(StreamFusionPlannerFactory.createdPlannerCount()).isEqualTo(1);
        assertThat(StreamFusionPlannerFactory.translatedPlanCount()).isGreaterThan(0);
        assertThat(streamFusionResult).isEqualTo(flinkResult);
    }

    private static byte[] execute(boolean streamFusionEnabled) throws Exception {
        if (streamFusionEnabled) {
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        } else {
            System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            StreamFusionPlannerFactory.resetMetrics();
        }

        TableEnvironment tableEnvironment = TableEnvironment.create(
                EnvironmentSettings.newInstance().inBatchMode().build());
        tableEnvironment.getConfig().getConfiguration().set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);

        TableResult result = tableEnvironment.executeSql(SQL);
        try (CloseableIterator<Row> rows = result.collect();
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream output = new DataOutputStream(bytes)) {
            while (rows.hasNext()) {
                byte[] row = rows.next().toString().getBytes(StandardCharsets.UTF_8);
                output.writeInt(row.length);
                output.write(row);
            }
            output.flush();
            return bytes.toByteArray();
        }
    }
}

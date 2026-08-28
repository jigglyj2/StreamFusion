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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class ArrayUnnestParityTest extends SqlParityTestSupport {
    private static final java.util.List<Row> INPUTS = Arrays.asList(
            Row.of((Object) new Integer[] {1, 2, 2}),
            Row.of((Object) new Integer[] {}),
            Row.of((Object) new Integer[] {null, -4}),
            Row.of((Object) null));

    @Test
    void nativeInnerArrayUnnestMatchesFlinkCardinalityNullsAndDuplicates() throws Exception {
        assertDataStreamParity(
                "SELECT item FROM array_unnest_input " + "CROSS JOIN UNNEST(metric) AS expanded(item)",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                INPUTS,
                "array_unnest_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isEqualTo(1);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void leftArrayUnnestFallsBackWithItsJoinSemanticReason() throws Exception {
        assertDataStreamParity(
                "SELECT item FROM array_left_unnest_input " + "LEFT JOIN UNNEST(metric) AS expanded(item) ON TRUE",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                INPUTS,
                "array_left_unnest_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isZero();
        assertThat(StreamFusionPlanningDiagnostics.explain())
                .contains("Accelerated: no")
                .contains("only inner/cross array UNNEST is supported");
    }

    @Test
    void nativeArrayUnnestPreservesEveryInputRowKindForEachElement() throws Exception {
        java.util.List<Row> changelog = Arrays.asList(
                Row.ofKind(RowKind.INSERT, (Object) new Integer[] {1, 2}),
                Row.ofKind(RowKind.UPDATE_BEFORE, (Object) new Integer[] {3}),
                Row.ofKind(RowKind.UPDATE_AFTER, (Object) new Integer[] {3, 4}),
                Row.ofKind(RowKind.DELETE, (Object) new Integer[] {5, 5}));

        byte[] flink = executeChangelog(changelog, false);
        byte[] streamFusion = executeChangelog(changelog, true);

        assertThat(streamFusion).isEqualTo(flink);
        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isEqualTo(1);
    }

    private static byte[] executeChangelog(java.util.List<Row> rows, boolean streamFusionEnabled) throws Exception {
        if (streamFusionEnabled) {
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        } else {
            System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            StreamFusionPlannerFactory.resetMetrics();
        }
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tables = StreamTableEnvironment.create(
                environment, EnvironmentSettings.newInstance().inStreamingMode().build());
        tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        DataStream<Row> input = environment.fromCollection(
                rows, Types.ROW_NAMED(new String[] {"metric"}, Types.OBJECT_ARRAY(Types.INT)));
        Table table = tables.fromChangelogStream(
                input,
                Schema.newBuilder()
                        .column("metric", DataTypes.ARRAY(DataTypes.INT()))
                        .build());
        tables.createTemporaryView("array_changelog_unnest_input", table);
        return collect(tables.executeSql(
                "SELECT item FROM array_changelog_unnest_input CROSS JOIN UNNEST(metric) AS expanded(item)"));
    }
}

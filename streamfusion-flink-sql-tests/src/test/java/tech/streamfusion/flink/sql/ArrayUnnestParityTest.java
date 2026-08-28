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

import java.math.BigDecimal;
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
    void nativeLeftArrayUnnestNullExtendsNullAndEmptyArrays() throws Exception {
        assertDataStreamParity(
                "SELECT item FROM array_left_unnest_input " + "LEFT JOIN UNNEST(metric) AS expanded(item) ON TRUE",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                INPUTS,
                "array_left_unnest_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isEqualTo(1);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
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

    @Test
    void nativeArrayUnnestSupportsVariableAndFixedWidthScalarBoundaries() throws Exception {
        assertDataStreamParity(
                "SELECT item FROM varchar_array_unnest_input " + "CROSS JOIN UNNEST(metric) AS expanded(item)",
                Types.OBJECT_ARRAY(Types.STRING),
                DataTypes.ARRAY(DataTypes.STRING()),
                Arrays.asList(
                        Row.of((Object) new String[] {"alpha", "你好", null}),
                        Row.of((Object) new String[] {}),
                        Row.of((Object) null)),
                "varchar_array_unnest_input");

        assertDataStreamParity(
                "SELECT item FROM decimal_array_unnest_input " + "CROSS JOIN UNNEST(metric) AS expanded(item)",
                Types.OBJECT_ARRAY(Types.BIG_DEC),
                DataTypes.ARRAY(DataTypes.DECIMAL(10, 2)),
                Arrays.asList(
                        Row.of((Object) new BigDecimal[] {new BigDecimal("12.34"), new BigDecimal("-0.01"), null}),
                        Row.of((Object) new BigDecimal[] {}),
                        Row.of((Object) null)),
                "decimal_array_unnest_input");

        assertDataStreamParity(
                "SELECT item FROM char_array_unnest_input " + "CROSS JOIN UNNEST(metric) AS expanded(item)",
                Types.OBJECT_ARRAY(Types.STRING),
                DataTypes.ARRAY(DataTypes.CHAR(4)),
                Arrays.asList(
                        Row.of((Object) new String[] {"abcd", "xy  ", null}),
                        Row.of((Object) new String[] {}),
                        Row.of((Object) null)),
                "char_array_unnest_input");

        assertDataStreamParity(
                "SELECT item FROM binary_array_unnest_input " + "CROSS JOIN UNNEST(metric) AS expanded(item)",
                Types.OBJECT_ARRAY(Types.PRIMITIVE_ARRAY(Types.BYTE)),
                DataTypes.ARRAY(DataTypes.BINARY(3)),
                Arrays.asList(
                        Row.of((Object) new byte[][] {new byte[] {0, 1, 2}, new byte[] {-1, 0, 1}, null}),
                        Row.of((Object) new byte[][] {}),
                        Row.of((Object) null)),
                "binary_array_unnest_input");
    }

    @Test
    void nativeArrayUnnestWithOrdinalityMatchesFlinkOneBasedPositions() throws Exception {
        assertDataStreamParity(
                "SELECT item, ord_idx FROM array_ordinality_input "
                        + "CROSS JOIN UNNEST(metric) WITH ORDINALITY AS expanded(item, ord_idx)",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                INPUTS,
                "array_ordinality_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isEqualTo(1);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void nativeLeftArrayUnnestWithOrdinalityNullExtendsBothFields() throws Exception {
        assertDataStreamParity(
                "SELECT item, ord_idx FROM array_left_ordinality_input "
                        + "LEFT JOIN UNNEST(metric) WITH ORDINALITY AS expanded(item, ord_idx) ON TRUE",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                INPUTS,
                "array_left_ordinality_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isEqualTo(1);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void nativeArrayOfRowsUnnestFlattensFieldsAndPreservesNullElements() throws Exception {
        assertDataStreamParity(
                "SELECT label, amount FROM row_array_unnest_input "
                        + "CROSS JOIN UNNEST(metric) AS expanded(label, amount)",
                Types.OBJECT_ARRAY(Types.ROW_NAMED(new String[] {"label", "amount"}, Types.STRING, Types.INT)),
                DataTypes.ARRAY(DataTypes.ROW(
                        DataTypes.FIELD("label", DataTypes.STRING()), DataTypes.FIELD("amount", DataTypes.INT()))),
                Arrays.asList(
                        Row.of((Object) new Row[] {Row.of("alpha", 1), Row.of("你好", null), null}),
                        Row.of((Object) new Row[] {}),
                        Row.of((Object) null)),
                "row_array_unnest_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isEqualTo(1);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void nativeLeftArrayOfRowsDistinguishesNullElementsFromSyntheticRows() throws Exception {
        java.util.List<Row> inputs = Arrays.asList(
                Row.of((Object) new Row[] {Row.of("alpha", 1), null, Row.of(null, 2)}),
                Row.of((Object) new Row[] {}),
                Row.of((Object) null));
        org.apache.flink.api.common.typeinfo.TypeInformation<Row[]> externalType =
                Types.OBJECT_ARRAY(Types.ROW_NAMED(new String[] {"label", "amount"}, Types.STRING, Types.INT));
        org.apache.flink.table.types.DataType logicalType = DataTypes.ARRAY(DataTypes.ROW(
                DataTypes.FIELD("label", DataTypes.STRING()), DataTypes.FIELD("amount", DataTypes.INT())));

        assertDataStreamParity(
                "SELECT label, amount FROM left_row_array_unnest_input "
                        + "LEFT JOIN UNNEST(metric) AS expanded(label, amount) ON TRUE",
                externalType,
                logicalType,
                inputs,
                "left_row_array_unnest_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isEqualTo(1);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void leftArrayOfRowsWithOrdinalityExplainsFlink23ParityFallback() {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tables = StreamTableEnvironment.create(environment);
        DataStream<Row> input = environment
                .fromData(Row.of((Object) new Row[] {Row.of("alpha", 1)}))
                .returns(Types.ROW_NAMED(
                        new String[] {"metric"},
                        Types.OBJECT_ARRAY(
                                Types.ROW_NAMED(new String[] {"label", "amount"}, Types.STRING, Types.INT))));
        Table table = tables.fromDataStream(
                input,
                Schema.newBuilder()
                        .column(
                                "metric",
                                DataTypes.ARRAY(DataTypes.ROW(
                                        DataTypes.FIELD("label", DataTypes.STRING()),
                                        DataTypes.FIELD("amount", DataTypes.INT()))))
                        .build());
        tables.createTemporaryView("left_row_array_ordinality_input", table);

        assertThat(tables.explainSql("SELECT label, amount, ord_idx FROM left_row_array_ordinality_input "
                        + "LEFT JOIN UNNEST(metric) WITH ORDINALITY "
                        + "AS expanded(label, amount, ord_idx) ON TRUE"))
                .contains("Accelerated: no")
                .contains("Flink 2.3 fails its output arity contract");
    }

    @Test
    void nativeArrayOfRowsPreservesScalarArrayFieldsWithoutFlatteningThem() throws Exception {
        java.util.List<Row> inputs = Arrays.asList(
                Row.of((Object) new Row[] {
                    Row.of("values", new Integer[] {1, null, 3}),
                    Row.of("empty", new Integer[] {}),
                    Row.of("null", null),
                    null
                }),
                Row.of((Object) new Row[] {}),
                Row.of((Object) null));
        org.apache.flink.api.common.typeinfo.TypeInformation<Row[]> externalType = Types.OBJECT_ARRAY(
                Types.ROW_NAMED(new String[] {"label", "values"}, Types.STRING, Types.OBJECT_ARRAY(Types.INT)));
        org.apache.flink.table.types.DataType logicalType = DataTypes.ARRAY(DataTypes.ROW(
                DataTypes.FIELD("label", DataTypes.STRING()),
                DataTypes.FIELD("values", DataTypes.ARRAY(DataTypes.INT()))));

        assertDataStreamParity(
                "SELECT label, nested_values FROM nested_row_array_unnest_input "
                        + "CROSS JOIN UNNEST(metric) AS expanded(label, nested_values)",
                externalType,
                logicalType,
                inputs,
                "nested_row_array_unnest_input");

        assertDataStreamParity(
                "SELECT label, nested_values FROM left_nested_row_array_unnest_input "
                        + "LEFT JOIN UNNEST(metric) AS expanded(label, nested_values) ON TRUE",
                externalType,
                logicalType,
                inputs,
                "left_nested_row_array_unnest_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isEqualTo(1);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void nativeNestedArrayUnnestPreservesEachInnerArrayAndOrdinality() throws Exception {
        java.util.List<Row> inputs = Arrays.asList(
                Row.of((Object) new Integer[][] {new Integer[] {1, null}, new Integer[] {}, null}),
                Row.of((Object) new Integer[][] {}),
                Row.of((Object) null));

        assertDataStreamParity(
                "SELECT item, ord_idx FROM nested_array_unnest_input "
                        + "CROSS JOIN UNNEST(metric) WITH ORDINALITY AS expanded(item, ord_idx)",
                Types.OBJECT_ARRAY(Types.OBJECT_ARRAY(Types.INT)),
                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())),
                inputs,
                "nested_array_unnest_input");

        assertDataStreamParity(
                "SELECT item, ord_idx FROM left_nested_array_unnest_input "
                        + "LEFT JOIN UNNEST(metric) WITH ORDINALITY AS expanded(item, ord_idx) ON TRUE",
                Types.OBJECT_ARRAY(Types.OBJECT_ARRAY(Types.INT)),
                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())),
                inputs,
                "left_nested_array_unnest_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isEqualTo(1);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
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

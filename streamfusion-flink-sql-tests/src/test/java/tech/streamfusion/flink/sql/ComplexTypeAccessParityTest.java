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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class ComplexTypeAccessParityTest extends SqlParityTestSupport {
    @Test
    void mapElementsMatchFlinkByteForByte() throws Exception {
        Map<String, Integer> first = new LinkedHashMap<>();
        first.put("present", 7);
        first.put("null_value", null);
        Map<String, Integer> second = new LinkedHashMap<>();
        second.put("present", 9);

        assertDataStreamParity(
                "SELECT metric['present'], metric['null_value'], metric['missing'] FROM map_input",
                Types.MAP(Types.STRING, Types.INT),
                DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.INT()),
                Arrays.asList(Row.of(first), Row.of(second), Row.of(new LinkedHashMap<>()), Row.of((Object) null)),
                "map_input");

        assertNativeCalcRan();
    }

    @Test
    void arrayElementsMatchFlinkByteForByte() throws Exception {
        assertDataStreamParity(
                "SELECT metric[1], metric[2], metric[4] FROM array_input",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                Arrays.asList(
                        Row.of((Object) new Integer[] {1, null, 3}),
                        Row.of((Object) new Integer[] {4}),
                        Row.of((Object) new Integer[] {}),
                        Row.of((Object) null)),
                "array_input");

        assertNativeCalcRan();
    }

    @Test
    void rowFieldsInsideArraysMatchFlinkByteForByte() throws Exception {
        TypeInformation<Row> rowType = Types.ROW_NAMED(new String[] {"label", "amount"}, Types.STRING, Types.INT);
        assertDataStreamParity(
                "SELECT metric[1].label, metric[2].amount FROM array_of_rows_input",
                Types.OBJECT_ARRAY(rowType),
                DataTypes.ARRAY(DataTypes.ROW(
                        DataTypes.FIELD("label", DataTypes.STRING()), DataTypes.FIELD("amount", DataTypes.INT()))),
                Arrays.asList(
                        Row.of((Object) new Row[] {Row.of("x", 7), Row.of("y", null)}),
                        Row.of((Object) new Row[] {Row.of("z", 9)}),
                        Row.of((Object) new Row[] {}),
                        Row.of((Object) null)),
                "array_of_rows_input");

        assertNativeCalcRan();
    }

    @Test
    void rowFieldsMatchFlinkByteForByte() throws Exception {
        assertDataStreamParity(
                "SELECT metric.label, metric.amount FROM row_input",
                Types.ROW_NAMED(new String[] {"label", "amount"}, Types.STRING, Types.INT),
                DataTypes.ROW(DataTypes.FIELD("label", DataTypes.STRING()), DataTypes.FIELD("amount", DataTypes.INT())),
                Arrays.asList(Row.of(Row.of("x", 7)), Row.of(Row.of("y", null)), Row.of((Object) null)),
                "row_input");

        assertNativeCalcRan();
    }

    @Test
    void nestedRowFieldsAndPredicatesMatchFlinkByteForByte() throws Exception {
        TypeInformation<Row> innerType = Types.ROW_NAMED(new String[] {"quantity", "note"}, Types.INT, Types.STRING);
        TypeInformation<Row> metricType =
                Types.ROW_NAMED(new String[] {"outer_label", "inner"}, Types.STRING, innerType);
        DataType logicalType = DataTypes.ROW(
                DataTypes.FIELD("outer_label", DataTypes.STRING()),
                DataTypes.FIELD(
                        "inner",
                        DataTypes.ROW(
                                DataTypes.FIELD("quantity", DataTypes.INT()),
                                DataTypes.FIELD("note", DataTypes.STRING()))));
        List<Row> rows = Arrays.asList(
                Row.of(Row.of("a", Row.of(1, "low"))),
                Row.of(Row.of("b", Row.of(3, null))),
                Row.of(Row.of("c", null)),
                Row.of((Object) null));

        assertDataStreamParity(
                "SELECT metric.outer_label, metric.`inner`.note FROM nested_row_input "
                        + "WHERE metric.`inner`.quantity >= 2",
                metricType,
                logicalType,
                rows,
                "nested_row_input");

        assertNativeCalcRan();
    }

    private static void assertNativeCalcRan() {
        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }
}

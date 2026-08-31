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
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class CollectionFunctionParityTest extends SqlParityTestSupport {
    @Test
    void arrayContainsMatchesFlinkByteForByteInProjectionAndFilter() throws Exception {
        java.util.List<Row> rows = Arrays.asList(
                Row.of((Object) new Integer[] {1, 2, 3}),
                Row.of((Object) new Integer[] {1, null, 3}),
                Row.of((Object) new Integer[] {}),
                Row.of((Object) null));

        assertDataStreamParity(
                "SELECT ARRAY_CONTAINS(metric, 2), ARRAY_CONTAINS(metric, 42) FROM array_input",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                rows,
                "array_input");
        assertNativeCalcRan();

        assertDataStreamParity(
                "SELECT metric FROM filtered_array_input WHERE ARRAY_CONTAINS(metric, 3)",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                rows,
                "filtered_array_input");
        assertNativeCalcRan();
    }

    @Test
    void nullableArrayContainsNeedleUsesFlinkNullSearchSemanticsNatively() throws Exception {
        assertDataStreamParity(
                "SELECT ARRAY_CONTAINS(metric, CAST(NULL AS INT)) FROM nullable_needle_input",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                Arrays.asList(
                        Row.of((Object) new Integer[] {1, null, 3}),
                        Row.of((Object) new Integer[] {1, 2, 3}),
                        Row.of((Object) null)),
                "nullable_needle_input");

        assertNativeCalcRan();
    }

    @Test
    void perRowNullableArrayContainsNeedleUsesBothNativeBranches() throws Exception {
        TypeInformation<Row> metricType =
                Types.ROW_NAMED(new String[] {"items", "needle"}, Types.OBJECT_ARRAY(Types.INT), Types.INT);
        DataType logicalType = DataTypes.ROW(
                DataTypes.FIELD("items", DataTypes.ARRAY(DataTypes.INT())), DataTypes.FIELD("needle", DataTypes.INT()));

        assertDataStreamParity(
                "SELECT ARRAY_CONTAINS(metric.items, metric.needle) FROM dynamic_array_contains_input",
                metricType,
                logicalType,
                Arrays.asList(
                        Row.of(Row.of(new Integer[] {1, null, 3}, 3)),
                        Row.of(Row.of(new Integer[] {1, null, 3}, null)),
                        Row.of(Row.of(new Integer[] {1, 2, 3}, null)),
                        Row.of(Row.of(null, 1)),
                        Row.of((Object) null)),
                "dynamic_array_contains_input");

        assertNativeCalcRan();
    }

    @Test
    void arrayCardinalityMatchesFlinkByteForByte() throws Exception {
        assertDataStreamParity(
                "SELECT CARDINALITY(metric) FROM array_input",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                Arrays.asList(
                        Row.of((Object) new Integer[] {1, null, 3}), Row.of((Object) new Integer[] {}), Row.of((Object)
                                null)),
                "array_input");

        assertNativeCalcRan();
    }

    @Test
    void mapCardinalityMatchesFlinkByteForByte() throws Exception {
        Map<String, Integer> populated = new LinkedHashMap<>();
        populated.put("a", 1);
        populated.put("b", null);

        assertDataStreamParity(
                "SELECT CARDINALITY(metric) FROM map_input",
                Types.MAP(Types.STRING, Types.INT),
                DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.INT()),
                Arrays.asList(Row.of(populated), Row.of(new LinkedHashMap<>()), Row.of((Object) null)),
                "map_input");

        assertNativeCalcRan();
    }

    @Test
    void nestedArrayCardinalityCountsOnlyTheOuterArray() throws Exception {
        assertDataStreamParity(
                "SELECT CARDINALITY(metric) FROM nested_array_input",
                Types.OBJECT_ARRAY(Types.OBJECT_ARRAY(Types.INT)),
                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())),
                Arrays.asList(
                        Row.of((Object) new Integer[][] {{1, 2}, {3}}),
                        Row.of((Object) new Integer[][] {}),
                        Row.of((Object) null)),
                "nested_array_input");

        assertNativeCalcRan();
    }

    private static void assertNativeCalcRan() {
        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }
}

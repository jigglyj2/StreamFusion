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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class CollectionTransformationParityTest extends SqlParityTestSupport {
    @Test
    void arrayConcatMatchesFlinkAndComposesWithNativeArrays() throws Exception {
        assertDataStreamParity(
                "SELECT ARRAY_CONCAT(metric, ARRAY_REVERSE(metric), metric) FROM array_input",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                Arrays.asList(
                        Row.of((Object) new Integer[] {1, null, 3}), Row.of((Object) new Integer[] {}), Row.of((Object)
                                null)),
                "array_input");

        assertNativeCalcRan();
    }

    @Test
    void arrayConcatMatchesFlinkForNestedRows() throws Exception {
        assertDataStreamParity(
                "SELECT ARRAY_CONCAT(metric, ARRAY_REVERSE(metric)) FROM row_array_input",
                Types.OBJECT_ARRAY(Types.ROW_NAMED(new String[] {"label", "amount"}, Types.STRING, Types.INT)),
                DataTypes.ARRAY(DataTypes.ROW(
                        DataTypes.FIELD("label", DataTypes.STRING()), DataTypes.FIELD("amount", DataTypes.INT()))),
                Arrays.asList(
                        Row.of((Object) new Row[] {Row.of("a", 1), null, Row.of("b", null)}),
                        Row.of((Object) new Row[] {}),
                        Row.of((Object) null)),
                "row_array_input");

        assertNativeCalcRan();
    }

    @Test
    void arrayAppendAndPrependMatchFlinkForPrimitiveElements() throws Exception {
        assertDataStreamParity(
                "SELECT ARRAY_APPEND(metric, 9), ARRAY_APPEND(metric, CAST(NULL AS INT)), "
                        + "ARRAY_PREPEND(metric, 0), ARRAY_PREPEND(metric, CAST(NULL AS INT)) FROM array_input",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                Arrays.asList(
                        Row.of((Object) new Integer[] {1, 2, null}), Row.of((Object) new Integer[] {}), Row.of((Object)
                                null)),
                "array_input");

        assertNativeCalcRan();
    }

    @Test
    void arrayAppendAndPrependComposeWithNestedRowAccess() throws Exception {
        assertDataStreamParity(
                "SELECT ARRAY_APPEND(metric, metric[1]), ARRAY_PREPEND(metric, metric[2]) " + "FROM row_array_input",
                Types.OBJECT_ARRAY(Types.ROW_NAMED(new String[] {"label", "amount"}, Types.STRING, Types.INT)),
                DataTypes.ARRAY(DataTypes.ROW(
                        DataTypes.FIELD("label", DataTypes.STRING()), DataTypes.FIELD("amount", DataTypes.INT()))),
                Arrays.asList(
                        Row.of((Object) new Row[] {Row.of("a", 1), Row.of("b", null)}),
                        Row.of((Object) new Row[] {Row.of("c", 3)}),
                        Row.of((Object) new Row[] {}),
                        Row.of((Object) null)),
                "row_array_input");

        assertNativeCalcRan();
    }

    @Test
    void arrayReverseMatchesFlinkForPrimitiveElements() throws Exception {
        assertDataStreamParity(
                "SELECT ARRAY_REVERSE(metric) FROM array_input",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                Arrays.asList(
                        Row.of((Object) new Integer[] {1, 2, null, 4}),
                        Row.of((Object) new Integer[] {}),
                        Row.of((Object) null)),
                "array_input");

        assertNativeCalcRan();
    }

    @Test
    void arrayReverseMatchesFlinkForRowElements() throws Exception {
        assertDataStreamParity(
                "SELECT ARRAY_REVERSE(metric) FROM row_array_input",
                Types.OBJECT_ARRAY(Types.ROW_NAMED(new String[] {"label", "amount"}, Types.STRING, Types.INT)),
                DataTypes.ARRAY(DataTypes.ROW(
                        DataTypes.FIELD("label", DataTypes.STRING()), DataTypes.FIELD("amount", DataTypes.INT()))),
                Arrays.asList(
                        Row.of((Object) new Row[] {Row.of("a", 1), null, Row.of("b", null)}),
                        Row.of((Object) new Row[] {}),
                        Row.of((Object) null)),
                "row_array_input");

        assertNativeCalcRan();
    }

    private static void assertNativeCalcRan() {
        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }
}

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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class ComplexTypeProjectionParityTest extends SqlParityTestSupport {
    @Test
    void nativeArrayReferencesMatchFlinkByteForByte() throws Exception {
        List<Row> rows = Arrays.asList(
                Row.of((Object) new Integer[] {1, null, 3}), Row.of((Object) new Integer[] {4, 5}), Row.of((Object)
                        null));

        assertDataStreamParity(
                "SELECT metric FROM array_input WHERE metric IS NOT NULL",
                Types.OBJECT_ARRAY(Types.INT),
                DataTypes.ARRAY(DataTypes.INT()),
                rows,
                "array_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    @Test
    void nativeMapReferencesMatchFlinkByteForByte() throws Exception {
        Map<String, Integer> first = new LinkedHashMap<>();
        first.put("a", 1);
        first.put("b", null);
        Map<String, Integer> second = new LinkedHashMap<>();
        second.put("c", 2);

        assertDataStreamParity(
                "SELECT metric FROM map_input WHERE metric IS NOT NULL",
                Types.MAP(Types.STRING, Types.INT),
                DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.INT()),
                Arrays.asList(Row.of(first), Row.of(second), Row.of((Object) null)),
                "map_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @Test
    void nativeNestedRowReferencesMatchFlinkByteForByte() throws Exception {
        assertDataStreamParity(
                "SELECT metric FROM row_input WHERE metric IS NOT NULL",
                Types.ROW_NAMED(new String[] {"label", "value"}, Types.STRING, Types.INT),
                DataTypes.ROW(DataTypes.FIELD("label", DataTypes.STRING()), DataTypes.FIELD("value", DataTypes.INT())),
                Arrays.asList(Row.of(Row.of("x", 7)), Row.of(Row.of("y", null)), Row.of((Object) null)),
                "row_input");

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }
}

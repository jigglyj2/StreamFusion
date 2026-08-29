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

import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class PlanningParityTest extends SqlParityTestSupport {
    @Test
    void explainStatesWhenEveryExpressionIsAccelerated() {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        tableEnvironment.createTemporaryView(
                "supported_explain_input", environment.fromData(Row.of("Alpha")).returns(Types.ROW(Types.STRING)));

        assertThat(tableEnvironment.explainSql(
                        "SELECT UPPER(f0) FROM supported_explain_input WHERE LOWER(f0) = 'alpha'"))
                .contains("== StreamFusion Acceleration ==")
                .contains("Accelerated: yes")
                .contains("every internal node and expression has a StreamFusion implementation");
    }

    @Test
    void explainIncludesUnsupportedExpressionPath() {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        tableEnvironment.createTemporaryView(
                "explain_input", environment.fromData(Row.of(" a ")).returns(Types.ROW(Types.STRING)));

        assertThat(tableEnvironment.explainSql("SELECT TRIM(CAST(f0 AS CHAR(3))) FROM explain_input"))
                .contains("== StreamFusion Acceleration ==")
                .contains("Accelerated: no")
                .contains("projection[0]/TRIM")
                .contains("the entire plan will use Flink");
    }

    @Test
    void nestedCalcExpressionsMatchFlinkByteForByte() throws Exception {
        String sql = "SELECT doubled + 1 FROM ("
                + "SELECT (id + 10) * 2 AS doubled FROM "
                + "(VALUES (1), (2), (3)) AS input(id) WHERE id >= 2"
                + ") WHERE doubled < 26";

        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @Test
    void nestedTrimExpressionAccelerates() throws Exception {
        String sql = "SELECT UPPER(trimmed_name) FROM ("
                + "SELECT TRIM(name) AS trimmed_name FROM "
                + "(VALUES (' a '), ('b')) AS input(name)"
                + ") WHERE trimmed_name = 'A'";

        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
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
}

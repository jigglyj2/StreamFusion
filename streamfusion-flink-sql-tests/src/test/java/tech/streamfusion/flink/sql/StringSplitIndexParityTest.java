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
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class StringSplitIndexParityTest extends SqlParityTestSupport {
    private static final String INPUT = "(VALUES "
            + "('a::b::c', 0), ('a::b::c', 1), ('a::b::c', 2), ('a::b::c', 3), "
            + "('::a::::b::', 2), ('😀::文字', 1), ('abc', 0), ('', 0), "
            + "('abc', -1), (CAST(NULL AS STRING), 0), ('abc', CAST(NULL AS INT))) "
            + "input(text_value, index_value)";

    @ParameterizedTest
    @MethodSource("queries")
    void literalDelimiterMatchesFlinkByteForByte(String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    private static Stream<String> queries() {
        return Stream.of(
                "SELECT SPLIT_INDEX(text_value, '::', index_value) FROM " + INPUT,
                "SELECT text_value FROM " + INPUT + " WHERE SPLIT_INDEX(text_value, '::', index_value) = 'b'",
                "SELECT SPLIT_INDEX(UPPER(text_value), '::', index_value + 0) FROM " + INPUT);
    }

    @Test
    void emptyDelimiterHasAnExplainReason() {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        tableEnvironment.createTemporaryView(
                "split_index_explain_input", environment.fromData(Row.of("a b")).returns(Types.ROW(Types.STRING)));

        assertThat(tableEnvironment.explainSql("SELECT SPLIT_INDEX(f0, '', 0) FROM split_index_explain_input"))
                .contains("Accelerated: no")
                .contains("Java whitespace splitting")
                .contains("the entire plan will use Flink");
    }
}

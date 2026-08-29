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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class StringEltParityTest extends SqlParityTestSupport {
    private static final String INPUT = "(VALUES "
            + "(1, 'first', 'second'), (2, 'left', 'right'), "
            + "(0, 'zero', 'unused'), (-1, 'negative', 'unused'), "
            + "(3, 'past', 'end'), (CAST(NULL AS INT), 'null-index', 'unused'), "
            + "(1, CAST(NULL AS STRING), 'nullable'), "
            + "(2, '😀', '文字')) input(index_value, option_a, option_b)";

    @ParameterizedTest
    @MethodSource("queries")
    void dynamicSelectionMatchesFlinkByteForByte(String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount())
                .withFailMessage(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
    }

    private static Stream<String> queries() {
        return Stream.of(
                "SELECT ELT(index_value, option_a, option_b) FROM " + INPUT,
                "SELECT option_a FROM " + INPUT + " WHERE ELT(index_value, option_a, option_b) = 'right'",
                "SELECT ELT(index_value, UPPER(option_a), option_b, '') FROM " + INPUT);
    }

    @Test
    void brokenFlinkIndexWidthsHaveAnExplainReason() {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        StreamTableEnvironment tableEnvironment =
                StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());

        assertThat(tableEnvironment.explainSql(
                        "SELECT ELT(CAST(1 AS BIGINT), CAST('a' AS STRING), CAST('b' AS STRING))"))
                .contains("Accelerated: no")
                .contains("Flink 2.3 throws ClassCastException")
                .contains("the entire plan will use Flink");
    }
}

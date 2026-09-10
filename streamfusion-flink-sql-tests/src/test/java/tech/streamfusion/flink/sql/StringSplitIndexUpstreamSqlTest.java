/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file to You under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

/** String-delimiter cases from Flink ScalarFunctionsTest at c0f8d1a1 (testSplitIndex). */
class StringSplitIndexUpstreamSqlTest extends SqlParityTestSupport {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void upstreamValuesAndUnsupportedDelimiterModes(boolean enabled) throws Exception {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
        if (enabled)
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        var environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        var tables = StreamTableEnvironment.create(environment);
        tables.createTemporaryView(
                "split_input",
                environment
                        .fromData(Row.of("AQID", "Test", null))
                        .returns(Types.ROW(Types.STRING, Types.STRING, Types.STRING)));
        var result = tables.executeSql(
                "SELECT SPLIT_INDEX(f0, 'I', 0), SPLIT_INDEX(f0, 'I', 2), SPLIT_INDEX(f0, 'I', -1), SPLIT_INDEX(f0, 'I', CAST(NULL AS INT)), SPLIT_INDEX(f2, 'I', 0), SPLIT_INDEX(f1, 'e', 1) FROM split_input");
        try (var rows = result.collect()) {
            assertThat(rows.next()).isEqualTo(Row.of("AQ", null, null, null, null, "st"));
            assertThat(rows.hasNext()).isFalse();
        }
        if (enabled)
            assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
        StreamFusionPlannerFactory.resetMetrics();
        var fallback = tables.executeSql(
                "SELECT SPLIT_INDEX(f0, 73, 0), SPLIT_INDEX(f0, 256, 0), SPLIT_INDEX(f0, 0, 0), SPLIT_INDEX(f0, CAST(NULL AS VARCHAR), 0) FROM split_input");
        try (var rows = fallback.collect()) {
            assertThat(rows.next()).isEqualTo(Row.of("AQ", null, null, null));
            assertThat(rows.hasNext()).isFalse();
        }
        if (enabled)
            assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
    }
}

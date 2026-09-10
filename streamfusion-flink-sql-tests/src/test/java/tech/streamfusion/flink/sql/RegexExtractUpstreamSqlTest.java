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

/** Two upstream return-type cases from RegexpFunctionsITCase at c0f8d1a1, plus concat/error coverage. */
class RegexExtractUpstreamSqlTest extends SqlParityTestSupport {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void upstreamValuesReturnTypesAndInvalidPatternFallback(boolean enabled) throws Exception {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
        if (enabled)
            System.setProperty(
                    StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        var environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        var tables = StreamTableEnvironment.create(environment);
        tables.createTemporaryView(
                "regex_input",
                environment
                        .fromData(Row.of("22", "ABC", "("))
                        .returns(Types.ROW(Types.STRING, Types.STRING, Types.STRING)));
        var result = tables.executeSql(
                "SELECT REGEXP_EXTRACT(f0, '[A-Z]+'), REGEXP_EXTRACT(f1, '[A-Z]+'), REGEXP_EXTRACT(f1, '[A-' || 'Z]+') FROM regex_input");
        assertThat(result.getResolvedSchema().getColumnDataTypes()).allSatisfy(type -> {
            assertThat(type.getLogicalType().isNullable()).isTrue();
            assertThat(type.getLogicalType().getTypeRoot())
                    .isEqualTo(org.apache.flink.table.types.logical.LogicalTypeRoot.VARCHAR);
        });
        try (var rows = result.collect()) {
            assertThat(rows.next()).isEqualTo(Row.of(null, "ABC", "ABC"));
            assertThat(rows.hasNext()).isFalse();
        }
        if (enabled)
            assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
        // Pinned Flink 2.3 returns NULL for invalid patterns, including literals. Newer
        // upstream revisions reject literals during planning; preserve the pinned runtime.
        StreamFusionPlannerFactory.resetMetrics();
        var invalid = tables.executeSql("SELECT REGEXP_EXTRACT(f1, '(') FROM regex_input");
        try (var rows = invalid.collect()) {
            assertThat(rows.next()).isEqualTo(Row.of((Object) null));
            assertThat(rows.hasNext()).isFalse();
        }
        if (enabled)
            assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
        var dynamic = tables.executeSql("SELECT REGEXP_EXTRACT(f1, f2) FROM regex_input");
        try (var rows = dynamic.collect()) {
            assertThat(rows.next()).isEqualTo(Row.of((Object) null));
            assertThat(rows.hasNext()).isFalse();
        }
    }
}

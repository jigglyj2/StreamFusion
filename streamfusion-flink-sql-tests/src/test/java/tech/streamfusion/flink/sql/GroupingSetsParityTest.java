/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

class GroupingSetsParityTest extends SqlParityTestSupport {
    @ParameterizedTest(name = "{0}")
    @MethodSource("queries")
    void expandFormsFallBackAsAWholeAndMatchFlinkByteForByte(String ignoredName, String sql) throws Exception {
        assertParity(sql, true);

        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isZero();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("queries")
    void explainAttributesFallbackToTheUnsupportedAggregate(String ignoredName, String sql) {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        StreamTableEnvironment tableEnvironment =
                StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());

        assertThat(tableEnvironment.explainSql(sql))
                .contains("== StreamFusion Acceleration ==")
                .contains("Accelerated: no")
                .contains("StreamExecGroupAggregate")
                .contains("does not yet implement expanded grouping-set semantics")
                .doesNotContain("StreamExecExpand: operator has no StreamFusion physical implementation");
    }

    private static Stream<Arguments> queries() {
        String input = "(VALUES ('a', 'east', 1), ('b', 'west', 2), ('a', 'west', 3)) "
                + "AS input(category, region, amount) ";
        return Stream.of(
                Arguments.of(
                        "grouping sets",
                        "SELECT category, region, SUM(amount) FROM " + input
                                + "GROUP BY GROUPING SETS ((category), (region), ())"),
                Arguments.of(
                        "rollup",
                        "SELECT category, region, SUM(amount) FROM " + input + "GROUP BY ROLLUP (category, region)"),
                Arguments.of(
                        "cube",
                        "SELECT category, region, SUM(amount) FROM " + input + "GROUP BY CUBE (category, region)"));
    }
}

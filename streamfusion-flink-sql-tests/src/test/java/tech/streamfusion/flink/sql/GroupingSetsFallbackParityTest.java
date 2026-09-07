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

class GroupingSetsFallbackParityTest extends SqlParityTestSupport {
    @ParameterizedTest(name = "{0}")
    @MethodSource("queries")
    void expandFormsRunWithFallbackAndMatchFlinkByteForByte(String ignoredName, String sql) throws Exception {
        assertFallbackParity(sql, true);

        SqlFallbackAssertions.nativeBatchesAreZero(StreamFusionPlannerFactory.nativeGroupAggregateBatchCount());
        SqlFallbackAssertions.admission();
    }

    @ParameterizedTest(name = "bounded {0}")
    @MethodSource("queries")
    void boundedExpandFormsRunWithFallbackAndMatchFlinkByteForByte(String ignoredName, String sql) throws Exception {
        assertFallbackParity(sql, false);

        SqlFallbackAssertions.admission();
        SqlFallbackAssertions.nativeBatchesAreZero(StreamFusionPlannerFactory.nativeGroupAggregateBatchCount());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("queries")
    void explainReportsWholePlanFallbackForGatedAggregate(String ignoredName, String sql) {
        System.setProperty(
                StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, StreamFusionPlannerFactory.class.getName());
        StreamTableEnvironment tableEnvironment =
                StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());

        assertThat(tableEnvironment.explainSql(sql))
                .contains("== StreamFusion Acceleration ==")
                .contains("Accelerated: no")
                .contains("native persistent state is temporarily disabled")
                .doesNotContain("StreamFusionExpand", "StreamFusionGroupAggregate");
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
                        "SELECT category, region, SUM(amount) FROM " + input + "GROUP BY CUBE (category, region)"),
                Arguments.of(
                        "nullable scalar key widening",
                        "SELECT binary_value, decimal_value, date_value, timestamp_value, COUNT(*) FROM "
                                + "(VALUES "
                                + "(CAST(X'0102' AS BINARY(2)), CAST(12.34 AS DECIMAL(10, 2)), "
                                + "DATE '2026-09-03', TIMESTAMP '2026-09-03 12:00:00'), "
                                + "(CAST(X'0102' AS BINARY(2)), CAST(12.34 AS DECIMAL(10, 2)), "
                                + "DATE '2026-09-03', TIMESTAMP '2026-09-03 12:00:00')) "
                                + "AS input(binary_value, decimal_value, date_value, timestamp_value) "
                                + "GROUP BY GROUPING SETS "
                                + "((binary_value, decimal_value, date_value, timestamp_value), ())"),
                Arguments.of(
                        "nullable nested key widening",
                        "SELECT array_value, row_value, COUNT(*) FROM "
                                + "(VALUES (ARRAY[1, 2], ROW('alpha', 7)), "
                                + "(ARRAY[1, 2], ROW('alpha', 7))) AS input(array_value, row_value) "
                                + "GROUP BY GROUPING SETS ((array_value, row_value), ())"));
    }
}

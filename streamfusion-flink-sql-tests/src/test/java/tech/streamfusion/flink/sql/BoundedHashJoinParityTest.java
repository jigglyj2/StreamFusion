/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class BoundedHashJoinParityTest extends SqlParityTestSupport {
    @ParameterizedTest(name = "{0}")
    @MethodSource("queries")
    void boundedHashJoinMatchesFlinkByteForByte(String ignoredName, String query) throws Exception {
        assertParity(query, false);

        assertThat(StreamFusionPlannerFactory.nativeRegularJoinBatchCount())
                .as(StreamFusionPlanningDiagnostics.explain())
                .isGreaterThan(0);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    private static Stream<Arguments> queries() {
        String left = "(VALUES "
                + "(1, 'left-1', ARRAY[1, 2], CAST(12.34 AS DECIMAL(10, 2))), "
                + "(1, 'left-2', ARRAY[3], CAST(23.45 AS DECIMAL(10, 2))), "
                + "(2, 'left-3', ARRAY[4, 5], CAST(34.56 AS DECIMAL(10, 2))), "
                + "(CAST(NULL AS INT), 'left-null', ARRAY[6], CAST(45.67 AS DECIMAL(10, 2)))) "
                + "AS l(id, payload, items, amount)";
        String right = "(VALUES "
                + "(1, 'right-1', MAP['a', 1]), "
                + "(3, 'right-3', MAP['b', 2]), "
                + "(CAST(NULL AS INT), 'right-null', MAP['c', 3])) "
                + "AS r(id, payload, attributes)";
        return Stream.of(
                Arguments.of(
                        "inner with duplicate and nested payloads",
                        "SELECT l.id, l.payload, l.items, l.amount, r.payload, r.attributes FROM " + left + " JOIN "
                                + right + " ON l.id = r.id"),
                Arguments.of(
                        "left outer",
                        "SELECT l.id, l.payload, r.payload FROM " + left + " LEFT JOIN " + right + " ON l.id = r.id"),
                Arguments.of(
                        "right outer",
                        "SELECT l.id, l.payload, r.id, r.payload FROM " + left + " RIGHT JOIN " + right
                                + " ON l.id = r.id"),
                Arguments.of(
                        "full outer",
                        "SELECT l.id, l.payload, r.id, r.payload FROM " + left + " FULL OUTER JOIN " + right
                                + " ON l.id = r.id"),
                Arguments.of(
                        "semi",
                        "SELECT l.id, l.payload, l.items FROM " + left + " WHERE l.id IN (SELECT r.id FROM " + right
                                + ")"),
                Arguments.of(
                        "anti",
                        "SELECT l.id, l.payload, l.items FROM " + left + " WHERE l.id NOT IN (SELECT r.id FROM " + right
                                + ")"),
                Arguments.of(
                        "residual predicate",
                        "SELECT l.id, l.payload, r.payload FROM " + left + " JOIN " + right
                                + " ON l.id = r.id AND l.amount > 20 AND r.id > 0"));
    }
}

/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class SetOperationsParityTest extends SqlParityTestSupport {
    @ParameterizedTest(name = "{0}")
    @MethodSource("setOperationTypes")
    void everyFlinkSetKeyTypeMatchesByteForByte(
            String description, TypeInformation<?> type, DataType dataType, Object first, Object second)
            throws Exception {
        assertDataStreamParity(
                "SELECT metric FROM set_type_input INTERSECT SELECT metric FROM set_type_input",
                type,
                dataType,
                List.of(Row.of(first), Row.of(first), Row.of(second), Row.of((Object) null)),
                "set_type_input");
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void intersectAllReplicatesNestedArrowValues() throws Exception {
        String row =
                "(ARRAY[1, CAST(NULL AS INT)], " + "MAP['first', 1, 'nullable', CAST(NULL AS INT)], ROW('nested', 7))";
        String sql = "SELECT * FROM (VALUES "
                + row
                + ") AS left_input(array_value, map_value, row_value) INTERSECT ALL "
                + "SELECT * FROM (VALUES "
                + row
                + ") AS right_input(array_value, map_value, row_value)";

        assertParity(sql, true);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(tech.streamfusion.flink.StreamFusionPlannerFactory.nativeCalcBatchCount())
                .isGreaterThan(0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL"})
    void setOperationMatchesFlinkForNulls(String operation) throws Exception {
        // Matching rows in streaming EXCEPT and duplicate count changes in ALL operations can have
        // multiple valid intermediate changelogs depending on two-input scheduling. Their ordered
        // transitions are covered below the SQL layer; keep this generated whole-plan comparison
        // byte-deterministic while still exercising null keys and all four physical rewrites.
        boolean except = operation.startsWith("EXCEPT");
        String rightValues = except ? "(3), (4)" : "(1), (3), (CAST(NULL AS INT))";
        String sql = "SELECT v FROM (VALUES (1), (2), (CAST(NULL AS INT))) AS left_input(v) "
                + operation
                + " SELECT v FROM (VALUES "
                + rightValues
                + ") "
                + "AS right_input(v)";

        assertParity(sql, true);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(tech.streamfusion.flink.StreamFusionPlannerFactory.nativeGroupAggregateBatchCount())
                .isGreaterThan(0);
        if (operation.endsWith("ALL")) {
            assertThat(tech.streamfusion.flink.StreamFusionPlannerFactory.nativeCalcBatchCount())
                    .isGreaterThan(0);
        } else {
            assertThat(tech.streamfusion.flink.StreamFusionPlannerFactory.nativeRegularJoinBatchCount())
                    .isGreaterThan(0);
        }
    }

    static java.util.stream.Stream<Arguments> setOperationTypes() {
        return SelectDistinctParityTest.distinctTypes();
    }
}
